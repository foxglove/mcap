import { IWritable } from "../common/IWritable";
import { Mcap0MemoryRecordWriter, MemoryWritable } from "./Mcap0MemoryRecordWriter";
import { Mcap0RecordWriter } from "./Mcap0RecordWriter";
import { ChannelInfo, Message, Header, Attachment, Chunk, MessageIndex, ChunkIndex } from "./types";

class ChunkBuilder {
  private memoryWritable = new MemoryWritable();
  private recordWriter: Mcap0MemoryRecordWriter;
  private messageIndices = new Map<number, MessageIndex>();

  startTime = 0n;
  endTime = 0n;

  get numMessages(): number {
    return 0;
  }

  get buffer(): Uint8Array {
    return this.memoryWritable.buffer;
  }

  get indices(): IterableIterator<MessageIndex> {
    return this.messageIndices.values();
  }

  constructor() {
    this.recordWriter = new Mcap0MemoryRecordWriter(this.memoryWritable);
  }

  addChannelInfo(info: ChannelInfo): void {
    if (!this.messageIndices.has(info.channelId)) {
      this.messageIndices.set(info.channelId, {
        channelId: info.channelId,
        count: 0,
        records: [],
      });
    }
    this.recordWriter.writeChannelInfo(info);
  }

  addMessage(message: Message): void {
    if (this.startTime === 0n) {
      this.startTime = message.recordTime;
    }
    this.endTime = message.recordTime;

    const messageIndex = this.messageIndices.get(message.channelId);
    if (!messageIndex) {
      // fixme - should I make a new empty message index instead? that seems valid and better
      throw new Error("Unable to find message index");
    }

    messageIndex.count += 1;
    messageIndex.records.push([message.recordTime, BigInt(this.memoryWritable.length)]);

    this.recordWriter.writeMessage(message);
  }

  reset(): void {
    this.memoryWritable.reset();
    this.messageIndices.clear();
  }
}

/**
 * Mcap0IndexedWriter provides an interface for writing messages
 * to indexed mcap files.
 *
 * NOTE: callers must wait on any method call to complete before calling another
 * method. Calling a method before another has completed will result in a corrupt
 * mcap file.
 */
export class Mcap0IndexedWriter {
  private writable: IWritable;
  private recordWriter: Mcap0RecordWriter;
  private channelInfos = new Map<number, ChannelInfo>();
  private writtenChannelIds = new Set<number>();
  private chunkIndices: ChunkIndex[] = [];
  private chunkBuilder: ChunkBuilder = new ChunkBuilder();

  constructor(writable: IWritable) {
    this.writable = writable;
    this.recordWriter = new Mcap0RecordWriter(writable);
  }

  async start(header: Header): Promise<void> {
    await this.recordWriter.writeMagic();
    await this.recordWriter.writeHeader(header);
  }

  async end(): Promise<void> {
    await this.finalizeChunk();

    const position = this.writable.position();

    for (const channelInfo of this.channelInfos.values()) {
      await this.recordWriter.writeChannelInfo(channelInfo);
    }

    for (const chunkIndex of this.chunkIndices) {
      await this.recordWriter.writeChunkIndex(chunkIndex);
    }

    // fixme - write the attachment index records

    await this.recordWriter.writeFooter({
      indexOffset: position,
      indexCrc: 0,
    });

    await this.recordWriter.writeMagic();
  }

  /**
   * Add channel info and return a generated channel id. The channel id is used when adding messages.
   */
  async registerChannel(info: Omit<ChannelInfo, "channelId">): Promise<number> {
    const channelId = this.channelInfos.size + 1;
    this.channelInfos.set(channelId, {
      ...info,
      channelId,
    });

    return channelId;
  }

  async addMessage(message: Message): Promise<void> {
    // write out channel id if we have not yet done so
    if (!this.writtenChannelIds.has(message.channelId)) {
      const channelInfo = this.channelInfos.get(message.channelId);
      if (!channelInfo) {
        throw new Error(
          `Mcap0UnindexedWriter#addMessage failed: missing channel info for id ${message.channelId}`,
        );
      }

      this.chunkBuilder.addChannelInfo(channelInfo);
      this.writtenChannelIds.add(message.channelId);
    }

    this.chunkBuilder.addMessage(message);

    if (this.chunkBuilder.numMessages > 10) {
      await this.finalizeChunk();
    }
  }

  async addAttachment(attachment: Attachment): Promise<void> {
    await this.recordWriter.writeAttachment(attachment);
  }

  private async finalizeChunk(): Promise<void> {
    if (this.chunkBuilder.numMessages === 0) {
      return;
    }

    const chunkData = this.chunkBuilder.buffer;
    const chunkRecord: Chunk = {
      uncompressedSize: BigInt(chunkData.length),
      uncompressedCrc: 0,
      compression: "",
      records: chunkData,
    };

    const offset = this.writable.position();
    const chunkIndex: ChunkIndex = {
      startTime: this.chunkBuilder.startTime,
      endTime: this.chunkBuilder.endTime,
      chunkOffset: offset,
      messageIndexOffsets: new Map(),
      messageIndexLength: 0n,
      compression: chunkRecord.compression,
      compressedSize: 0n,
      uncompressedSize: chunkRecord.uncompressedSize,
    };

    await this.recordWriter.writeChunk(chunkRecord);

    const startPosition = this.writable.position();
    for (const messageIndex of this.chunkBuilder.indices) {
      chunkIndex.messageIndexOffsets.set(messageIndex.channelId, this.writable.position());
      await this.recordWriter.writeMessageIndex(messageIndex);
    }

    chunkIndex.messageIndexLength = this.writable.position() - startPosition;

    this.chunkIndices.push(chunkIndex);
    this.chunkBuilder.reset();
  }
}
