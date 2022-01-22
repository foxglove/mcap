import { IWritable } from "../common/IWritable";
import { ChunkBuilder } from "./ChunkBuilder";
import { Mcap0BufferRecordBuilder } from "./Mcap0BufferedRecordBuilder";
import { ChannelInfo, Message, Header, Attachment, Chunk, ChunkIndex } from "./types";

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
  private recordWriter = new Mcap0BufferRecordBuilder();
  private channelInfos = new Map<number, ChannelInfo>();
  private writtenChannelIds = new Set<number>();
  private chunkIndices: ChunkIndex[] = [];
  private chunkBuilder: ChunkBuilder = new ChunkBuilder();

  constructor(writable: IWritable) {
    this.writable = writable;
  }

  async start(header: Header): Promise<void> {
    this.recordWriter.writeMagic();
    this.recordWriter.writeHeader(header);

    await this.writable.write(this.recordWriter.buffer);
    this.recordWriter.reset();
  }

  async end(): Promise<void> {
    await this.finalizeChunk();

    const position = this.writable.position();

    for (const channelInfo of this.channelInfos.values()) {
      this.recordWriter.writeChannelInfo(channelInfo);
    }

    await this.writable.write(this.recordWriter.buffer);
    this.recordWriter.reset();

    for (const chunkIndex of this.chunkIndices) {
      this.recordWriter.writeChunkIndex(chunkIndex);
    }

    await this.writable.write(this.recordWriter.buffer);
    this.recordWriter.reset();

    this.recordWriter.writeFooter({
      indexOffset: position,
      indexCrc: 0,
    });

    this.recordWriter.writeMagic();

    await this.writable.write(this.recordWriter.buffer);
    this.recordWriter.reset();
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
    this.recordWriter.writeAttachment(attachment);

    await this.writable.write(this.recordWriter.buffer);
    this.recordWriter.reset();
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

    this.recordWriter.writeChunk(chunkRecord);

    const startPosition = this.writable.position();
    for (const messageIndex of this.chunkBuilder.indices) {
      chunkIndex.messageIndexOffsets.set(messageIndex.channelId, this.writable.position());
      this.recordWriter.writeMessageIndex(messageIndex);
    }

    chunkIndex.messageIndexLength = this.writable.position() - startPosition;

    this.chunkIndices.push(chunkIndex);
    this.chunkBuilder.reset();

    await this.writable.write(this.recordWriter.buffer);
    this.recordWriter.reset();
  }
}
