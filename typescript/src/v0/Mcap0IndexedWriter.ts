import { IWritable } from "../common/IWritable";
import { ChunkBuilder } from "./ChunkBuilder";
import { Mcap0RecordBuilder } from "./Mcap0RecordBuilder";
import { Opcode } from "./constants";
import {
  ChannelInfo,
  Message,
  Header,
  Attachment,
  Chunk,
  ChunkIndex,
  SummaryOffset,
} from "./types";

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
  private recordWriter = new Mcap0RecordBuilder();
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

    const summaryOffsets: SummaryOffset[] = [];

    const summaryStart = this.writable.position();

    const channelInfoStart = this.writable.position();
    let channelInfoLength = 0n;
    for (const channelInfo of this.channelInfos.values()) {
      channelInfoLength += this.recordWriter.writeChannelInfo(channelInfo);
    }
    summaryOffsets.push({
      groupOpcode: Opcode.CHANNEL_INFO,
      groupStart: channelInfoStart,
      groupEnd: channelInfoStart + channelInfoLength,
    });

    await this.writable.write(this.recordWriter.buffer);
    this.recordWriter.reset();

    const chunkIndexStart = this.writable.position();
    let chunkIndexLength = 0n;
    for (const chunkIndex of this.chunkIndices) {
      chunkIndexLength += this.recordWriter.writeChunkIndex(chunkIndex);
    }
    summaryOffsets.push({
      groupOpcode: Opcode.CHUNK_INDEX,
      groupStart: chunkIndexStart,
      groupEnd: chunkIndexStart + chunkIndexLength,
    });

    await this.writable.write(this.recordWriter.buffer);
    this.recordWriter.reset();

    const summaryOffsetStart = this.writable.position();

    for (const summaryOffset of summaryOffsets) {
      this.recordWriter.writeSummaryOffset(summaryOffset);
    }
    this.recordWriter.reset();

    this.recordWriter.writeFooter({
      summaryStart,
      summaryOffsetStart,
      crc: 0,
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
      startTime: this.chunkBuilder.startTime,
      endTime: this.chunkBuilder.endTime,
      uncompressedSize: BigInt(chunkData.length),
      uncompressedCrc: 0,
      compression: "",
      records: chunkData,
    };

    const chunkStart = this.writable.position();

    const recordRecordSize = this.recordWriter.writeChunk(chunkRecord);
    const chunkEnd = chunkStart + recordRecordSize;

    const chunkIndex: ChunkIndex = {
      startTime: chunkRecord.startTime,
      endTime: chunkRecord.endTime,
      chunkStart,
      chunkEnd,
      messageIndexOffsets: new Map(),
      messageIndexLength: 0n,
      compression: chunkRecord.compression,
      compressedSize: 0n,
      uncompressedSize: chunkRecord.uncompressedSize,
    };

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
