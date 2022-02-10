import { IWritable } from "../common/IWritable";
import { ChunkBuilder } from "./ChunkBuilder";
import { Mcap0RecordBuilder } from "./Mcap0RecordBuilder";
import { Opcode } from "./constants";
import {
  Channel,
  Message,
  Header,
  Attachment,
  Chunk,
  ChunkIndex,
  AttachmentIndex,
  MetadataIndex,
  SummaryOffset,
  Metadata,
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
  private channels = new Map<number, Channel>();
  private writtenChannelIds = new Set<number>();
  private chunkBuilder: ChunkBuilder = new ChunkBuilder();

  // indices
  private chunkIndices: ChunkIndex[] = [];
  private attachmentIndices: AttachmentIndex[] = [];
  private metadataIndices: MetadataIndex[] = [];

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

    const channelStart = this.writable.position();
    let channelLength = 0n;
    for (const channel of this.channels.values()) {
      channelLength += this.recordWriter.writeChannel(channel);
    }
    summaryOffsets.push({
      groupOpcode: Opcode.CHANNEL,
      groupStart: channelStart,
      groupLength: channelLength,
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
      groupLength: chunkIndexLength,
    });

    const attachmentIndexStart = this.writable.position();
    let attachmentIndexLength = 0n;
    for (const attachmentIndex of this.attachmentIndices) {
      attachmentIndexLength += this.recordWriter.writeAttachmentIndex(attachmentIndex);
    }
    summaryOffsets.push({
      groupOpcode: Opcode.ATTACHMENT_INDEX,
      groupStart: attachmentIndexStart,
      groupLength: attachmentIndexLength,
    });

    const metadataIndexStart = this.writable.position();
    let metadataIndexLength = 0n;
    for (const metadataIndex of this.metadataIndices) {
      metadataIndexLength += this.recordWriter.writeMetadataIndex(metadataIndex);
    }
    summaryOffsets.push({
      groupOpcode: Opcode.METADATA_INDEX,
      groupStart: metadataIndexStart,
      groupLength: metadataIndexLength,
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
      summaryCrc: 0,
    });

    this.recordWriter.writeMagic();

    await this.writable.write(this.recordWriter.buffer);
    this.recordWriter.reset();
  }

  /**
   * Add channel and return a generated channel id. The channel id is used when adding messages.
   */
  async registerChannel(info: Omit<Channel, "id">): Promise<number> {
    const id = this.channels.size + 1;
    this.channels.set(id, {
      ...info,
      id,
    });

    return id;
  }

  async addMessage(message: Message): Promise<void> {
    // write out channel id if we have not yet done so
    if (!this.writtenChannelIds.has(message.channelId)) {
      const channel = this.channels.get(message.channelId);
      if (!channel) {
        throw new Error(
          `Mcap0UnindexedWriter#addMessage failed: missing channel for id ${message.channelId}`,
        );
      }

      this.chunkBuilder.addChannel(channel);
      this.writtenChannelIds.add(message.channelId);
    }

    this.chunkBuilder.addMessage(message);

    if (this.chunkBuilder.numMessages > 10) {
      await this.finalizeChunk();
    }
  }

  async addAttachment(attachment: Attachment): Promise<void> {
    const length = this.recordWriter.writeAttachment(attachment);

    const offset = this.writable.position();
    this.attachmentIndices.push({
      logTime: attachment.logTime,
      name: attachment.name,
      contentType: attachment.contentType,
      offset,
      dataSize: BigInt(attachment.data.byteLength),
      length,
    });

    await this.writable.write(this.recordWriter.buffer);
    this.recordWriter.reset();
  }

  async addMetadata(metadata: Metadata): Promise<void> {
    const recordSize = this.recordWriter.writeMetadata(metadata);

    const offset = this.writable.position();
    this.metadataIndices.push({
      name: metadata.name,
      offset,
      length: recordSize,
    });

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

    const chunkStartOffset = this.writable.position();

    const recordRecordSize = this.recordWriter.writeChunk(chunkRecord);
    const chunkEnd = chunkStartOffset + recordRecordSize;

    const chunkIndex: ChunkIndex = {
      startTime: chunkRecord.startTime,
      endTime: chunkRecord.endTime,
      chunkStartOffset,
      chunkLength: chunkEnd,
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
