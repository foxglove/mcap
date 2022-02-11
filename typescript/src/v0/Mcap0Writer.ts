import { IWritable } from "../common/IWritable";
import { ChunkBuilder } from "./ChunkBuilder";
import { Mcap0RecordBuilder } from "./Mcap0RecordBuilder";
import { Opcode } from "./constants";
import {
  Schema,
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
  Statistics,
} from "./types";

type Mcap0WriterOptions = {
  writable: IWritable;
  useStatistics?: boolean;
  useSummaryOffsets?: boolean;
  useChunks?: boolean;
  repeatSchemas?: boolean;
  repeatChannels?: boolean;
  useAttachmentIndex?: boolean;
  useMetadataIndex?: boolean;
  useMessageIndex?: boolean;
  useChunkIndex?: boolean;
};

/**
 * Mcap0Writer provides an interface for writing messages to MCAP files.
 *
 * NOTE: callers must wait on any method call to complete before calling another
 * method. Calling a method before another has completed will result in a corrupt
 * MCAP file.
 */
export class Mcap0Writer {
  private static MIN_CHANNEL_ID = 1; //FIXME- only for conformance tests
  private static MIN_SCHEMA_ID = 1;
  private writable: IWritable;
  private recordWriter = new Mcap0RecordBuilder();
  private schemas = new Map<number, Schema>();
  private channels = new Map<number, Channel>();
  private writtenSchemaIds = new Set<number>();
  private writtenChannelIds = new Set<number>();
  private chunkBuilder: ChunkBuilder | undefined;

  private statistics: Statistics | undefined;
  private useSummaryOffsets: boolean;
  private repeatSchemas: boolean;
  private repeatChannels: boolean;

  // indices
  private chunkIndices: ChunkIndex[] | undefined;
  private attachmentIndices: AttachmentIndex[] | undefined;
  private metadataIndices: MetadataIndex[] | undefined;

  constructor({
    writable,
    useStatistics = true,
    useSummaryOffsets = true,
    useChunks = true,
    repeatSchemas = true,
    repeatChannels = true,
    useAttachmentIndex = true,
    useMetadataIndex = true,
    useMessageIndex = true,
    useChunkIndex = true,
  }: Mcap0WriterOptions) {
    this.writable = writable;
    this.useSummaryOffsets = useSummaryOffsets;
    if (useStatistics) {
      this.statistics = {
        messageCount: 0n,
        schemaCount: 0,
        channelCount: 0,
        attachmentCount: 0,
        metadataCount: 0,
        chunkCount: 0,
        messageStartTime: 0n,
        messageEndTime: 0n,
        channelMessageCounts: new Map(),
      };
    }
    if (useChunks) {
      this.chunkBuilder = new ChunkBuilder({ useMessageIndex });
    }
    this.repeatSchemas = repeatSchemas;
    this.repeatChannels = repeatChannels;
    if (useAttachmentIndex) {
      this.attachmentIndices = [];
    }
    if (useMetadataIndex) {
      this.metadataIndices = [];
    }
    if (useChunkIndex) {
      this.chunkIndices = [];
    }
  }

  async start(header: Header): Promise<void> {
    this.recordWriter.writeMagic();
    this.recordWriter.writeHeader(header);

    await this.writable.write(this.recordWriter.buffer);
    this.recordWriter.reset();
  }

  async end(): Promise<void> {
    await this.finalizeChunk();

    this.recordWriter.writeDataEnd({ dataSectionCrc: 0 });
    await this.writable.write(this.recordWriter.buffer);
    this.recordWriter.reset();

    const summaryOffsets: SummaryOffset[] = [];

    const summaryStart = this.writable.position();

    if (this.repeatSchemas) {
      const schemaStart = this.writable.position();
      let schemaLength = 0n;
      for (const schema of this.schemas.values()) {
        schemaLength += this.recordWriter.writeSchema(schema);
      }
      summaryOffsets.push({
        groupOpcode: Opcode.SCHEMA,
        groupStart: schemaStart,
        groupLength: schemaLength,
      });
    }

    //FIXME: better position calculation + fewer write calls?

    if (this.repeatChannels) {
      await this.writable.write(this.recordWriter.buffer);
      this.recordWriter.reset();
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
    }

    if (this.statistics) {
      await this.writable.write(this.recordWriter.buffer);
      this.recordWriter.reset();
      const statisticsStart = this.writable.position();
      const statisticsLength = this.recordWriter.writeStatistics(this.statistics);
      summaryOffsets.push({
        groupOpcode: Opcode.STATISTICS,
        groupStart: statisticsStart,
        groupLength: statisticsLength,
      });
    }

    await this.writable.write(this.recordWriter.buffer);
    this.recordWriter.reset();

    if (this.chunkIndices) {
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
    }

    if (this.attachmentIndices) {
      await this.writable.write(this.recordWriter.buffer);
      this.recordWriter.reset();
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
    }

    if (this.metadataIndices) {
      await this.writable.write(this.recordWriter.buffer);
      this.recordWriter.reset();
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
    }

    await this.writable.write(this.recordWriter.buffer);
    this.recordWriter.reset();

    const summaryOffsetStart = this.writable.position();
    const summaryLength = summaryOffsetStart - summaryStart;

    if (this.useSummaryOffsets) {
      for (const summaryOffset of summaryOffsets) {
        if (summaryOffset.groupLength !== 0n) {
          this.recordWriter.writeSummaryOffset(summaryOffset);
        }
      }
    }

    this.recordWriter.writeFooter({
      summaryStart: summaryLength === 0n ? 0n : summaryStart,
      summaryOffsetStart: this.useSummaryOffsets ? summaryOffsetStart : 0n,
      summaryCrc: 0,
    });

    this.recordWriter.writeMagic();

    await this.writable.write(this.recordWriter.buffer);
    this.recordWriter.reset();
  }

  /**
   * Add a schema and return a generated schema id. The schema id is used when adding channels.
   */
  async registerSchema(info: Omit<Schema, "id">): Promise<number> {
    const id = Mcap0Writer.MIN_SCHEMA_ID + this.schemas.size;
    this.schemas.set(id, { ...info, id });
    if (this.statistics) {
      ++this.statistics.schemaCount;
    }
    return id;
  }

  /**
   * Add a channel and return a generated channel id. The channel id is used when adding messages.
   */
  async registerChannel(info: Omit<Channel, "id">): Promise<number> {
    const id = Mcap0Writer.MIN_CHANNEL_ID + this.channels.size;
    this.channels.set(id, { ...info, id });
    if (this.statistics) {
      ++this.statistics.channelCount;
    }
    return id;
  }

  async addMessage(message: Message): Promise<void> {
    if (this.statistics) {
      if (this.statistics.messageCount === 0n) {
        this.statistics.messageStartTime = message.logTime;
        this.statistics.messageEndTime = message.logTime;
      } else {
        if (message.logTime < this.statistics.messageStartTime) {
          this.statistics.messageStartTime = message.logTime;
        }
        if (message.logTime > this.statistics.messageEndTime) {
          this.statistics.messageEndTime = message.logTime;
        }
      }
      this.statistics.channelMessageCounts.set(
        message.channelId,
        (this.statistics.channelMessageCounts.get(message.channelId) ?? 0n) + 1n,
      );
      ++this.statistics.messageCount;
    }
    // write out channel and schema if we have not yet done so
    if (!this.writtenChannelIds.has(message.channelId)) {
      const channel = this.channels.get(message.channelId);
      if (!channel) {
        throw new Error(
          `Mcap0Writer#addMessage failed: missing channel for id ${message.channelId}`,
        );
      }

      if (!this.writtenSchemaIds.has(channel.schemaId)) {
        const schema = this.schemas.get(channel.schemaId);
        if (!schema) {
          throw new Error(
            `Mcap0Writer#addMessage failed: missing schema for id ${channel.schemaId}`,
          );
        }
        if (this.chunkBuilder) {
          this.chunkBuilder.addSchema(schema);
        } else {
          this.recordWriter.writeSchema(schema);
        }
        this.writtenSchemaIds.add(channel.schemaId);
      }

      if (this.chunkBuilder) {
        this.chunkBuilder.addChannel(channel);
      } else {
        this.recordWriter.writeChannel(channel);
      }
      this.writtenChannelIds.add(message.channelId);
    }

    if (this.chunkBuilder) {
      this.chunkBuilder.addMessage(message);
    } else {
      this.recordWriter.writeMessage(message);
    }

    if (this.chunkBuilder && this.chunkBuilder.numMessages > 10) {
      await this.finalizeChunk();
    }
  }

  async addAttachment(attachment: Attachment): Promise<void> {
    const length = this.recordWriter.writeAttachment(attachment);
    if (this.statistics) {
      ++this.statistics.attachmentCount;
    }

    if (this.attachmentIndices) {
      const offset = this.writable.position();
      this.attachmentIndices.push({
        logTime: attachment.logTime,
        name: attachment.name,
        contentType: attachment.contentType,
        offset,
        dataSize: BigInt(attachment.data.byteLength),
        length,
      });
    }

    await this.writable.write(this.recordWriter.buffer);
    this.recordWriter.reset();
  }

  async addMetadata(metadata: Metadata): Promise<void> {
    const recordSize = this.recordWriter.writeMetadata(metadata);

    if (this.metadataIndices) {
      const offset = this.writable.position();
      this.metadataIndices.push({
        name: metadata.name,
        offset,
        length: recordSize,
      });
    }

    await this.writable.write(this.recordWriter.buffer);
    this.recordWriter.reset();
  }

  private async finalizeChunk(): Promise<void> {
    if (!this.chunkBuilder || this.chunkBuilder.numMessages === 0) {
      return;
    }
    if (this.statistics) {
      ++this.statistics.chunkCount;
    }

    const chunkData = this.chunkBuilder.buffer;
    const chunkRecord: Chunk = {
      messageStartTime: this.chunkBuilder.messageStartTime,
      messageEndTime: this.chunkBuilder.messageEndTime,
      uncompressedSize: BigInt(chunkData.length),
      uncompressedCrc: 0,
      compression: "",
      records: chunkData,
    };

    const chunkStartOffset = this.writable.position();

    const chunkLength = this.recordWriter.writeChunk(chunkRecord);

    const messageIndexOffsets = this.chunkIndices ? new Map<number, bigint>() : undefined;

    await this.writable.write(this.recordWriter.buffer);
    this.recordWriter.reset();

    const messageIndexStart = this.writable.position();
    let messageIndexLength = 0n;
    for (const messageIndex of this.chunkBuilder.indices) {
      messageIndexOffsets?.set(messageIndex.channelId, messageIndexStart + messageIndexLength);
      messageIndexLength += this.recordWriter.writeMessageIndex(messageIndex);
    }

    if (this.chunkIndices) {
      this.chunkIndices.push({
        messageStartTime: chunkRecord.messageStartTime,
        messageEndTime: chunkRecord.messageEndTime,
        chunkStartOffset,
        chunkLength,
        messageIndexOffsets: messageIndexOffsets!,
        messageIndexLength,
        compression: chunkRecord.compression,
        compressedSize: BigInt(chunkRecord.records.byteLength),
        uncompressedSize: chunkRecord.uncompressedSize,
      });
    }
    this.chunkBuilder.reset();

    await this.writable.write(this.recordWriter.buffer);
    this.recordWriter.reset();
  }
}
