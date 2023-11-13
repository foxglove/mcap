import { crc32Init, crc32Update, crc32Final } from "@foxglove/crc";

import { IWritable } from "./IWritable";
import { McapRecordBuilder } from "./McapRecordBuilder";
import { Opcode } from "./constants";
import {
  Schema,
  Channel,
  Footer,
  Attachment,
  ChunkIndex,
  AttachmentIndex,
  MetadataIndex,
  SummaryOffset,
  Metadata,
  Statistics,
} from "./types";

export type McapAppenderOptions = {
  writable: IWritable;
  useStatistics?: boolean;
  useSummaryOffsets?: boolean;
  repeatSchemas?: boolean;
  repeatChannels?: boolean;
  useAttachmentIndex?: boolean;
  useMetadataIndex?: boolean;
  useChunkIndex?: boolean;
};

/**
 * McapAppender provides an interface for appending attachments and metadata to existing MCAP files.
 *
 * NOTE: callers must wait on any method call to complete before calling another
 * method. Calling a method before another has completed will result in a corrupt
 * MCAP file.
 */
export class McapAppender {
  private writable: IWritable;
  private recordWriter = new McapRecordBuilder();
  private schemas = new Map<number, Schema>();
  private channels = new Map<number, Channel>();
  private dataSectionCrc = crc32Init();

  public statistics: Statistics | undefined;
  private useSummaryOffsets: boolean;
  private repeatSchemas: boolean;
  private repeatChannels: boolean;

  // indices
  private chunkIndices: ChunkIndex[] | undefined;
  private attachmentIndices: AttachmentIndex[] | undefined;
  private metadataIndices: MetadataIndex[] | undefined;

  constructor(options: McapAppenderOptions) {
    const {
      writable,
      useStatistics = true,
      useSummaryOffsets = true,
      repeatSchemas = true,
      repeatChannels = true,
      useAttachmentIndex = true,
      useMetadataIndex = true,
      useChunkIndex = true,
    } = options;

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

  /**
   * Writes the summary and footer at the end of the MCAP file.
   * Call once done adding metadata/attachments and before closing the file.
   */
  async end(): Promise<void> {
    this.dataSectionCrc = crc32Update(this.dataSectionCrc, this.recordWriter.buffer);
    await this.writable.write(this.recordWriter.buffer);
    this.recordWriter.reset();

    this.recordWriter.writeDataEnd({ dataSectionCrc: crc32Final(this.dataSectionCrc) });
    await this.writable.write(this.recordWriter.buffer);
    this.recordWriter.reset();

    const summaryOffsets: SummaryOffset[] = [];

    const summaryStart = this.writable.position();
    let summaryCrc = crc32Init();

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

    if (this.repeatChannels) {
      summaryCrc = crc32Update(summaryCrc, this.recordWriter.buffer);
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
      summaryCrc = crc32Update(summaryCrc, this.recordWriter.buffer);
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

    summaryCrc = crc32Update(summaryCrc, this.recordWriter.buffer);
    await this.writable.write(this.recordWriter.buffer);
    this.recordWriter.reset();

    if (this.metadataIndices) {
      summaryCrc = crc32Update(summaryCrc, this.recordWriter.buffer);
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

    if (this.attachmentIndices) {
      summaryCrc = crc32Update(summaryCrc, this.recordWriter.buffer);
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

    if (this.chunkIndices) {
      summaryCrc = crc32Update(summaryCrc, this.recordWriter.buffer);
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

    summaryCrc = crc32Update(summaryCrc, this.recordWriter.buffer);
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

    summaryCrc = crc32Update(summaryCrc, this.recordWriter.buffer);

    const footer: Footer = {
      summaryStart: summaryLength === 0n ? 0n : summaryStart,
      summaryOffsetStart: this.useSummaryOffsets ? summaryOffsetStart : 0n,
      summaryCrc: 0,
    };
    const tempBuffer = new DataView(new ArrayBuffer(1 + 8 + 8 + 8));
    tempBuffer.setUint8(0, Opcode.FOOTER);
    tempBuffer.setBigUint64(1, 8n + 8n + 4n, true);
    tempBuffer.setBigUint64(1 + 8, footer.summaryStart, true);
    tempBuffer.setBigUint64(1 + 8 + 8, footer.summaryOffsetStart, true);
    summaryCrc = crc32Update(summaryCrc, tempBuffer);
    footer.summaryCrc = crc32Final(summaryCrc);

    this.recordWriter.writeFooter(footer);

    this.recordWriter.writeMagic();

    await this.writable.write(this.recordWriter.buffer);
    this.recordWriter.reset();
  }

  /**
   * Set statistics from existing MCAP file.
   * The purpose of this is to update the information for the new summary with the existing data from the existing MCAP file.
   *
   * @param messageCount
   * @param messageStartTime
   * @param messageEndTime
   * @param chunkCount
   * @param channelMessageCounts
   */
  async setStatistics(
    messageCount: bigint,
    messageStartTime: bigint,
    messageEndTime: bigint,
    chunkCount: number,
    channelMessageCounts: Map<number, bigint>,
  ): Promise<void> {
    if (this.statistics) {
      this.statistics.messageCount = messageCount;
      this.statistics.messageStartTime = messageStartTime;
      this.statistics.messageEndTime = messageEndTime;
      this.statistics.chunkCount = chunkCount;
      this.statistics.channelMessageCounts = channelMessageCounts;
    }
  }

  /**
   * Register schema from existing MCAP file.
   * The purpose of this is to update the information for the new summary with the existing data from the existing MCAP file.
   *
   * @param info
   */
  async registerSchema(info: Schema): Promise<void> {
    this.schemas.set(info.id, info);
    if (this.statistics) {
      ++this.statistics.schemaCount;
    }
  }

  /**
   * Register channel from existing MCAP file.
   * The purpose of this is to update the information for the new summary with the existing data from the existing MCAP file.
   *
   * @param info
   */
  async registerChannel(info: Channel): Promise<void> {
    this.channels.set(info.id, info);
    if (this.statistics) {
      ++this.statistics.channelCount;
    }
  }

  /**
   * Add a new attachment to an existing MCAP file.
   *
   * @param attachment
   */
  async addAttachment(attachment: Attachment): Promise<void> {
    const length = this.recordWriter.writeAttachment(attachment);
    if (this.statistics) {
      ++this.statistics.attachmentCount;
    }

    if (this.attachmentIndices) {
      const offset = this.writable.position();
      this.attachmentIndices.push({
        logTime: attachment.logTime,
        createTime: attachment.createTime,
        name: attachment.name,
        mediaType: attachment.mediaType,
        offset,
        dataSize: BigInt(attachment.data.byteLength),
        length,
      });
    }

    this.dataSectionCrc = crc32Update(this.dataSectionCrc, this.recordWriter.buffer);
    await this.writable.write(this.recordWriter.buffer);
    this.recordWriter.reset();
  }

  /**
   * Add attachment index from existing MCAP file.
   * The purpose of this is to update the information for the new summary with the existing data from the existing MCAP file.
   *
   * @param attachmentIndex
   */
  async addAttachmentIndex(attachmentIndex: AttachmentIndex): Promise<void> {
    if (this.statistics) {
      ++this.statistics.attachmentCount;
    }

    if (this.attachmentIndices) {
      this.attachmentIndices.push(attachmentIndex);
    }

    this.dataSectionCrc = crc32Update(this.dataSectionCrc, this.recordWriter.buffer);
    this.recordWriter.reset();
  }

  /**
   * Add new metadata to an existing MCAP file.
   *
   * @param metadata
   */
  async addMetadata(metadata: Metadata): Promise<void> {
    const recordSize = this.recordWriter.writeMetadata(metadata);
    if (this.statistics) {
      ++this.statistics.metadataCount;
    }

    if (this.metadataIndices) {
      const offset = this.writable.position();
      this.metadataIndices.push({
        name: metadata.name,
        offset,
        length: recordSize,
      });
    }

    this.dataSectionCrc = crc32Update(this.dataSectionCrc, this.recordWriter.buffer);
    await this.writable.write(this.recordWriter.buffer);
    this.recordWriter.reset();
  }

  /**
   * Add metadata index from existing MCAP file.
   * The purpose of this is to update the information for the new summary with the existing data from the existing MCAP file.
   *
   * @param metadataIndex
   */
  async addMetadataIndex(metadataIndex: MetadataIndex): Promise<void> {
    if (this.statistics) {
      ++this.statistics.metadataCount;
    }

    if (this.metadataIndices) {
      this.metadataIndices.push(metadataIndex);
    }

    this.dataSectionCrc = crc32Update(this.dataSectionCrc, this.recordWriter.buffer);
    this.recordWriter.reset();
  }

  /**
   * Add chunk index from existing MCAP file.
   * The purpose of this is to update the information for the new summary with the existing data from the existing MCAP file.
   *
   * @param chunkIndex
   */
  async addChunkIndex(chunkIndex: ChunkIndex): Promise<void> {
    if (this.statistics) {
      ++this.statistics.chunkCount;
    }

    if (this.chunkIndices) {
      this.chunkIndices.push(chunkIndex);
    }

    this.dataSectionCrc = crc32Update(this.dataSectionCrc, this.recordWriter.buffer);
    this.recordWriter.reset();
  }
}
