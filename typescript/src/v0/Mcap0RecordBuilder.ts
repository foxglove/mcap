import { SummaryOffset } from ".";
import { BufferBuilder } from "./BufferBuilder";
import { MCAP0_MAGIC, Opcode } from "./constants";
import {
  ChannelInfo,
  Header,
  Footer,
  Message,
  Attachment,
  Chunk,
  ChunkIndex,
  MessageIndex,
} from "./types";

/**
 * Mcap0RecordBuilder provides methods to serialize mcap records to a buffer in memory.
 *
 * It makes no effort to ensure spec compatability on the order of records, this is the responsibility
 * of the caller.
 *
 * You'll likely want to use one of the higher level writer interfaces unless you are building your
 * own higher level writing interface.
 */
export class Mcap0RecordBuilder {
  private bufferBuilder = new BufferBuilder();

  get length(): number {
    return this.bufferBuilder.length;
  }

  get buffer(): Uint8Array {
    return this.bufferBuilder.buffer;
  }

  reset(): void {
    this.bufferBuilder.reset();
  }

  writeMagic(): void {
    this.bufferBuilder.bytes(new Uint8Array(MCAP0_MAGIC));
  }

  writeHeader(header: Header): void {
    this.bufferBuilder.uint8(Opcode.HEADER);

    const startPosition = this.bufferBuilder.length;
    this.bufferBuilder
      .uint64(0n) // placeholder size
      .string(header.profile)
      .string(header.library)
      .array(header.metadata);

    const endPosition = this.bufferBuilder.length;
    this.bufferBuilder
      .seek(startPosition)
      .uint64(BigInt(endPosition - startPosition - 8))
      .seek(endPosition);
  }

  writeFooter(footer: Footer): void {
    this.bufferBuilder
      .uint8(Opcode.FOOTER)
      .uint64(20n) // footer is fixed length
      .uint64(footer.summaryStart)
      .uint64(footer.summaryOffsetStart)
      .uint32(footer.crc);
  }

  writeChannelInfo(info: ChannelInfo): bigint {
    this.bufferBuilder.uint8(Opcode.CHANNEL_INFO);

    const startPosition = this.bufferBuilder.length;
    this.bufferBuilder
      .uint64(0n) // placeholder
      .uint16(info.channelId)
      .string(info.topicName)
      .string(info.messageEncoding)
      .string(info.schemaFormat)
      .string(info.schema)
      .string(info.schemaName)
      .array(info.userData);

    const endPosition = this.bufferBuilder.length;
    this.bufferBuilder
      .seek(startPosition)
      .uint64(BigInt(endPosition - startPosition - 8))
      .seek(endPosition);

    return BigInt(endPosition - startPosition + 1);
  }

  writeMessage(message: Message): void {
    this.bufferBuilder
      .uint8(Opcode.MESSAGE)
      .uint64(BigInt(22 + message.messageData.byteLength))
      .uint16(message.channelId)
      .uint32(message.sequence)
      .uint64(message.publishTime)
      .uint64(message.recordTime)
      .bytes(message.messageData);
  }

  writeAttachment(attachment: Attachment): void {
    this.bufferBuilder.uint8(Opcode.ATTACHMENT);

    const startPosition = this.bufferBuilder.length;
    this.bufferBuilder
      .uint64(0n) // placeholder
      .string(attachment.name)
      .uint64(attachment.recordTime)
      .string(attachment.contentType)
      .bytes(attachment.data);

    const endPosition = this.bufferBuilder.length;
    this.bufferBuilder
      .seek(startPosition)
      .uint64(BigInt(endPosition - startPosition - 8))
      .seek(endPosition);
  }

  writeChunk(chunk: Chunk): bigint {
    this.bufferBuilder.uint8(Opcode.CHUNK);

    const startPosition = this.bufferBuilder.length;
    this.bufferBuilder
      .uint64(0n) // palceholder
      .uint64(chunk.startTime)
      .uint64(chunk.endTime)
      .uint64(chunk.uncompressedSize)
      .uint32(chunk.uncompressedCrc)
      .string(chunk.compression)
      .bytes(chunk.records);

    const endPosition = this.bufferBuilder.length;
    this.bufferBuilder
      .seek(startPosition)
      .uint64(BigInt(endPosition - startPosition - 8))
      .seek(endPosition);

    return BigInt(endPosition - startPosition - 1);
  }

  writeChunkIndex(chunkIndex: ChunkIndex): bigint {
    this.bufferBuilder.uint8(Opcode.CHUNK_INDEX);

    const startPosition = this.bufferBuilder.length;
    this.bufferBuilder
      .uint64(0n) // placeholder
      .uint64(chunkIndex.startTime)
      .uint64(chunkIndex.endTime)
      .uint64(chunkIndex.chunkStart)
      .uint64(chunkIndex.chunkEnd)
      .uint32(chunkIndex.messageIndexOffsets.size * 10);

    for (const [channelId, offset] of chunkIndex.messageIndexOffsets) {
      this.bufferBuilder.uint16(channelId).uint64(offset);
    }

    this.bufferBuilder
      .uint64(chunkIndex.messageIndexLength)
      .string(chunkIndex.compression)
      .uint64(chunkIndex.compressedSize)
      .uint64(chunkIndex.uncompressedSize)
      .uint32(0);

    const endPosition = this.bufferBuilder.length;
    this.bufferBuilder
      .seek(startPosition)
      .uint64(BigInt(endPosition - startPosition - 8))
      .seek(endPosition);

    return BigInt(endPosition - startPosition + 1);
  }

  writeMessageIndex(messageIndex: MessageIndex): void {
    this.bufferBuilder.uint8(Opcode.MESSAGE_INDEX);

    // each records tuple is a fixed byte length
    const messageIndexRecordsByteLength = messageIndex.records.length * 16;

    this.bufferBuilder
      .uint64(BigInt(2 + 4 + messageIndexRecordsByteLength))
      .uint16(messageIndex.channelId)
      .uint32(messageIndexRecordsByteLength);

    for (const record of messageIndex.records) {
      this.bufferBuilder.uint64(record[0]).uint64(record[1]);
    }
  }

  writeSummaryOffset(summaryOffset: SummaryOffset): bigint {
    this.bufferBuilder
      .uint8(Opcode.SUMMARY_OFFSET)
      .uint8(summaryOffset.groupOpcode)
      .uint64(summaryOffset.groupStart)
      .uint64(summaryOffset.groupEnd);

    return 24n;
  }
}
