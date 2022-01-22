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
 * Mcap0BufferRecordWriter provides methods to serialize mcap records to a buffer in memory.
 *
 * It makes no effort to ensure spec compatability on the order of records, this is the responsibility
 * of the caller.
 *
 * You'll likely want to use one of the higher level writer interfaces unless you are building your
 * own higher level writing interface.
 */
export class Mcap0BufferRecordWriter {
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
      .uint64(12n) // footer is fixed length
      .uint64(footer.indexOffset)
      .uint32(footer.indexCrc);
  }

  writeChannelInfo(info: ChannelInfo): void {
    this.bufferBuilder.uint8(Opcode.CHANNEL_INFO);

    const startPosition = this.bufferBuilder.length;
    this.bufferBuilder
      .uint64(0n) // placeholder
      .uint16(info.channelId)
      .string(info.topicName)
      .string(info.encoding)
      .string(info.schemaName)
      .string(info.schema)
      .array(info.userData)
      .uint32(0); // crc

    const endPosition = this.bufferBuilder.length;
    this.bufferBuilder
      .seek(startPosition)
      .uint64(BigInt(endPosition - startPosition - 8))
      .seek(endPosition);
  }

  writeMessage(message: Message): void {
    this.bufferBuilder
      .uint8(Opcode.MESSAGE)
      .uint64(BigInt(2 + 4 + 8 + 8 + message.messageData.byteLength))
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

  writeChunk(chunk: Chunk): void {
    this.bufferBuilder.uint8(Opcode.CHUNK);

    const startPosition = this.bufferBuilder.length;
    this.bufferBuilder
      .uint64(0n) // palceholder
      .uint64(chunk.uncompressedSize)
      .uint32(chunk.uncompressedCrc)
      .string(chunk.compression)
      .bytes(chunk.records);

    const endPosition = this.bufferBuilder.length;
    this.bufferBuilder
      .seek(startPosition)
      .uint64(BigInt(endPosition - startPosition - 8))
      .seek(endPosition);
  }

  writeChunkIndex(chunkIndex: ChunkIndex): void {
    this.bufferBuilder.uint8(Opcode.CHUNK_INDEX);

    const startPosition = this.bufferBuilder.length;
    this.bufferBuilder
      .uint64(0n) // placeholder
      .uint64(chunkIndex.startTime)
      .uint64(chunkIndex.endTime)
      .uint64(chunkIndex.chunkOffset)
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
  }

  writeMessageIndex(messageIndex: MessageIndex): void {
    this.bufferBuilder.uint8(Opcode.MESSAGE_INDEX);

    const messageIndexRecordsByteLength = messageIndex.records.length * 16;

    this.bufferBuilder
      .uint64(BigInt(2 + 4 + 4 + messageIndexRecordsByteLength + 1))
      .uint16(messageIndex.channelId)
      .uint32(messageIndex.count)
      .uint32(messageIndexRecordsByteLength);

    for (const record of messageIndex.records) {
      this.bufferBuilder.uint64(record[0]).uint64(record[1]);
    }

    // crc
    this.bufferBuilder.uint32(0);
  }
}
