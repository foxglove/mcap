import { BufferedWriter } from "../common/BufferedWriter";
import { MCAP0_MAGIC, Opcode } from "./constants";
import { ChannelInfo, Header, Footer, Message, Attachment, Chunk } from "./types";

// fixme - move to separate file
// this is really similar to the BufferedWriter
// BufferedWritter is really some sort of builder
export class MemoryWritable {
  private fullBuffer = new Uint8Array(4096);
  private offset = 0;

  get length(): number {
    return this.offset;
  }

  get buffer(): Uint8Array {
    return this.fullBuffer.slice(0, this.offset);
  }

  ensureAdditionalCapacity(capacity: number): void {
    if (this.offset + capacity >= this.fullBuffer.byteLength) {
      const needCapacity = this.offset + capacity - this.fullBuffer.byteLength;
      const newBuffer = new Uint8Array((this.fullBuffer.byteLength + needCapacity) * 2);
      newBuffer.set(this.fullBuffer);

      this.fullBuffer = newBuffer;
    }
  }

  write(buffer: Uint8Array): void {
    this.ensureAdditionalCapacity(buffer.byteLength);
    this.fullBuffer.set(buffer, this.offset);
    this.offset += buffer.length;
  }

  reset(): void {
    this.offset = 0;
  }
}

/**
 * Mcap0MemoryRecordWriter provides methods to serialize mcap records to a buffer in memory.
 *
 * It makes no effort to ensure spec compatability on the order of records, this is the responsibility
 * of the caller.
 *
 * You'll likely want to use one of the higher level writer interfaces unless you are building your
 * own higher level writing interface.
 */
export class Mcap0MemoryRecordWriter {
  private recordPrefixWriter: BufferedWriter;
  private bufferedWriter: BufferedWriter;
  private writable: MemoryWritable;

  constructor(writable: MemoryWritable) {
    this.recordPrefixWriter = new BufferedWriter();
    this.bufferedWriter = new BufferedWriter();
    this.writable = writable;
  }

  writeMagic(): void {
    this.writable.write(new Uint8Array(MCAP0_MAGIC));
  }

  writeHeader(header: Header): void {
    this.bufferedWriter.string(header.profile);
    this.bufferedWriter.string(header.library);

    const keyValueWriter = new BufferedWriter();
    for (const item of header.metadata) {
      const [key, value] = item;
      keyValueWriter.string(key);
      keyValueWriter.string(value);
    }

    this.bufferedWriter.uint32(keyValueWriter.length);

    this.recordPrefixWriter.uint8(Opcode.HEADER);
    this.recordPrefixWriter.uint64(BigInt(this.bufferedWriter.length + keyValueWriter.length));

    this.writable.write(this.recordPrefixWriter.buffer);
    this.writable.write(this.bufferedWriter.buffer);
    this.writable.write(keyValueWriter.buffer);

    this.recordPrefixWriter.reset();
    this.bufferedWriter.reset();
  }

  writeFooter(footer: Footer): void {
    this.recordPrefixWriter.uint8(Opcode.FOOTER);
    this.recordPrefixWriter.uint64(12n); // footer is fixed length
    this.recordPrefixWriter.uint64(footer.indexOffset);
    this.recordPrefixWriter.uint32(footer.indexCrc);

    this.writable.write(this.recordPrefixWriter.buffer);

    this.recordPrefixWriter.reset();
  }

  writeChannelInfo(info: ChannelInfo): void {
    this.bufferedWriter.uint16(info.channelId);
    this.bufferedWriter.string(info.topicName);
    this.bufferedWriter.string(info.encoding);
    this.bufferedWriter.string(info.schemaName);
    this.bufferedWriter.string(info.schema);

    const keyValueWriter = new BufferedWriter();
    for (const item of info.userData) {
      const [key, value] = item;
      keyValueWriter.string(key);
      keyValueWriter.string(value);
    }

    this.bufferedWriter.uint32(keyValueWriter.length);

    // Add crc to keyValueWriter after adding the length of key/values to the bufferWriter
    // This allows the crc to serialize our with the keyValueWriter
    keyValueWriter.uint32(0);

    this.recordPrefixWriter.uint8(Opcode.CHANNEL_INFO);
    this.recordPrefixWriter.uint64(BigInt(this.bufferedWriter.length + keyValueWriter.length));

    this.writable.write(this.recordPrefixWriter.buffer);
    this.writable.write(this.bufferedWriter.buffer);
    this.writable.write(keyValueWriter.buffer);

    this.recordPrefixWriter.reset();
    this.bufferedWriter.reset();
  }

  writeMessage(message: Message): void {
    this.bufferedWriter.uint16(message.channelId);
    this.bufferedWriter.uint32(message.sequence);
    this.bufferedWriter.uint64(message.publishTime);
    this.bufferedWriter.uint64(message.recordTime);

    this.recordPrefixWriter.uint8(Opcode.MESSAGE);
    this.recordPrefixWriter.uint64(
      BigInt(this.bufferedWriter.length + message.messageData.byteLength),
    );

    this.writable.write(this.recordPrefixWriter.buffer);
    this.writable.write(this.bufferedWriter.buffer);
    this.writable.write(message.messageData);

    this.recordPrefixWriter.reset();
    this.bufferedWriter.reset();
  }

  writeAttachment(attachment: Attachment): void {
    this.bufferedWriter.string(attachment.name);
    this.bufferedWriter.uint64(attachment.recordTime);
    this.bufferedWriter.string(attachment.contentType);

    this.recordPrefixWriter.uint8(Opcode.ATTACHMENT);
    this.recordPrefixWriter.uint64(BigInt(this.bufferedWriter.length + attachment.data.byteLength));

    this.writable.write(this.recordPrefixWriter.buffer);
    this.writable.write(this.bufferedWriter.buffer);
    this.writable.write(attachment.data);

    this.recordPrefixWriter.reset();
    this.bufferedWriter.reset();
  }

  writeChunk(chunk: Chunk): void {
    this.bufferedWriter.uint64(chunk.uncompressedSize);
    this.bufferedWriter.uint32(chunk.uncompressedCrc);
    this.bufferedWriter.string(chunk.compression);

    this.recordPrefixWriter.uint8(Opcode.CHUNK);
    this.recordPrefixWriter.uint64(BigInt(this.bufferedWriter.length + chunk.records.byteLength));

    this.writable.write(this.recordPrefixWriter.buffer);
    this.writable.write(this.bufferedWriter.buffer);
    this.writable.write(chunk.records);

    this.recordPrefixWriter.reset();
    this.bufferedWriter.reset();
  }
}
