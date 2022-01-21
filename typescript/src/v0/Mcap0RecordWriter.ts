import { BufferedWriter } from "../common/BufferedWriter";
import { IWritable } from "../common/IWritable";
import { MCAP0_MAGIC, Opcode } from "./constants";
import {
  ChannelInfo,
  Header,
  Footer,
  Message,
  Attachment,
  Chunk,
  MessageIndex,
  ChunkIndex,
} from "./types";

/**
 * Mcap0RecordWriter provides methods to serialize mcap records to an IWritable.
 *
 * It makes no effort to ensure spec compatability on the order of records, this is the responsibility
 * of the caller.
 *
 * Unless you are building your own higher level writer interface, you'll likely want to use one of
 * the higher level writer interfaces.
 */
export class Mcap0RecordWriter {
  private recordPrefixWriter: BufferedWriter;
  private bufferedWriter: BufferedWriter;
  private writable: IWritable;

  constructor(writable: IWritable) {
    this.recordPrefixWriter = new BufferedWriter();
    this.bufferedWriter = new BufferedWriter();
    this.writable = writable;
  }

  async writeMagic(): Promise<void> {
    await this.writable.write(new Uint8Array(MCAP0_MAGIC));
  }

  async writeHeader(header: Header): Promise<void> {
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

    await this.writable.write(this.recordPrefixWriter.buffer);
    await this.writable.write(this.bufferedWriter.buffer);
    await this.writable.write(keyValueWriter.buffer);

    this.recordPrefixWriter.reset();
    this.bufferedWriter.reset();
  }

  async writeFooter(footer: Footer): Promise<void> {
    this.recordPrefixWriter.uint8(Opcode.FOOTER);
    this.recordPrefixWriter.uint64(12n); // footer is fixed length
    this.recordPrefixWriter.uint64(footer.indexOffset);
    this.recordPrefixWriter.uint32(footer.indexCrc);

    await this.writable.write(this.recordPrefixWriter.buffer);

    this.recordPrefixWriter.reset();
  }

  async writeChannelInfo(info: ChannelInfo): Promise<void> {
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

    await this.writable.write(this.recordPrefixWriter.buffer);
    await this.writable.write(this.bufferedWriter.buffer);
    await this.writable.write(keyValueWriter.buffer);

    this.recordPrefixWriter.reset();
    this.bufferedWriter.reset();
  }

  async writeMessage(message: Message): Promise<void> {
    this.bufferedWriter.uint16(message.channelId);
    this.bufferedWriter.uint32(message.sequence);
    this.bufferedWriter.uint64(message.publishTime);
    this.bufferedWriter.uint64(message.recordTime);

    this.recordPrefixWriter.uint8(Opcode.MESSAGE);
    this.recordPrefixWriter.uint64(
      BigInt(this.bufferedWriter.length + message.messageData.byteLength),
    );

    await this.writable.write(this.recordPrefixWriter.buffer);
    await this.writable.write(this.bufferedWriter.buffer);
    await this.writable.write(message.messageData);

    this.recordPrefixWriter.reset();
    this.bufferedWriter.reset();
  }

  async writeAttachment(attachment: Attachment): Promise<void> {
    this.bufferedWriter.string(attachment.name);
    this.bufferedWriter.uint64(attachment.recordTime);
    this.bufferedWriter.string(attachment.contentType);

    this.recordPrefixWriter.uint8(Opcode.CHANNEL_INFO);
    this.recordPrefixWriter.uint64(BigInt(this.bufferedWriter.length + attachment.data.byteLength));

    await this.writable.write(this.recordPrefixWriter.buffer);
    await this.writable.write(this.bufferedWriter.buffer);
    await this.writable.write(attachment.data);

    this.recordPrefixWriter.reset();
    this.bufferedWriter.reset();
  }

  async writeChunk(chunk: Chunk): Promise<void> {
    this.bufferedWriter.uint64(chunk.uncompressedSize);
    this.bufferedWriter.uint32(chunk.uncompressedCrc);
    this.bufferedWriter.string(chunk.compression);

    this.recordPrefixWriter.uint8(Opcode.CHUNK);
    this.recordPrefixWriter.uint64(BigInt(this.bufferedWriter.length + chunk.records.byteLength));

    await this.writable.write(this.recordPrefixWriter.buffer);
    await this.writable.write(this.bufferedWriter.buffer);
    await this.writable.write(chunk.records);

    this.recordPrefixWriter.reset();
    this.bufferedWriter.reset();
  }

  async writeMessageIndex(messageIndex: MessageIndex): Promise<void> {
    this.bufferedWriter.uint16(messageIndex.channelId);
    this.bufferedWriter.uint32(messageIndex.count);

    const arrayWriter = new BufferedWriter();
    for (const record of messageIndex.records) {
      arrayWriter.uint64(record[0]);
      arrayWriter.uint64(record[1]);
    }

    // crc
    arrayWriter.uint32(0);

    this.bufferedWriter.uint32(arrayWriter.length);

    this.recordPrefixWriter.uint8(Opcode.MESSAGE_INDEX);
    this.recordPrefixWriter.uint64(BigInt(this.bufferedWriter.length + arrayWriter.length));

    await this.writable.write(this.recordPrefixWriter.buffer);
    await this.writable.write(this.bufferedWriter.buffer);
    await this.writable.write(arrayWriter.buffer);

    this.recordPrefixWriter.reset();
    this.bufferedWriter.reset();
  }

  async writeChunkIndex(chunkIndex: ChunkIndex): Promise<void> {
    this.bufferedWriter.uint64(chunkIndex.startTime);
    this.bufferedWriter.uint64(chunkIndex.endTime);
    this.bufferedWriter.uint64(chunkIndex.chunkOffset);

    const arrayWriter = new BufferedWriter();
    for (const [channelId, offset] of chunkIndex.messageIndexOffsets) {
      arrayWriter.uint16(channelId);
      arrayWriter.uint64(offset);
    }

    this.bufferedWriter.uint32(arrayWriter.length);
    this.bufferedWriter.bytes(arrayWriter.buffer);

    this.bufferedWriter.uint64(chunkIndex.messageIndexLength);
    this.bufferedWriter.string(chunkIndex.compression);
    this.bufferedWriter.uint64(chunkIndex.compressedSize);
    this.bufferedWriter.uint64(chunkIndex.uncompressedSize);

    // crc
    this.bufferedWriter.uint32(0);

    this.recordPrefixWriter.uint8(Opcode.CHUNK_INDEX);
    this.recordPrefixWriter.uint64(BigInt(this.bufferedWriter.length));

    await this.writable.write(this.recordPrefixWriter.buffer);
    await this.writable.write(this.bufferedWriter.buffer);

    this.recordPrefixWriter.reset();
    this.bufferedWriter.reset();
  }
}
