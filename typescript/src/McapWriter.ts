import { open, FileHandle } from "fs/promises";

import { MCAP_MAGIC, RecordType } from "./constants";
import { ChannelInfo, McapRecord, Message } from "./types";

const LITTLE_ENDIAN = true;

class Writer {
  buffer: ArrayBuffer;
  private view: DataView;
  private offset = 0;
  private textEncoder = new TextEncoder();

  constructor(scratchBuffer?: ArrayBuffer) {
    this.buffer = scratchBuffer ?? new ArrayBuffer(4096);
    this.view = new DataView(this.buffer);
  }

  size(): number {
    return this.offset;
  }

  ensureCapacity(capacity: number): void {
    if (this.offset + capacity >= this.buffer.byteLength) {
      const newBuffer = new ArrayBuffer(this.buffer.byteLength * 2);
      new Uint8Array(newBuffer).set(new Uint8Array(this.buffer));
      this.buffer = newBuffer;
      this.view = new DataView(newBuffer);
    }
  }
  int8(value: number): void {
    this.ensureCapacity(1);
    this.view.setInt8(this.offset, value);
    this.offset += 1;
  }
  uint8(value: number): void {
    this.ensureCapacity(1);
    this.view.setUint8(this.offset, value);
    this.offset += 1;
  }
  int16(value: number): void {
    this.ensureCapacity(2);
    this.view.setInt16(this.offset, value, LITTLE_ENDIAN);
    this.offset += 2;
  }
  uint16(value: number): void {
    this.ensureCapacity(2);
    this.view.setUint16(this.offset, value, LITTLE_ENDIAN);
    this.offset += 2;
  }
  int32(value: number): void {
    this.ensureCapacity(4);
    this.view.setInt32(this.offset, value, LITTLE_ENDIAN);
    this.offset += 4;
  }
  uint32(value: number): void {
    this.ensureCapacity(4);
    this.view.setUint32(this.offset, value, LITTLE_ENDIAN);
    this.offset += 4;
  }
  int64(value: bigint): void {
    this.ensureCapacity(8);
    this.view.setBigInt64(this.offset, value, LITTLE_ENDIAN);
    this.offset += 8;
  }
  uint64(value: bigint): void {
    this.ensureCapacity(8);
    this.view.setBigUint64(this.offset, value, LITTLE_ENDIAN);
    this.offset += 8;
  }
  string(value: string): void {
    this.uint32(value.length);
    const stringBytes = this.textEncoder.encode(value);
    this.ensureCapacity(stringBytes.byteLength);
    new Uint8Array(this.buffer, this.offset, stringBytes.byteLength).set(stringBytes);
    this.offset += stringBytes.length;
  }

  toUint8(): Uint8Array {
    return new Uint8Array(this.buffer, 0, this.size());
  }
}

export default class McapWriter {
  private writeStream?: FileHandle;

  async open(pathname: string): Promise<void> {
    this.writeStream = await open(pathname, "w");

    // write the magic
    // 0x89, M, C, A, P, \r, \n, \n
    await this.writeStream.write(new Uint8Array(MCAP_MAGIC));

    // write the format version
    await this.writeStream.write(new Uint8Array([1]));
  }

  async write(record: McapRecord): Promise<void> {
    switch (record.type) {
      case "ChannelInfo":
        await this.writeChannelInfoRecord(record);
        break;
      case "Message":
        await this.writeMessageRecord(record);
        break;
      default:
        throw new Error(`Unsupported record type: ${record.type}`);
    }
  }

  async end(): Promise<void> {
    // write the footer
    const serializer = new Writer();
    serializer.uint8(RecordType.FOOTER);
    serializer.uint64(0n);
    serializer.uint32(0);
    await this.writeStream?.write(serializer.toUint8());

    await this.writeStream?.close();
  }

  private async writeChannelInfoRecord(info: ChannelInfo): Promise<void> {
    const serializer = new Writer();
    serializer.uint32(info.id);
    serializer.string(info.topic);
    serializer.string(info.encoding);
    serializer.string(info.schemaName);
    serializer.string(info.schema);

    const preamble = new Writer();
    preamble.uint8(RecordType.CHANNEL_INFO);
    preamble.uint32(serializer.size());

    await this.writeStream?.write(preamble.toUint8());
    await this.writeStream?.write(serializer.toUint8());
  }

  private async writeMessageRecord(message: Message): Promise<void> {
    const serializer = new Writer();
    serializer.uint32(message.channelInfo.id);
    serializer.uint64(message.timestamp);

    const preamble = new Writer();
    preamble.uint8(RecordType.MESSAGE);
    preamble.uint32(serializer.size() + message.data.byteLength);

    await this.writeStream?.write(preamble.toUint8());
    await this.writeStream?.write(serializer.toUint8());
    await this.writeStream?.write(new Uint8Array(message.data));
  }
}
