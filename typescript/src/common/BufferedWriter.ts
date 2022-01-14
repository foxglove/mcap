import { IWritable } from "../v0";

const LITTLE_ENDIAN = true;

export class BufferedWriter {
  private buffer = new Uint8Array(4096);
  private view: DataView;
  private textEncoder = new TextEncoder();
  private offset = 0;

  constructor() {
    this.view = new DataView(this.buffer.buffer);
  }

  size(): number {
    return this.offset;
  }

  ensureCapacity(capacity: number): void {
    if (this.offset + capacity >= this.buffer.byteLength) {
      const newBuffer = new Uint8Array(this.buffer.byteLength * 2);
      newBuffer.set(this.buffer);

      this.buffer = newBuffer;
      this.view = new DataView(this.buffer.buffer);
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

  async flush(writable: IWritable): Promise<void> {
    if (this.offset === 0) {
      return;
    }

    this.offset = 0;
    await writable.write(new Uint8Array(this.buffer, 0, this.offset));
  }
}
