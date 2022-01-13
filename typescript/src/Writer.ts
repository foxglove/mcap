export const LITTLE_ENDIAN = true;

export default class Writer {
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
