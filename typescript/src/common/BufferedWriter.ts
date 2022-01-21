const LITTLE_ENDIAN = true;

export class BufferedWriter {
  private fullBuffer = new Uint8Array(4096);
  private view: DataView;
  private textEncoder = new TextEncoder();
  private offset = 0;

  constructor() {
    this.view = new DataView(this.fullBuffer.buffer);
  }

  get length(): number {
    return this.offset;
  }

  get buffer(): Readonly<Uint8Array> {
    return this.fullBuffer.slice(0, this.offset);
  }

  int8(value: number): void {
    this.ensureAdditionalCapacity(1);
    this.view.setInt8(this.offset, value);
    this.offset += 1;
  }
  uint8(value: number): void {
    this.ensureAdditionalCapacity(1);
    this.view.setUint8(this.offset, value);
    this.offset += 1;
  }
  int16(value: number): void {
    this.ensureAdditionalCapacity(2);
    this.view.setInt16(this.offset, value, LITTLE_ENDIAN);
    this.offset += 2;
  }
  uint16(value: number): void {
    this.ensureAdditionalCapacity(2);
    this.view.setUint16(this.offset, value, LITTLE_ENDIAN);
    this.offset += 2;
  }
  int32(value: number): void {
    this.ensureAdditionalCapacity(4);
    this.view.setInt32(this.offset, value, LITTLE_ENDIAN);
    this.offset += 4;
  }
  uint32(value: number): void {
    this.ensureAdditionalCapacity(4);
    this.view.setUint32(this.offset, value, LITTLE_ENDIAN);
    this.offset += 4;
  }
  int64(value: bigint): void {
    this.ensureAdditionalCapacity(8);
    this.view.setBigInt64(this.offset, value, LITTLE_ENDIAN);
    this.offset += 8;
  }
  uint64(value: bigint): void {
    this.ensureAdditionalCapacity(8);
    this.view.setBigUint64(this.offset, value, LITTLE_ENDIAN);
    this.offset += 8;
  }
  string(value: string): void {
    const stringBytes = this.textEncoder.encode(value);
    this.ensureAdditionalCapacity(stringBytes.byteLength + 4);
    this.uint32(value.length);
    this.fullBuffer.set(stringBytes, this.offset);
    this.offset += stringBytes.length;
  }
  bytes(buffer: Uint8Array): void {
    this.ensureAdditionalCapacity(buffer.byteLength);
    this.fullBuffer.set(buffer, this.offset);
    this.offset += buffer.length;
  }

  reset(): void {
    this.offset = 0;
  }

  private ensureAdditionalCapacity(capacity: number): void {
    if (this.offset + capacity >= this.fullBuffer.byteLength) {
      const needCapacity = this.offset + capacity - this.fullBuffer.byteLength;
      const newBuffer = new Uint8Array((this.fullBuffer.byteLength + needCapacity) * 2);
      newBuffer.set(this.fullBuffer);

      this.fullBuffer = newBuffer;
      this.view = new DataView(this.fullBuffer.buffer);
    }
  }
}
