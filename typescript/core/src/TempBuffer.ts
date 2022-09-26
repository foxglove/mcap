import { IWritable } from "./IWritable";
import { IReadable } from "./types";

export class TempBuffer implements IReadable, IWritable {
  private _buffer = new ArrayBuffer(1024);
  private _size = 0;

  position(): bigint {
    return BigInt(this._size);
  }
  async write(data: Uint8Array): Promise<void> {
    if (this._size + data.byteLength > this._buffer.byteLength) {
      const newBuffer = new ArrayBuffer(this._size + data.byteLength);
      new Uint8Array(newBuffer).set(new Uint8Array(this._buffer));
      this._buffer = newBuffer;
    }
    new Uint8Array(this._buffer, this._size).set(data);
    this._size += data.byteLength;
  }

  async size(): Promise<bigint> {
    return BigInt(this._size);
  }
  async read(offset: bigint, size: bigint): Promise<Uint8Array> {
    if (offset < 0n || offset + size > BigInt(this._buffer.byteLength)) {
      throw new Error("read out of range");
    }
    return new Uint8Array(this._buffer, Number(offset), Number(size));
  }

  get(): Uint8Array {
    return new Uint8Array(this._buffer, 0, this._size);
  }
}
