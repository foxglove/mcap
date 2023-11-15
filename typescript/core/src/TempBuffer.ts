import { IWritable } from "./IWritable";
import { IReadable } from "./types";

export class TempBuffer implements IReadable, IWritable {
  #buffer = new ArrayBuffer(1024);
  #size = 0;

  position(): bigint {
    return BigInt(this.#size);
  }
  async write(data: Uint8Array): Promise<void> {
    if (this.#size + data.byteLength > this.#buffer.byteLength) {
      const newBuffer = new ArrayBuffer(this.#size + data.byteLength);
      new Uint8Array(newBuffer).set(new Uint8Array(this.#buffer));
      this.#buffer = newBuffer;
    }
    new Uint8Array(this.#buffer, this.#size).set(data);
    this.#size += data.byteLength;
  }

  async size(): Promise<bigint> {
    return BigInt(this.#size);
  }
  async read(offset: bigint, size: bigint): Promise<Uint8Array> {
    if (offset < 0n || offset + size > BigInt(this.#buffer.byteLength)) {
      throw new Error("read out of range");
    }
    return new Uint8Array(this.#buffer, Number(offset), Number(size));
  }

  get(): Uint8Array {
    return new Uint8Array(this.#buffer, 0, this.#size);
  }
}
