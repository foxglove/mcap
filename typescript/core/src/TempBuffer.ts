import type { ISeekableWriter } from "./ISeekableWriter.ts";
import type { IWritable } from "./IWritable.ts";
import type { IReadable } from "./types.ts";

/**
 * In-memory buffer used for reading and writing MCAP files in tests. Can be used as both an IReadable and an IWritable.
 */
export class TempBuffer implements IReadable, IWritable, ISeekableWriter {
  #buffer = new ArrayBuffer(0);
  #position = 0;

  constructor(source?: ArrayBufferView | ArrayBuffer) {
    if (source instanceof ArrayBuffer) {
      this.#buffer = source;
    } else if (source) {
      const copy = new Uint8Array(source.byteLength);
      copy.set(new Uint8Array(source.buffer, source.byteOffset, source.byteLength));
      this.#buffer = copy.buffer;
    }
  }

  #setCapacity(capacity: number) {
    if (this.#buffer.byteLength !== capacity) {
      const newBuffer = new ArrayBuffer(capacity);
      new Uint8Array(newBuffer).set(
        new Uint8Array(this.#buffer, 0, Math.min(this.#buffer.byteLength, capacity)),
      );
      this.#buffer = newBuffer;
    }
  }

  position(): bigint {
    return BigInt(this.#position);
  }

  async seek(position: bigint): Promise<void> {
    if (position < 0n) {
      throw new Error(`Attempted to seek to negative position ${position}`);
    } else if (position > this.#buffer.byteLength) {
      this.#setCapacity(Number(position));
    }
    this.#position = Number(position);
  }

  async truncate(): Promise<void> {
    const newBuffer = new ArrayBuffer(this.#position);
    new Uint8Array(newBuffer).set(new Uint8Array(this.#buffer, 0, this.#position));
    this.#buffer = newBuffer;
  }

  async write(data: Uint8Array): Promise<void> {
    if (this.#position + data.byteLength > this.#buffer.byteLength) {
      this.#setCapacity(this.#position + data.byteLength);
    }
    new Uint8Array(this.#buffer, this.#position).set(data);
    this.#position += data.byteLength;
  }

  async size(): Promise<bigint> {
    return BigInt(this.#buffer.byteLength);
  }

  async read(offset: bigint, size: bigint): Promise<Uint8Array> {
    if (offset < 0n || offset + size > BigInt(this.#buffer.byteLength)) {
      throw new Error("read out of range");
    }
    return new Uint8Array(this.#buffer, Number(offset), Number(size));
  }

  get(): Uint8Array {
    return new Uint8Array(this.#buffer);
  }
}
