import type { McapTypes } from "@mcap/core";
import type { FileHandle } from "node:fs/promises";

/**
 * IReadable implementation for FileHandle.
 */
export class FileHandleReadable implements McapTypes.IReadable {
  #handle: FileHandle;
  #buffer = new ArrayBuffer(4096);

  constructor(handle: FileHandle) {
    this.#handle = handle;
  }

  async size(): Promise<bigint> {
    return BigInt((await this.#handle.stat()).size);
  }

  async read(offset: bigint, length: bigint): Promise<Uint8Array> {
    if (offset > Number.MAX_SAFE_INTEGER || length > Number.MAX_SAFE_INTEGER) {
      throw new Error(`Read too large: offset ${offset}, length ${length}`);
    }
    if (length > this.#buffer.byteLength) {
      this.#buffer = new ArrayBuffer(Number(length * 2n));
    }
    const result = await this.#handle.read({
      buffer: new DataView(this.#buffer, 0, Number(length)),
      position: Number(offset),
    });
    if (result.bytesRead !== Number(length)) {
      throw new Error(
        `Read only ${result.bytesRead} bytes from offset ${offset}, expected ${length}`,
      );
    }
    return new Uint8Array(result.buffer.buffer, result.buffer.byteOffset, result.bytesRead);
  }
}
