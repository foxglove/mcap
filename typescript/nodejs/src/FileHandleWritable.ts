import type { IWritable } from "@mcap/core";
import type { FileHandle } from "node:fs/promises";

/**
 * IWritable implementation for FileHandle.
 */
export class FileHandleWritable implements IWritable {
  #handle: FileHandle;
  #totalBytesWritten = 0;

  constructor(handle: FileHandle) {
    this.#handle = handle;
  }

  async write(buffer: Uint8Array): Promise<void> {
    const written = await this.#handle.write(buffer);
    this.#totalBytesWritten += written.bytesWritten;
  }

  position(): bigint {
    return BigInt(this.#totalBytesWritten);
  }
}
