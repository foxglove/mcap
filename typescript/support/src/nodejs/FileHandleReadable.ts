import { FileHandle } from "fs/promises";

interface IReadable {
  size(): Promise<bigint>;
  read(offset: bigint, size: bigint): Promise<Uint8Array>;
}

/**
 * IReadable implementation for FileHandle.
 */
export class FileHandleReadable implements IReadable {
  private handle: FileHandle;
  private buffer = new ArrayBuffer(4096);

  constructor(handle: FileHandle) {
    this.handle = handle;
  }

  async size(): Promise<bigint> {
    const stat = await this.handle.stat();
    return BigInt(stat.size);
  }

  async read(offset: bigint, size: bigint): Promise<Uint8Array> {
    if (offset > Number.MAX_SAFE_INTEGER || size > Number.MAX_SAFE_INTEGER) {
      throw new Error(`Read too large: offset ${offset}, length ${length}`);
    }
    if (size > this.buffer.byteLength) {
      this.buffer = new ArrayBuffer(Number(size * 2n));
    }
    const result = await this.handle.read({
      buffer: new DataView(this.buffer, 0, Number(length)),
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
