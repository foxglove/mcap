import { FileHandle } from "fs/promises";

interface IWritable {
  write(buffer: Uint8Array): Promise<unknown>;
  position(): bigint;
}

/**
 * IWritable implementation for FileHandle.
 */
export class FileHandleWritable implements IWritable {
  private handle: FileHandle;
  private totalBytesWritten = 0;

  constructor(handle: FileHandle) {
    this.handle = handle;
  }

  async write(buffer: Uint8Array): Promise<void> {
    const written = await this.handle.write(buffer);
    this.totalBytesWritten += written.bytesWritten;
  }

  position(): bigint {
    return BigInt(this.totalBytesWritten);
  }
}
