import type { IReadable } from "@mcap/core";

/**
 * IReadable implementation for Blob (and File, which is a Blob).
 */
export class BlobReadable implements IReadable {
  #blob: Blob;

  public constructor(blob: Blob) {
    this.#blob = blob;
  }

  public async size(): Promise<bigint> {
    return BigInt(this.#blob.size);
  }

  public async read(offset: bigint, size: bigint): Promise<Uint8Array> {
    if (offset + size > this.#blob.size) {
      throw new Error(
        `Read of ${size} bytes at offset ${offset} exceeds file size ${this.#blob.size}`,
      );
    }
    return new Uint8Array(
      await this.#blob.slice(Number(offset), Number(offset + size)).arrayBuffer(),
    );
  }
}
