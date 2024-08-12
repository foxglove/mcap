import type { McapTypes } from "@mcap/core";

/**
 * IReadable implementation for Blob (and File, which is a Blob).
 */
export class BlobReadable implements McapTypes.IReadable {
  #blob: Blob;

  public constructor(blob: Blob) {
    this.#blob = blob;
  }

  public async size(): Promise<number> {
    return this.#blob.size;
  }

  public async read(offset: number, size: number): Promise<Uint8Array> {
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
