import type { IReadable } from "./types.ts";

/**
 * Wraps an {@link IReadable} and caches the bytes returned from `read()` keyed by offset. Reads at
 * an offset that was previously cached return a subarray of the cached buffer instead of calling
 * the underlying readable.
 *
 * The cache is capped by `maxCacheSizeBytes`; once the cache is full, new reads pass through
 * without being cached. No eviction is performed.
 *
 * Note: reads are cached by *exact* offset. A read that partially overlaps a cached range but
 * starts at a different offset will miss.
 */
export class CachedReadable implements IReadable {
  /**
   * The underlying source of the data to be cached.
   */
  #readable: IReadable;
  /**
   * Cached data. Indexed by offset request and stored as a Uint8Array.
   * If the requested size is less than the cached data, the cached data is returned as a subarray.
   * If the requested size is greater than the cached data, the a new request is made to the underlying readable.
   */
  #cache = new Map<bigint, Uint8Array>();
  /**
   * The maximum size of the cache in bytes.
   */
  #maxCacheSizeBytes: number;
  /**
   * The current size of the cache in bytes.
   */
  #currentCacheSizeBytes = 0;
  /**
   * The size of the underlying readable.
   */
  #size: bigint | undefined;

  constructor(readable: IReadable, maxCacheSizeBytes: number) {
    this.#readable = readable;
    this.#maxCacheSizeBytes = maxCacheSizeBytes;
  }

  async size(): Promise<bigint> {
    if (this.#size == undefined) {
      this.#size = await this.#readable.size();
    }
    return this.#size;
  }

  async read(offset: bigint, size: bigint): Promise<Uint8Array> {
    const requestedSize = Number(size);
    const cached = this.#cache.get(offset);
    if (cached != undefined && cached.byteLength >= requestedSize) {
      return cached.byteLength === requestedSize ? cached : cached.subarray(0, requestedSize);
    }

    const data = await this.#readable.read(offset, size);

    // The underlying readable is allowed to reuse its backing buffer across reads, so we must copy
    // the bytes before storing them in the cache.
    if (this.#currentCacheSizeBytes + data.byteLength <= this.#maxCacheSizeBytes) {
      const copy = new Uint8Array(data);
      this.#cache.set(offset, copy);
      this.#currentCacheSizeBytes += copy.byteLength;
      return copy;
    }

    return data;
  }
}
