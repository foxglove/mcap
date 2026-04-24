import type { IReadable } from "./types.ts";

/**
 * Wraps an {@link IReadable} and caches the bytes returned from `read()` keyed by offset. Reads at
 * an offset that was previously cached return a subarray of the cached buffer instead of calling
 * the underlying readable.
 *
 * The cache is capped by `maxCacheSizeBytes`; once the cache is full, new reads pass through
 * without being cached. No eviction is performed.
 *
 * Concurrent reads for the same offset are deduplicated: the second caller joins the first
 * in-flight read rather than issuing a redundant underlying read.
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
   * In-flight reads keyed by offset so concurrent callers can await the same promise.
   */
  #pending = new Map<bigint, Promise<Uint8Array>>();
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
  supportsConcurrentReads = true;

  constructor(readable: IReadable, maxCacheSizeBytes: number) {
    this.#readable = readable;
    this.#maxCacheSizeBytes = maxCacheSizeBytes;
  }

  async size(): Promise<bigint> {
    return (this.#size ??= await this.#readable.size());
  }

  async read(offset: bigint, size: bigint): Promise<Uint8Array> {
    const requestedSize = Number(size);

    const cached = this.#cache.get(offset);
    if (cached != undefined && cached.byteLength >= requestedSize) {
      return cached.byteLength === requestedSize ? cached : cached.subarray(0, requestedSize);
    }

    // Join an in-flight read for this offset if one is already pending.
    const pending = this.#pending.get(offset);
    if (pending != undefined) {
      const data = await pending;
      if (data.byteLength >= requestedSize) {
        return data.byteLength === requestedSize ? data : data.subarray(0, requestedSize);
      }
      // The pending read was for fewer bytes; fall through to issue a new read.
    }

    const readPromise = (async () => {
      try {
        const data = await this.#readable.read(offset, size);
        // Always copy to produce a stable buffer for concurrent waiters and to prevent
        // corruption if the underlying readable reuses its backing buffer.
        const copy = new Uint8Array(data);
        const existing = this.#cache.get(offset);
        const existingSize = existing?.byteLength ?? 0;
        const newTotalSize = this.#currentCacheSizeBytes - existingSize + copy.byteLength;
        if (newTotalSize <= this.#maxCacheSizeBytes) {
          this.#cache.set(offset, copy);
          this.#currentCacheSizeBytes = newTotalSize;
        }
        return copy;
      } finally {
        this.#pending.delete(offset);
      }
    })();

    this.#pending.set(offset, readPromise);
    return await readPromise;
  }
}
