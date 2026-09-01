import { CachedReadable } from "./CachedReadable.ts";
import type { IReadable } from "./types.ts";

/**
 * Create a readable that simulates buffer reuse, like some IReadable implementations do.
 * Tracks read calls so tests can verify cache behavior.
 */
function makeReadable(data: Uint8Array): IReadable & { reads: { offset: bigint; size: bigint }[] } {
  const reusableBuffer = new Uint8Array(data.byteLength);
  const reads: { offset: bigint; size: bigint }[] = [];
  return {
    reads,
    size: async () => BigInt(data.byteLength),
    read: async (offset, size) => {
      reads.push({ offset, size });
      reusableBuffer.set(
        new Uint8Array(data.buffer, data.byteOffset + Number(offset), Number(size)),
      );
      reusableBuffer.fill(0xff, Number(size));
      return new Uint8Array(reusableBuffer.buffer, 0, Number(size));
    },
  };
}

describe("CachedReadable", () => {
  it("returns cached bytes without re-reading", async () => {
    const data = new Uint8Array([0, 1, 2, 3, 4, 5, 6, 7]);
    const readable = makeReadable(data);
    const cached = new CachedReadable(readable, 1024);

    const first = await cached.read(2n, 4n);
    expect(Array.from(first)).toEqual([2, 3, 4, 5]);
    expect(readable.reads).toHaveLength(1);

    const second = await cached.read(2n, 4n);
    expect(Array.from(second)).toEqual([2, 3, 4, 5]);
    expect(readable.reads).toHaveLength(1);
  });

  it("copies bytes on cache so underlying buffer reuse does not corrupt cache", async () => {
    const data = new Uint8Array([0, 1, 2, 3, 4, 5, 6, 7]);
    const readable = makeReadable(data);
    const cached = new CachedReadable(readable, 1024);

    const first = await cached.read(0n, 4n);
    expect(Array.from(first)).toEqual([0, 1, 2, 3]);

    // A different read would overwrite a reused buffer; make sure the first cached entry
    // is unaffected.
    await cached.read(4n, 4n);
    const firstAgain = await cached.read(0n, 4n);
    expect(Array.from(firstAgain)).toEqual([0, 1, 2, 3]);
  });

  it("returns a prefix when cached range is larger than requested", async () => {
    const data = new Uint8Array([0, 1, 2, 3, 4, 5, 6, 7]);
    const readable = makeReadable(data);
    const cached = new CachedReadable(readable, 1024);

    await cached.read(0n, 8n);
    expect(readable.reads).toHaveLength(1);

    const prefix = await cached.read(0n, 3n);
    expect(Array.from(prefix)).toEqual([0, 1, 2]);
    expect(readable.reads).toHaveLength(1);
  });

  it("reads through without caching when full", async () => {
    const data = new Uint8Array([0, 1, 2, 3, 4, 5, 6, 7]);
    const readable = makeReadable(data);
    const cached = new CachedReadable(readable, 4);

    await cached.read(0n, 4n);
    expect(readable.reads).toHaveLength(1);

    // Second read should not fit; passes through but is not cached.
    await cached.read(4n, 4n);
    expect(readable.reads).toHaveLength(2);
    await cached.read(4n, 4n);
    expect(readable.reads).toHaveLength(3);

    // The first-cached entry is still available.
    await cached.read(0n, 4n);
    expect(readable.reads).toHaveLength(3);
  });

  it("concurrent reads for the same offset issue only one underlying read", async () => {
    const data = new Uint8Array([0, 1, 2, 3, 4, 5, 6, 7]);
    const readable = makeReadable(data);
    const cached = new CachedReadable(readable, 1024);

    const [a, b] = await Promise.all([cached.read(0n, 4n), cached.read(0n, 4n)]);
    expect(Array.from(a)).toEqual([0, 1, 2, 3]);
    expect(Array.from(b)).toEqual([0, 1, 2, 3]);
    expect(readable.reads).toHaveLength(1);
  });

  it("concurrent reads for the same offset do not double-count cache size", async () => {
    const data = new Uint8Array([0, 1, 2, 3, 4, 5, 6, 7]);
    const readable = makeReadable(data);
    // Cache fits exactly one 4-byte read.
    const cached = new CachedReadable(readable, 4);

    await Promise.all([cached.read(0n, 4n), cached.read(0n, 4n)]);
    expect(readable.reads).toHaveLength(1);

    // Cache should now be exactly full; a different offset should still be readable on-demand.
    await cached.read(4n, 4n);
    expect(readable.reads).toHaveLength(2);
  });

  it("re-reading the same offset with a larger size does not block caching other offsets", async () => {
    const data = new Uint8Array(16);
    for (let i = 0; i < 16; i++) {
      data[i] = i;
    }
    const readable = makeReadable(data);
    const cached = new CachedReadable(readable, 12);

    await cached.read(0n, 4n);
    expect(readable.reads).toHaveLength(1);

    await cached.read(0n, 8n);
    expect(readable.reads).toHaveLength(2);

    await cached.read(4n, 4n);
    expect(readable.reads).toHaveLength(3);

    await cached.read(4n, 4n);
    expect(readable.reads).toHaveLength(3);
  });

  it("cleans up pending entry on read failure so retries can succeed", async () => {
    const data = new Uint8Array([0, 1, 2, 3]);
    let failNext = true;
    const readable: IReadable & { reads: number } = {
      reads: 0,
      size: async () => BigInt(data.byteLength),
      read: async (offset, size) => {
        readable.reads++;
        if (failNext) {
          failNext = false;
          throw new Error("transient error");
        }
        return new Uint8Array(data.buffer, Number(offset), Number(size));
      },
    };
    const cached = new CachedReadable(readable, 1024);

    await expect(cached.read(0n, 4n)).rejects.toThrow("transient error");
    expect(readable.reads).toBe(1);

    // After the failure the pending entry is gone; the next read retries the underlying readable.
    const result = await cached.read(0n, 4n);
    expect(Array.from(result)).toEqual([0, 1, 2, 3]);
    expect(readable.reads).toBe(2);
  });

  it("memoizes size()", async () => {
    const readable = makeReadable(new Uint8Array(16));
    let sizeCalls = 0;
    const wrapped: IReadable = {
      size: async () => {
        sizeCalls++;
        return await readable.size();
      },
      read: readable.read.bind(readable),
    };
    const cached = new CachedReadable(wrapped, 1024);
    expect(await cached.size()).toBe(16n);
    expect(await cached.size()).toBe(16n);
    expect(sizeCalls).toBe(1);
  });
});
