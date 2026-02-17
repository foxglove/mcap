import type { DecompressHandlers } from "@mcap/core";

let handlersPromise: Promise<DecompressHandlers> | undefined;
export async function loadDecompressHandlers(): Promise<DecompressHandlers> {
  return await (handlersPromise ??= _loadDecompressHandlers());
}

// eslint-disable-next-line no-underscore-dangle
async function _loadDecompressHandlers(): Promise<DecompressHandlers> {
  const [decompressZstd, decompressLZ4, bzip2] = await Promise.all([
    import("@foxglove/wasm-zstd").then(async (mod) => {
      await mod.isLoaded;
      return mod.decompress;
    }),
    import("@foxglove/wasm-lz4").then(async (mod) => {
      await mod.default.isLoaded;
      return mod.default;
    }),
    // CJS module consumed from NodeNext ESM: class is nested under default.default
    import("@foxglove/wasm-bz2").then(async (mod) => {
      return await mod.default.default.init();
    }),
  ]);

  return {
    lz4: (buffer, decompressedSize) =>
      new Uint8Array(decompressLZ4(buffer, Number(decompressedSize))),

    bz2: (buffer, decompressedSize) =>
      bzip2.decompress(buffer, Number(decompressedSize), { small: false }),

    zstd: (buffer, decompressedSize) =>
      new Uint8Array(decompressZstd(buffer, Number(decompressedSize))),
  };
}
