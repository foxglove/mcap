import type { McapTypes } from "@mcap/core";

let handlersPromise: Promise<McapTypes.DecompressHandlers> | undefined;
export async function loadDecompressHandlers(): Promise<McapTypes.DecompressHandlers> {
  return await (handlersPromise ??= _loadDecompressHandlers());
}

// eslint-disable-next-line no-underscore-dangle
async function _loadDecompressHandlers(): Promise<McapTypes.DecompressHandlers> {
  const [decompressZstd, decompressLZ4, bzip2] = await Promise.all([
    import("@foxglove/wasm-zstd").then(async (mod) => {
      await mod.isLoaded;
      return mod.decompress;
    }),
    import("@foxglove/wasm-lz4").then(async (mod) => {
      await mod.default.isLoaded;
      return mod.default;
    }),
    import("@foxglove/wasm-bz2").then(async (mod) => await mod.default.init()),
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
