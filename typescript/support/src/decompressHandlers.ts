import type { DecompressHandlers } from "@mcap/core";

type Bzip2Module = (typeof import("@foxglove/wasm-bz2"))["default"];
type Lz4Module = (typeof import("@foxglove/wasm-lz4"))["default"];

let handlersPromise: Promise<DecompressHandlers> | undefined;
export async function loadDecompressHandlers(): Promise<DecompressHandlers> {
  return await (handlersPromise ??= _loadDecompressHandlers());
}

// eslint-disable-next-line no-underscore-dangle
async function _loadDecompressHandlers(): Promise<DecompressHandlers> {
  const [decompressZstd, decompressLZ4, bzip2] = await Promise.all([
    // Conditional default imports are required to support both ESM and CJS
    import("@foxglove/wasm-zstd").then(async (mod) => {
      await mod.isLoaded;
      return mod.decompress;
    }),
    import("@foxglove/wasm-lz4").then(async (mod) => {
      const lz4 = ((mod as { default?: unknown }).default ?? mod) as Lz4Module;
      await lz4.isLoaded;
      return lz4;
    }),

    import("@foxglove/wasm-bz2").then(async (mod) => {
      const bzip2 = ((mod.default as { default?: unknown }).default ?? mod.default) as Bzip2Module;
      return await bzip2.init();
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
