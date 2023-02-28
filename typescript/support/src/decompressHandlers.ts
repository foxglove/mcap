type DecompressHandlers = {
  [compression: string]: (buffer: Uint8Array, decompressedSize: bigint) => Uint8Array;
};

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
    import("@foxglove/wasm-bz2").then(async (mod) => await mod.default.init()),
  ]);

  return {
    lz4: (buffer, decompressedSize) => decompressLZ4(buffer, Number(decompressedSize)),

    bz2: (buffer, decompressedSize) =>
      bzip2.decompress(buffer, Number(decompressedSize), { small: false }),

    zstd: (buffer, decompressedSize) => decompressZstd(buffer, Number(decompressedSize)),
  };
}
