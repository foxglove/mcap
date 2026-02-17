import type { DecompressHandlers } from "@mcap/core";

import { unwrapDefaultExport } from "./esmInterop.ts";

type Bzip2Module = typeof import("@foxglove/wasm-bz2");
type Lz4Module = (typeof import("@foxglove/wasm-lz4"))["default"];
type ZstdModule = typeof import("@foxglove/wasm-zstd");

let handlersPromise: Promise<DecompressHandlers> | undefined;
export async function loadDecompressHandlers(): Promise<DecompressHandlers> {
  return await (handlersPromise ??= _loadDecompressHandlers());
}

// eslint-disable-next-line no-underscore-dangle
async function _loadDecompressHandlers(): Promise<DecompressHandlers> {
  const [decompressBzip2, decompressLZ4, decompressZstd] = await Promise.all([
    import("@foxglove/wasm-bz2").then(async (mod) => {
      const bzip2 = unwrapDefaultExport<Bzip2Module>(mod);
      const instance = await bzip2.init();
      return instance.decompress.bind(instance);
    }),

    import("@foxglove/wasm-lz4").then(async (mod) => {
      const lz4 = unwrapDefaultExport<Lz4Module>(mod);
      await lz4.isLoaded;
      return lz4;
    }),

    import("@foxglove/wasm-zstd").then(async (mod) => {
      const zstd = unwrapDefaultExport<ZstdModule>(mod);
      await zstd.isLoaded;
      return zstd.decompress;
    }),
  ]);

  return {
    bz2: (buffer, decompressedSize) =>
      decompressBzip2(buffer, Number(decompressedSize), { small: false }),

    lz4: (buffer, decompressedSize) =>
      new Uint8Array(decompressLZ4(buffer, Number(decompressedSize))),

    zstd: (buffer, decompressedSize) =>
      new Uint8Array(decompressZstd(buffer, Number(decompressedSize))),
  };
}
