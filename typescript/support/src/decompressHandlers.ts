import type { DecompressHandlers } from "@mcap/core";

type Bzip2Module = typeof import("@foxglove/wasm-bz2");
type Lz4Module = (typeof import("@foxglove/wasm-lz4"))["default"];
type ZstdModule = typeof import("@foxglove/wasm-zstd");

/**
 * Unwraps default exports from dynamic imports so the same code works
 * with ESM modules and CommonJS modules wrapped by Node's ESM interop.
 */
function unwrapDefaultExports<T>(mod: unknown): T {
  if (mod != undefined && typeof mod === "object" && "default" in mod && mod.default != undefined) {
    return mod.default as T;
  }

  return mod as T;
}

let handlersPromise: Promise<DecompressHandlers> | undefined;
export async function loadDecompressHandlers(): Promise<DecompressHandlers> {
  return await (handlersPromise ??= _loadDecompressHandlers());
}

// eslint-disable-next-line no-underscore-dangle
async function _loadDecompressHandlers(): Promise<DecompressHandlers> {
  const [decompressBzip2, decompressLZ4, decompressZstd] = await Promise.all([
    import("@foxglove/wasm-bz2").then(async (mod) => {
      const bzip2 = unwrapDefaultExports<Bzip2Module>(mod);
      const instance = await bzip2.init();
      return instance.decompress.bind(instance);
    }),

    import("@foxglove/wasm-lz4").then(async (mod) => {
      const lz4 = unwrapDefaultExports<Lz4Module>(mod);
      await lz4.isLoaded;
      return lz4;
    }),

    import("@foxglove/wasm-zstd").then(async (mod) => {
      const zstd = unwrapDefaultExports<ZstdModule>(mod);
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
