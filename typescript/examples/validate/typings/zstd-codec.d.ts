declare module "zstd-codec" {
  class Simple {
    decompress(compressed_bytes: Uint8Array): Uint8Array | null;
  }
  class Streaming {
    decompressChunks(chunks: Iterable<Uint8Array>, size_hint?: number): Uint8Array | null;
  }

  export type { Simple as ZstdSimple };
  export type { Streaming as ZstdStreaming };

  export type ZstdModule = {
    Simple: typeof Simple;
    Streaming: typeof Streaming;
  };

  export const ZstdCodec: {
    run(callback: (zstd: ZstdModule) => void): void;
  };
}
