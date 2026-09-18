import type { ChunkInfo } from "./model.ts";

export const RULER_HEIGHT = 42;
export const ROW_HEIGHT = 48;

/** Height of the timeline viewport, including its border, capped by the host. */
export function inspectorHeight(
  rows: number | undefined,
  maxHeight = 520,
): number {
  const cap = Number.isFinite(maxHeight) ? Math.max(1, maxHeight) : 520;
  return Math.min(
    cap,
    rows == undefined ? 320 : RULER_HEIGHT + Math.max(1, rows) * ROW_HEIGHT + 2,
  );
}

/** Ratio of summed uncompressed chunk bodies to summed stored chunk bodies. */
export function compressionRatio(
  chunks: readonly ChunkInfo[],
): number | undefined {
  let compressed = 0,
    uncompressed = 0;
  for (const chunk of chunks) {
    compressed += chunk.compressedSize;
    uncompressed += chunk.uncompressedSize;
  }
  return compressed > 0 && uncompressed > 0
    ? uncompressed / compressed
    : undefined;
}
