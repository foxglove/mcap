import type { ChunkInfo } from "./model.ts";

export const RULER_HEIGHT = 42;
export const ROW_HEIGHT = 48;

/** Height of the viewport, keeping details usable while respecting the host cap. */
export function inspectorHeight(
  rows: number | undefined,
  maxHeight = 520,
  minHeight = 320,
  requestedHeight?: number,
): number {
  const cap = Number.isFinite(maxHeight) ? Math.max(1, maxHeight) : 520;
  const minimum = Number.isFinite(minHeight) ? Math.max(1, minHeight) : 320;
  const content =
    requestedHeight != undefined && Number.isFinite(requestedHeight)
      ? requestedHeight
      : rows == undefined
        ? 320
        : RULER_HEIGHT + Math.max(1, rows) * ROW_HEIGHT + 2;
  return Math.min(cap, Math.max(minimum, content));
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
