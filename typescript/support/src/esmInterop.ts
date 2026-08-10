/**
 * Normalizes ESM/CJS interop module shapes by returning `mod.default` when
 * present, otherwise returning `mod` unchanged.
 */
// eslint-disable-next-line @typescript-eslint/no-unnecessary-type-parameters
export function unwrapDefaultExport<T>(mod: unknown): T {
  if (mod != null && typeof mod === "object" && "default" in mod && mod.default != null) {
    return mod.default as T;
  }
  return mod as T;
}
