import { MCAP_MAGIC } from "../pre0/constants";
import { MCAP0_MAGIC } from "../v0/constants";

export type McapVersion = "pre0" | "0";

export const DETECT_VERSION_BYTES_REQUIRED = 8;

/**
 * Detect MCAP version from file prefix. At least `DETECT_VERSION_BYTES_REQUIRED` bytes must be
 * provided for the version to be detectable.
 */
export function detectVersion(prefix: DataView): McapVersion | undefined {
  if (prefix.byteLength < DETECT_VERSION_BYTES_REQUIRED) {
    return undefined;
  }
  if (MCAP_MAGIC.every((val, i) => val === prefix.getUint8(i))) {
    return "pre0";
  }
  if (MCAP0_MAGIC.every((val, i) => val === prefix.getUint8(i))) {
    return "0";
  }
  return undefined;
}
