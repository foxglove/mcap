import { MCAP_MAGIC } from "./constants.ts";

/** Check if the given buffer starts with the MCAP magic prefix. */
export function hasMcapPrefix(prefix: DataView): boolean {
  return (
    prefix.byteLength >= MCAP_MAGIC.length &&
    MCAP_MAGIC.every((val, i) => val === prefix.getUint8(i))
  );
}
