import { MCAP0_MAGIC } from "../v0/constants";

/** Check if the given buffer starts with the MCAP0 magic prefix. */
export function hasMcapPrefix(prefix: DataView): boolean {
  return (
    prefix.byteLength >= MCAP0_MAGIC.length &&
    MCAP0_MAGIC.every((val, i) => val === prefix.getUint8(i))
  );
}
