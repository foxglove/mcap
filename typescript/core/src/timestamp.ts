import { NsTimestamp } from "./types";

export const TIMESTAMP_UNIX_EPOCH = Object.freeze({ sec: 0, nsec: 0 });

export function timestampToNumber(ns: NsTimestamp): number {
  return ns.sec * 1_000_000_000 + ns.nsec;
}

// NOTE: could be lossy
export function timestampFromNumber(ns: number): NsTimestamp {
  const sec = Math.floor(ns / 1_000_000_000);
  const nsec = ns % 1_000_000_000;
  return { sec, nsec };
}

export function timestampFromBigInt(ns: bigint): NsTimestamp {
  const sec = Number(ns / 1_000_000_000n);
  const nsec = Number(ns % 1_000_000_000n);
  return { sec, nsec };
}

export function timestampFromU32x2(low: number, high: number): NsTimestamp {
  const sec = (high >>> 0) * 2 ** 32 + (low >>> 0) / 1e9;
  const nsec = (low >>> 0) % 1e9;

  return { sec: Math.floor(sec), nsec };
}

export function timestampToU32x2(ns: NsTimestamp): [number, number] {
  const low = (ns.sec * 1e9 + ns.nsec) >>> 0;
  const high = (ns.sec / 2 ** 32) >>> 0;
  return [low, high];
}

export function timestampCompare(a: NsTimestamp, b: NsTimestamp): number {
  if (a.sec !== b.sec) {
    return a.sec - b.sec;
  }
  return a.nsec - b.nsec;
}

export function timestampMax(a: NsTimestamp, b: NsTimestamp): NsTimestamp {
  return timestampCompare(a, b) > 0 ? a : b;
}

export function timestampMin(a: NsTimestamp, b: NsTimestamp): NsTimestamp {
  return timestampCompare(a, b) < 0 ? a : b;
}

export function maybeTimestampMax(
  a: NsTimestamp | undefined,
  b: NsTimestamp | undefined,
): NsTimestamp | undefined {
  if (a === undefined) {
    return b;
  }
  if (b === undefined) {
    return a;
  }
  return timestampMax(a, b);
}

export function timestampAdd(a: NsTimestamp, b: NsTimestamp): NsTimestamp {
  const totalNsec = a.nsec + b.nsec;
  const totalSec = a.sec + b.sec + Math.floor(totalNsec / 1_000_000_000);
  return { sec: totalSec, nsec: totalNsec % 1_000_000_000 };
}

export function timestampMul(a: NsTimestamp, b: number): NsTimestamp {
  const totalNsec = a.nsec * b;
  const totalSec = a.sec * b + Math.floor(totalNsec / 1_000_000_000);
  return { sec: totalSec, nsec: totalNsec % 1_000_000_000 };
}
