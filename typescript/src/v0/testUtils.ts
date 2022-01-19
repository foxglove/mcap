import { crc32 } from "@foxglove/crc";

import { Opcode } from "./constants";

export function uint16LE(n: number): Uint8Array {
  const result = new Uint8Array(2);
  new DataView(result.buffer).setUint16(0, n, true);
  return result;
}

export function uint32LE(n: number): Uint8Array {
  const result = new Uint8Array(4);
  new DataView(result.buffer).setUint32(0, n, true);
  return result;
}

export function uint64LE(n: bigint): Uint8Array {
  const result = new Uint8Array(8);
  new DataView(result.buffer).setBigUint64(0, n, true);
  return result;
}

export function string(str: string): Uint8Array {
  const encoded = new TextEncoder().encode(str);
  const result = new Uint8Array(4 + encoded.length);
  new DataView(result.buffer).setUint32(0, encoded.length, true);
  result.set(encoded, 4);
  return result;
}

export function record(type: Opcode, data: number[]): Uint8Array {
  const result = new Uint8Array(1 + 8 + data.length);
  result[0] = type;
  new DataView(result.buffer).setBigUint64(1, BigInt(data.length), true);
  result.set(data, 1 + 8);
  return result;
}

export function keyValues<K, V>(
  serializeK: (_: K) => Uint8Array,
  serializeV: (_: V) => Uint8Array,
  pairs: [K, V][],
): Uint8Array {
  const serialized = pairs.flatMap(([key, value]) => [serializeK(key), serializeV(value)]);
  const totalLen = serialized.reduce((total, ser) => total + ser.length, 0);
  const result = new Uint8Array(4 + totalLen);
  new DataView(result.buffer).setUint32(0, totalLen, true);
  let offset = 4;
  for (const ser of serialized) {
    result.set(ser, offset);
    offset += ser.length;
  }
  return result;
}

export function crcSuffix(data: number[]): number[] {
  const crc = crc32(Uint8Array.from(data));
  return [...data, ...uint32LE(crc)];
}

export async function collect<T>(iterable: AsyncIterable<T>): Promise<T[]> {
  const result: T[] = [];
  for await (const item of iterable) {
    result.push(item);
  }
  return result;
}
