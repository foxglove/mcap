export const MCAP_MAGIC = Buffer.from([0x89, 0x4d, 0x43, 0x41, 0x50, 0x30, 0x0d, 0x0a]);

export function record(opcode: number, body: Buffer): Buffer {
  return Buffer.concat([Buffer.from([opcode]), uint64(body.length), body]);
}

export function mcapString(value: string): Buffer {
  const bytes = Buffer.from(value, "utf8");
  return Buffer.concat([uint32(bytes.length), bytes]);
}

export function prefixedBytes(value: Buffer): Buffer {
  return Buffer.concat([uint32(value.length), value]);
}

export function uint16(value: number): Buffer {
  const out = Buffer.alloc(2);
  out.writeUInt16LE(value);
  return out;
}

export function uint32(value: number): Buffer {
  const out = Buffer.alloc(4);
  out.writeUInt32LE(value);
  return out;
}

export function int32(value: number): Buffer {
  const out = Buffer.alloc(4);
  out.writeInt32LE(value);
  return out;
}

export function float32(value: number): Buffer {
  const out = Buffer.alloc(4);
  out.writeFloatLE(value);
  return out;
}

export function uint64(value: bigint | number): Buffer {
  if (typeof value === "number" && !Number.isSafeInteger(value)) {
    throw new Error(`uint64 number value must be a safe integer: ${value.toString()}`);
  }
  const integer = typeof value === "bigint" ? value : BigInt(value);
  if (integer < 0n) {
    throw new Error(`uint64 value must be non-negative: ${value.toString()}`);
  }
  const out = Buffer.alloc(8);
  out.writeBigUInt64LE(integer);
  return out;
}
