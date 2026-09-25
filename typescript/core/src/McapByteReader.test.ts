import McapByteReader from "./McapByteReader.ts";

function readerFromBytes(bytes: number[]): McapByteReader {
  const buffer = new Uint8Array(bytes);
  return new McapByteReader(new DataView(buffer.buffer, buffer.byteOffset, buffer.byteLength));
}

describe("McapByteReader", () => {
  it("u8ArrayBorrow shares memory with the source buffer", () => {
    const buffer = new Uint8Array([1, 2, 3, 4]);
    const reader = new McapByteReader(
      new DataView(buffer.buffer, buffer.byteOffset, buffer.byteLength),
    );
    const borrowed = reader.u8ArrayBorrow(2);
    expect(Array.from(borrowed)).toEqual([1, 2]);
    buffer[0] = 9;
    expect(borrowed[0]).toBe(9);
  });

  it("u8ArrayCopy does not share memory with the source buffer", () => {
    const buffer = new Uint8Array([1, 2, 3, 4]);
    const reader = new McapByteReader(
      new DataView(buffer.buffer, buffer.byteOffset, buffer.byteLength),
    );
    const copied = reader.u8ArrayCopy(2);
    expect(Array.from(copied)).toEqual([1, 2]);
    buffer[0] = 9;
    expect(copied[0]).toBe(1);
  });

  it.each([
    ["u8ArrayBorrow", 10],
    ["u8ArrayBorrow", -1],
    ["u8ArrayBorrow", NaN],
    ["u8ArrayCopy", 10],
    ["u8ArrayCopy", -1],
    ["u8ArrayCopy", NaN],
  ] as const)("%s(%s) throws and does not move the offset", (method, length) => {
    const reader = readerFromBytes([1, 2, 3, 4]);
    expect(() => reader[method](length)).toThrow(
      `Byte array length ${length} exceeds bounds of buffer`,
    );
    expect(reader.offset).toBe(0);
    expect(reader.bytesRemaining()).toBe(4);
  });
});
