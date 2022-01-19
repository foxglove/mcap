import { crc32 } from "@foxglove/crc";

import { TypedMcapRecords } from ".";
import Mcap0StreamReader from "./Mcap0StreamReader";
import { MCAP0_MAGIC, Opcode } from "./constants";

function uint16LE(n: number): Uint8Array {
  const result = new Uint8Array(2);
  new DataView(result.buffer).setUint16(0, n, true);
  return result;
}
function uint32LE(n: number): Uint8Array {
  const result = new Uint8Array(4);
  new DataView(result.buffer).setUint32(0, n, true);
  return result;
}
function uint64LE(n: bigint): Uint8Array {
  const result = new Uint8Array(8);
  new DataView(result.buffer).setBigUint64(0, n, true);
  return result;
}
function string(str: string): Uint8Array {
  const encoded = new TextEncoder().encode(str);
  const result = new Uint8Array(4 + encoded.length);
  new DataView(result.buffer).setUint32(0, encoded.length, true);
  result.set(encoded, 4);
  return result;
}
function record(type: Opcode, data: number[]): Uint8Array {
  const result = new Uint8Array(1 + 8 + data.length);
  result[0] = type;
  new DataView(result.buffer).setBigUint64(1, BigInt(data.length), true);
  result.set(data, 1 + 8);
  return result;
}
function keyValues<K, V>(
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
function crcSuffix(data: number[]): number[] {
  const crc = crc32(Uint8Array.from(data));
  return [...data, ...uint32LE(crc)];
}

describe("McapReader", () => {
  it("rejects invalid header", () => {
    for (let i = 0; i < MCAP0_MAGIC.length - 1; i++) {
      const reader = new Mcap0StreamReader();
      const badMagic = MCAP0_MAGIC.slice();
      badMagic[i] = 0x00;
      reader.append(new Uint8Array([...badMagic]));
      expect(() => reader.nextRecord()).toThrow("Expected MCAP magic");
    }
  });

  it("rejects invalid footer magic", () => {
    const reader = new Mcap0StreamReader();
    reader.append(
      new Uint8Array([
        ...MCAP0_MAGIC,
        ...record(Opcode.FOOTER, [
          ...uint64LE(0x0123456789abcdefn), // index offset
          ...uint32LE(0x01234567), // index crc
        ]),
        ...MCAP0_MAGIC.slice(0, MCAP0_MAGIC.length - 1),
        0x00,
      ]),
    );
    expect(() => reader.nextRecord()).toThrow("Expected MCAP magic");
  });

  it("parses empty file", () => {
    const reader = new Mcap0StreamReader();
    reader.append(
      new Uint8Array([
        ...MCAP0_MAGIC,
        ...record(Opcode.FOOTER, [
          ...uint64LE(0x0123456789abcdefn), // index offset
          ...uint32LE(0x01234567), // index crc
        ]),
        ...MCAP0_MAGIC,
      ]),
    );
    expect(reader.nextRecord()).toEqual({
      type: "Footer",
      indexOffset: 0x0123456789abcdefn,
      indexCrc: 0x01234567,
    });
    expect(reader.done()).toBe(true);
  });

  it("accepts empty chunks", () => {
    const reader = new Mcap0StreamReader();
    reader.append(
      new Uint8Array([
        ...MCAP0_MAGIC,
        ...record(Opcode.CHUNK, [
          ...uint64LE(0n), // decompressed size
          ...uint32LE(0), // decompressed crc32
          ...string("lz4"), // compression
          // no chunk data
        ]),
        ...record(Opcode.FOOTER, [
          ...uint64LE(0n), // index offset
          ...uint32LE(0), // index crc
        ]),
        ...MCAP0_MAGIC,
      ]),
    );
    expect(reader.nextRecord()).toEqual({
      type: "Footer",
      indexOffset: 0n,
      indexCrc: 0,
    });
    expect(reader.done()).toBe(true);
  });

  it("waits patiently to parse one byte at a time, and rejects new data after read completed", () => {
    const reader = new Mcap0StreamReader();
    const data = new Uint8Array([
      ...MCAP0_MAGIC,
      ...record(Opcode.FOOTER, [
        ...uint64LE(0x0123456789abcdefn), // index offset
        ...uint32LE(0x01234567), // index crc
      ]),
      ...MCAP0_MAGIC,
    ]);
    for (let i = 0; i < data.length - 1; i++) {
      reader.append(new Uint8Array(data.buffer, i, 1));
      expect(reader.nextRecord()).toBeUndefined();
      expect(reader.done()).toBe(false);
    }
    reader.append(new Uint8Array(data.buffer, data.length - 1, 1));
    expect(reader.nextRecord()).toEqual({
      type: "Footer",
      indexOffset: 0x0123456789abcdefn,
      indexCrc: 0x01234567,
    });
    expect(reader.done()).toBe(true);
    expect(() => reader.append(new Uint8Array([42]))).toThrow("Already done reading");
  });

  it("rejects extraneous data at end of file", () => {
    const reader = new Mcap0StreamReader();
    reader.append(
      new Uint8Array([
        ...MCAP0_MAGIC,
        ...record(Opcode.FOOTER, [
          ...uint64LE(0x0123456789abcdefn), // index offset
          ...uint32LE(0x01234567), // index crc
        ]),
        ...MCAP0_MAGIC,
        42,
      ]),
    );
    expect(() => reader.nextRecord()).toThrow("bytes remaining after MCAP footer");
  });

  it("parses file with empty chunk", () => {
    const reader = new Mcap0StreamReader();
    reader.append(
      new Uint8Array([
        ...MCAP0_MAGIC,

        ...record(Opcode.CHUNK, [
          ...uint64LE(0n), // decompressed size
          ...uint32LE(0), // decompressed crc32
          ...string(""), // compression
          // (no chunk data)
        ]),

        ...record(Opcode.FOOTER, [
          ...uint64LE(0n), // index offset
          ...uint32LE(0), // index crc
        ]),
        ...MCAP0_MAGIC,
      ]),
    );
    expect(reader.nextRecord()).toEqual({
      type: "Footer",
      indexOffset: 0n,
      indexCrc: 0,
    });
    expect(reader.done()).toBe(true);
  });

  it("rejects chunk with incomplete record", () => {
    const reader = new Mcap0StreamReader();
    reader.append(
      new Uint8Array([
        ...MCAP0_MAGIC,

        ...record(Opcode.CHUNK, [
          ...uint64LE(1n), // decompressed size
          ...uint32LE(crc32(new Uint8Array([Opcode.CHANNEL_INFO]))), // decompressed crc32
          ...string(""), // compression

          Opcode.CHANNEL_INFO, // truncated record
        ]),

        ...record(Opcode.FOOTER, [
          ...uint64LE(0n), // index offset
          ...uint32LE(0), // index crc
        ]),
        ...MCAP0_MAGIC,
      ]),
    );
    expect(() => reader.nextRecord()).toThrow("bytes remaining in chunk");
  });

  it("rejects message at top level with no prior channel info", () => {
    const reader = new Mcap0StreamReader();
    reader.append(
      new Uint8Array([
        ...MCAP0_MAGIC,

        ...record(Opcode.MESSAGE, [
          ...uint16LE(42), // channel id
          ...uint64LE(0n), // timestamp
        ]),

        ...record(Opcode.FOOTER, [
          ...uint64LE(0n), // index offset
          ...uint32LE(0), // index crc
        ]),
        ...MCAP0_MAGIC,
      ]),
    );
    expect(() => reader.nextRecord()).toThrow(
      "Encountered message on channel 42 without prior channel info",
    );
  });

  it("rejects message in chunk with no prior channel info", () => {
    const message = record(Opcode.MESSAGE, [
      ...uint16LE(42), // channel id
      ...uint64LE(0n), // timestamp
    ]);
    const reader = new Mcap0StreamReader();
    reader.append(
      new Uint8Array([
        ...MCAP0_MAGIC,

        ...record(Opcode.CHUNK, [
          ...uint64LE(0n), // decompressed size
          ...uint32LE(crc32(message)), // decompressed crc32
          ...string(""), // compression
          ...message,
        ]),

        ...record(Opcode.FOOTER, [
          ...uint64LE(0n), // index offset
          ...uint32LE(0), // index crc
        ]),
        ...MCAP0_MAGIC,
      ]),
    );
    expect(() => reader.nextRecord()).toThrow(
      "Encountered message on channel 42 without prior channel info",
    );
  });

  it("rejects message in chunk with no prior channel info in the same chunk", () => {
    const channelInfo = record(
      Opcode.CHANNEL_INFO,
      crcSuffix([
        ...uint16LE(42), // channel id
        ...string("mytopic"), // topic
        ...string("utf12"), // encoding
        ...string("some data"), // schema name
        ...string("stuff"), // schema
        ...keyValues(string, string, [["foo", "bar"]]), // user data
      ]),
    );
    const message = record(Opcode.MESSAGE, [
      ...uint16LE(42), // channel id
      ...uint32LE(1), // sequence
      ...uint64LE(2n), // publish time
      ...uint64LE(3n), // record time
    ]);
    const reader = new Mcap0StreamReader();
    reader.append(
      new Uint8Array([
        ...MCAP0_MAGIC,

        ...record(Opcode.CHUNK, [
          ...uint64LE(0n), // decompressed size
          ...uint32LE(crc32(new Uint8Array([...channelInfo, ...message]))), // decompressed crc32
          ...string(""), // compression
          ...channelInfo,
          ...message,
        ]),

        ...record(Opcode.CHUNK, [
          ...uint64LE(0n), // decompressed size
          ...uint32LE(crc32(message)), // decompressed crc32
          ...string(""), // compression
          ...message,
        ]),

        ...record(Opcode.FOOTER, [
          ...uint64LE(0n), // index offset
          ...uint32LE(0), // index crc
        ]),
        ...MCAP0_MAGIC,
      ]),
    );
    const expectedChannelInfo: TypedMcapRecords["ChannelInfo"] = {
      type: "ChannelInfo",
      channelId: 42,
      topicName: "mytopic",
      encoding: "utf12",
      schemaName: "some data",
      schema: "stuff",
      userData: [["foo", "bar"]],
    };
    expect(reader.nextRecord()).toEqual(expectedChannelInfo);
    const expectedMessage: TypedMcapRecords["Message"] = {
      type: "Message",
      channelId: expectedChannelInfo.channelId,
      sequence: 1,
      publishTime: 2n,
      recordTime: 3n,
      messageData: new Uint8Array(0),
    };
    expect(reader.nextRecord()).toEqual(expectedMessage);
    expect(() => reader.nextRecord()).toThrow(
      "Encountered message on channel 42 without prior channel info in this chunk",
    );
  });

  it("parses channel info at top level", () => {
    const reader = new Mcap0StreamReader();
    reader.append(
      new Uint8Array([
        ...MCAP0_MAGIC,

        ...record(
          Opcode.CHANNEL_INFO,
          crcSuffix([
            ...uint16LE(1), // channel id
            ...string("mytopic"), // topic
            ...string("utf12"), // encoding
            ...string("some data"), // schema name
            ...string("stuff"), // schema
            ...keyValues(string, string, [["foo", "bar"]]), // user data
          ]),
        ),

        ...record(Opcode.FOOTER, [
          ...uint64LE(0n), // index offset
          ...uint32LE(0), // index crc
        ]),
        ...MCAP0_MAGIC,
      ]),
    );
    expect(reader.nextRecord()).toEqual({
      type: "ChannelInfo",
      channelId: 1,
      topicName: "mytopic",
      encoding: "utf12",
      schemaName: "some data",
      schema: "stuff",
      userData: [["foo", "bar"]],
    } as TypedMcapRecords["ChannelInfo"]);
    expect(reader.nextRecord()).toEqual({
      type: "Footer",
      indexOffset: 0n,
      indexCrc: 0,
    });
    expect(reader.done()).toBe(true);
  });

  it.each([true, false])("parses channel info in chunk (compressed: %s)", (compressed) => {
    const channelInfo = record(
      Opcode.CHANNEL_INFO,
      crcSuffix([
        ...uint16LE(1), // channel id
        ...string("mytopic"), // topic
        ...string("utf12"), // encoding
        ...string("some data"), // schema name
        ...string("stuff"), // schema
        ...keyValues(string, string, [["foo", "bar"]]), // user data
      ]),
    );
    const decompressHandlers = { xyz: () => channelInfo };
    const reader = new Mcap0StreamReader(compressed ? { decompressHandlers } : undefined);
    reader.append(
      new Uint8Array([
        ...MCAP0_MAGIC,

        ...record(Opcode.CHUNK, [
          ...uint64LE(0n), // decompressed size
          ...uint32LE(crc32(channelInfo)), // decompressed crc32
          ...string(compressed ? "xyz" : ""), // compression
          ...(compressed ? new TextEncoder().encode("compressed bytes") : channelInfo),
        ]),

        ...record(Opcode.FOOTER, [
          ...uint64LE(0n), // index offset
          ...uint32LE(0), // index crc
        ]),
        ...MCAP0_MAGIC,
      ]),
    );
    expect(reader.nextRecord()).toEqual({
      type: "ChannelInfo",
      channelId: 1,
      topicName: "mytopic",
      encoding: "utf12",
      schemaName: "some data",
      schema: "stuff",
      userData: [["foo", "bar"]],
    } as TypedMcapRecords["ChannelInfo"]);
    expect(reader.nextRecord()).toEqual({
      type: "Footer",
      indexOffset: 0n,
      indexCrc: 0,
    });
    expect(reader.done()).toBe(true);
  });

  describe.each(["unchunked file", "same chunk", "different chunks"] as const)(
    "rejects channel info with the same id in %s",
    (testType) => {
      it.each([
        {
          key: "topic",
          channelInfo2: record(
            Opcode.CHANNEL_INFO,
            crcSuffix([
              ...uint16LE(42), // channel id
              ...string("XXXXXXXX"), // topic
              ...string("utf12"), // encoding
              ...string("some data"), // schema name
              ...string("stuff"), // schema
              ...keyValues(string, string, [["foo", "bar"]]), // user data
            ]),
          ),
        },
        {
          key: "encoding",
          channelInfo2: record(
            Opcode.CHANNEL_INFO,
            crcSuffix([
              ...uint16LE(42), // channel id
              ...string("mytopic"), // topic
              ...string("XXXXXXXX"), // encoding
              ...string("some data"), // schema name
              ...string("stuff"), // schema
              ...keyValues(string, string, [["foo", "bar"]]), // user data
            ]),
          ),
        },
        {
          key: "schema name",
          channelInfo2: record(
            Opcode.CHANNEL_INFO,
            crcSuffix([
              ...uint16LE(42), // channel id
              ...string("mytopic"), // topic
              ...string("utf12"), // encoding
              ...string("XXXXXXXX"), // schema name
              ...string("stuff"), // schema
              ...keyValues(string, string, [["foo", "bar"]]), // user data
            ]),
          ),
        },
        {
          key: "schema",
          channelInfo2: record(
            Opcode.CHANNEL_INFO,
            crcSuffix([
              ...uint16LE(42), // channel id
              ...string("mytopic"), // topic
              ...string("utf12"), // encoding
              ...string("some data"), // schema name
              ...string("XXXXXXXX"), // schema
              ...keyValues(string, string, [["foo", "bar"]]), // user data
            ]),
          ),
        },
        {
          key: "data",
          channelInfo2: record(
            Opcode.CHANNEL_INFO,
            crcSuffix([
              ...uint16LE(42), // channel id
              ...string("mytopic"), // topic
              ...string("utf12"), // encoding
              ...string("some data"), // schema name
              ...string("stuff"), // schema
              ...keyValues(string, string, [
                ["foo", "bar"],
                ["baz", "quux"],
              ]), // user data
            ]),
          ),
        },
      ])("differing in $key", ({ channelInfo2 }) => {
        const channelInfo = record(
          Opcode.CHANNEL_INFO,
          crcSuffix([
            ...uint16LE(42), // channel id
            ...string("mytopic"), // topic
            ...string("utf12"), // encoding
            ...string("some data"), // schema name
            ...string("stuff"), // schema
            ...keyValues(string, string, [["foo", "bar"]]), // user data
          ]),
        );
        const reader = new Mcap0StreamReader();
        reader.append(
          new Uint8Array([
            ...MCAP0_MAGIC,

            ...(testType === "unchunked file"
              ? [...channelInfo, ...channelInfo2]
              : testType === "same chunk"
              ? record(Opcode.CHUNK, [
                  ...uint64LE(0n), // decompressed size
                  ...uint32LE(crc32(new Uint8Array([...channelInfo, ...channelInfo2]))), // decompressed crc32
                  ...string(""), // compression
                  ...channelInfo,
                  ...channelInfo2,
                ])
              : testType === "different chunks"
              ? [
                  ...record(Opcode.CHUNK, [
                    ...uint64LE(0n), // decompressed size
                    ...uint32LE(crc32(new Uint8Array(channelInfo))), // decompressed crc32
                    ...string(""), // compression
                    ...channelInfo,
                  ]),
                  ...record(Opcode.CHUNK, [
                    ...uint64LE(0n), // decompressed size
                    ...uint32LE(crc32(new Uint8Array(channelInfo2))), // decompressed crc32
                    ...string(""), // compression
                    ...channelInfo2,
                  ]),
                ]
              : []),

            ...record(Opcode.FOOTER, [
              ...uint64LE(0n), // index offset
              ...uint32LE(0), // index crc
            ]),
            ...MCAP0_MAGIC,
          ]),
        );
        expect(reader.nextRecord()).toEqual({
          type: "ChannelInfo",
          channelId: 42,
          topicName: "mytopic",
          encoding: "utf12",
          schemaName: "some data",
          schema: "stuff",
          userData: [["foo", "bar"]],
        } as TypedMcapRecords["ChannelInfo"]);
        expect(() => reader.nextRecord()).toThrow("differing channel infos for 42");
      });
    },
  );
});
