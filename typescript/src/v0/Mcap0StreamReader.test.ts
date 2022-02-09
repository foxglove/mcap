import { crc32 } from "@foxglove/crc";

import { TypedMcapRecords } from ".";
import Mcap0StreamReader from "./Mcap0StreamReader";
import { MCAP0_MAGIC, Opcode } from "./constants";
import {
  record,
  uint64LE,
  uint32LE,
  string,
  uint16LE,
  keyValues,
  crcSuffix,
  uint64PrefixedBytes,
} from "./testUtils";

describe("Mcap0StreamReader", () => {
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
          ...uint64LE(0x0123456789abcdefn), // summary start
          ...uint64LE(0x0123456789abcdefn), // summary offset start
          ...uint32LE(0x01234567), // summary crc
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
          ...uint64LE(0x0123456789abcdefn), // summary start
          ...uint64LE(0x0123456789abcdefn), // summary offset start
          ...uint32LE(0x01234567), // summary crc
        ]),
        ...MCAP0_MAGIC,
      ]),
    );
    expect(reader.nextRecord()).toEqual({
      type: "Footer",
      summaryStart: 0x0123456789abcdefn,
      summaryOffsetStart: 0x0123456789abcdefn,
      summaryCrc: 0x01234567,
    });
    expect(reader.done()).toBe(true);
  });

  it("accepts empty chunks", () => {
    const reader = new Mcap0StreamReader();
    reader.append(
      new Uint8Array([
        ...MCAP0_MAGIC,
        ...record(Opcode.CHUNK, [
          ...uint64LE(0n), // start_time
          ...uint64LE(0n), // end_time
          ...uint64LE(0n), // decompressed size
          ...uint32LE(0), // decompressed crc32
          ...string("lz4"), // compression
          ...uint64LE(BigInt(0n)),
          // no chunk data
        ]),
        ...record(Opcode.DATA_END, [
          ...uint32LE(0), // data section crc
        ]),
        ...record(Opcode.FOOTER, [
          ...uint64LE(0n), // summary start
          ...uint64LE(0n), // summary offset start
          ...uint32LE(0), // summary crc
        ]),
        ...MCAP0_MAGIC,
      ]),
    );
    expect(reader.nextRecord()).toEqual({
      type: "Footer",
      summaryStart: 0n,
      summaryOffsetStart: 0n,
      summaryCrc: 0,
    });
    expect(reader.done()).toBe(true);
  });

  it("waits patiently to parse one byte at a time, and rejects new data after read completed", () => {
    const reader = new Mcap0StreamReader();
    const data = new Uint8Array([
      ...MCAP0_MAGIC,
      ...record(Opcode.FOOTER, [
        ...uint64LE(0x0123456789abcdefn), // summary start
        ...uint64LE(0x0123456789abcdefn), // summary offset start
        ...uint32LE(0x01234567), // summary crc
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
      summaryStart: 0x0123456789abcdefn,
      summaryOffsetStart: 0x0123456789abcdefn,
      summaryCrc: 0x01234567,
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
          ...uint64LE(0x0123456789abcdefn), // summary start
          ...uint64LE(0x0123456789abcdefn), // summary offset start
          ...uint32LE(0x01234567), // summary crc
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
          ...uint64LE(0n), // start_time
          ...uint64LE(0n), // end_time
          ...uint64LE(0n), // decompressed size
          ...uint32LE(0), // decompressed crc32
          ...string(""), // compression
          ...uint64LE(BigInt(0n)),
          // (no chunk data)
        ]),

        ...record(Opcode.FOOTER, [
          ...uint64LE(0n), // summary start
          ...uint64LE(0n), // summary offset start
          ...uint32LE(0), // summary crc
        ]),
        ...MCAP0_MAGIC,
      ]),
    );
    expect(reader.nextRecord()).toEqual({
      type: "Footer",
      summaryStart: 0n,
      summaryOffsetStart: 0n,
      summaryCrc: 0,
    });
    expect(reader.done()).toBe(true);
  });

  it("rejects chunk with incomplete record", () => {
    const reader = new Mcap0StreamReader();
    reader.append(
      new Uint8Array([
        ...MCAP0_MAGIC,

        ...record(Opcode.CHUNK, [
          ...uint64LE(0n), // start_time
          ...uint64LE(0n), // end_time
          ...uint64LE(1n), // decompressed size
          ...uint32LE(crc32(new Uint8Array([Opcode.CHANNEL]))), // decompressed crc32
          ...string(""), // compression
          ...uint64LE(BigInt(1n)),
          Opcode.CHANNEL, // truncated record
        ]),

        ...record(Opcode.FOOTER, [
          ...uint64LE(0n), // summary start
          ...uint64LE(0n), // summary offset start
          ...uint32LE(0), // summary crc
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
          ...uint64LE(0n), // sequence
          ...uint64LE(0n), // publish time
          ...uint64LE(0n), // log time
        ]),

        ...record(Opcode.FOOTER, [
          ...uint64LE(0n), // summary start
          ...uint64LE(0n), // summary offset start
          ...uint32LE(0), // summary crc
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
      ...uint64LE(0n), // sequence
      ...uint64LE(0n), // publish time
      ...uint64LE(0n), // log time
    ]);
    const reader = new Mcap0StreamReader();
    reader.append(
      new Uint8Array([
        ...MCAP0_MAGIC,

        ...record(Opcode.CHUNK, [
          ...uint64LE(0n), // start_time
          ...uint64LE(0n), // end_time
          ...uint64LE(0n), // decompressed size
          ...uint32LE(crc32(message)), // decompressed crc32
          ...string(""), // compression
          ...uint64LE(BigInt(message.byteLength)),
          ...message,
        ]),

        ...record(Opcode.FOOTER, [
          ...uint64LE(0n), // summary start
          ...uint64LE(0n), // summary offset start
          ...uint32LE(0), // summary crc
        ]),
        ...MCAP0_MAGIC,
      ]),
    );
    expect(() => reader.nextRecord()).toThrow(
      "Encountered message on channel 42 without prior channel info",
    );
  });

  it("rejects schema data with incorrect length prefix", () => {
    const reader = new Mcap0StreamReader();
    reader.append(
      new Uint8Array([
        ...MCAP0_MAGIC,
        ...record(Opcode.SCHEMA, [
          ...uint16LE(42), // id
          ...string("name"), // name
          ...string("encoding"), // encoding
          ...uint32LE(3), // length prefix
          10,
          11,
        ]),
        ...record(Opcode.FOOTER, [
          ...uint64LE(0n), // summary start
          ...uint64LE(0n), // summary offset start
          ...uint32LE(0), // summary crc
        ]),
        ...MCAP0_MAGIC,
      ]),
    );
    expect(() => reader.nextRecord()).toThrow("Schema data length 3 exceeds bounds of record");
  });

  it("rejects attachment data with incorrect length prefix", () => {
    const reader = new Mcap0StreamReader();
    reader.append(
      new Uint8Array([
        ...MCAP0_MAGIC,
        ...record(
          Opcode.ATTACHMENT,
          crcSuffix([
            ...string("myFile"), // name
            ...uint64LE(1n), // created at
            ...uint64LE(2n), // log time
            ...string("text/plain"), // content type
            ...uint64LE(3n), // data length
            10,
            11,
          ]),
        ),
        ...record(Opcode.FOOTER, [
          ...uint64LE(0n), // summary start
          ...uint64LE(0n), // summary offset start
          ...uint32LE(0), // summary crc
        ]),
        ...MCAP0_MAGIC,
      ]),
    );
    expect(() => reader.nextRecord()).toThrow("Attachment data length 3 exceeds bounds of record");
  });

  it("parses channel info at top level", () => {
    const reader = new Mcap0StreamReader();
    reader.append(
      new Uint8Array([
        ...MCAP0_MAGIC,

        ...record(Opcode.CHANNEL, [
          ...uint16LE(1), // channel id
          ...string("myTopic"), // topic
          ...string("utf12"), // message encoding
          ...uint16LE(1), // schema id
          ...keyValues(string, string, [["foo", "bar"]]), // user data
        ]),

        ...record(Opcode.FOOTER, [
          ...uint64LE(0n), // summary start
          ...uint64LE(0n), // summary offset start
          ...uint32LE(0), // summary crc
        ]),
        ...MCAP0_MAGIC,
      ]),
    );
    expect(reader.nextRecord()).toEqual({
      type: "Channel",
      id: 1,
      topic: "myTopic",
      messageEncoding: "utf12",
      schemaId: 1,
      metadata: new Map([["foo", "bar"]]),
    } as TypedMcapRecords["Channel"]);
    expect(reader.nextRecord()).toEqual({
      type: "Footer",
      summaryStart: 0n,
      summaryOffsetStart: 0n,
      summaryCrc: 0,
    });
    expect(reader.done()).toBe(true);
  });

  it.each([true, false])("parses channel info in chunk (compressed: %s)", (compressed) => {
    const channel = record(Opcode.CHANNEL, [
      ...uint16LE(1), // channel id
      ...string("myTopic"), // topic
      ...string("utf12"), // message encoding
      ...uint16LE(1),
      ...keyValues(string, string, [["foo", "bar"]]), // user data
    ]);
    const decompressHandlers = { xyz: () => channel };
    const reader = new Mcap0StreamReader(compressed ? { decompressHandlers } : undefined);

    const payload = compressed ? new TextEncoder().encode("compressed bytes") : channel;
    reader.append(
      new Uint8Array([
        ...MCAP0_MAGIC,

        ...record(Opcode.CHUNK, [
          ...uint64LE(0n), // start_time
          ...uint64LE(0n), // end_time
          ...uint64LE(0n), // decompressed size
          ...uint32LE(crc32(channel)), // decompressed crc32
          ...string(compressed ? "xyz" : ""), // compression
          ...uint64LE(BigInt(payload.byteLength)),
          ...payload,
        ]),

        ...record(Opcode.FOOTER, [
          ...uint64LE(0n), // summary start
          ...uint64LE(0n), // summary offset start
          ...uint32LE(0), // summary crc
        ]),
        ...MCAP0_MAGIC,
      ]),
    );
    expect(reader.nextRecord()).toEqual({
      type: "Channel",
      id: 1,
      topic: "myTopic",
      messageEncoding: "utf12",
      schemaId: 1,
      metadata: new Map([["foo", "bar"]]),
    } as TypedMcapRecords["Channel"]);
    expect(reader.nextRecord()).toEqual({
      type: "Footer",
      summaryStart: 0n,
      summaryOffsetStart: 0n,
      summaryCrc: 0,
    });
    expect(reader.done()).toBe(true);
  });

  describe.each(["unchunked file", "same chunk", "different chunks"] as const)(
    "rejects channel info with the same id in %s",
    (testType) => {
      it.each([
        {
          key: "topic",
          channelInfo2: record(Opcode.CHANNEL, [
            ...uint16LE(42), // channel id
            ...string("XXXXXXXX"), // topic
            ...string("utf12"), // message encoding
            ...uint16LE(1), // schema id
            ...keyValues(string, string, [["foo", "bar"]]), // user data
          ]),
        },
        {
          key: "encoding",
          channelInfo2: record(Opcode.CHANNEL, [
            ...uint16LE(42), // channel id
            ...string("myTopic"), // topic
            ...string("XXXXXXXX"), // message encoding
            ...uint16LE(1), // schema id
            ...keyValues(string, string, [["foo", "bar"]]), // user data
          ]),
        },
        {
          key: "schema_id",
          channelInfo2: record(Opcode.CHANNEL, [
            ...uint16LE(42), // channel id
            ...string("myTopic"), // topic
            ...string("utf12"), // message encoding
            ...uint16LE(0), // schema id
            ...keyValues(string, string, [["foo", "bar"]]), // user data
          ]),
        },
      ])("differing in $key", ({ channelInfo2 }) => {
        const channel = record(Opcode.CHANNEL, [
          ...uint16LE(42), // channel id
          ...string("myTopic"), // topic
          ...string("utf12"), // message encoding
          ...uint16LE(1), // schema id
          ...keyValues(string, string, [["foo", "bar"]]), // user data
        ]);
        const reader = new Mcap0StreamReader();
        reader.append(
          new Uint8Array([
            ...MCAP0_MAGIC,

            ...(testType === "unchunked file"
              ? [...channel, ...channelInfo2]
              : testType === "same chunk"
              ? record(Opcode.CHUNK, [
                  ...uint64LE(0n), // start_time
                  ...uint64LE(0n), // end_time
                  ...uint64LE(0n), // decompressed size
                  ...uint32LE(crc32(new Uint8Array([...channel, ...channelInfo2]))), // decompressed crc32
                  ...string(""), // compression
                  ...uint64LE(BigInt(channel.byteLength + channelInfo2.byteLength)),
                  ...channel,
                  ...channelInfo2,
                ])
              : testType === "different chunks"
              ? [
                  ...record(Opcode.CHUNK, [
                    ...uint64LE(0n), // start_time
                    ...uint64LE(0n), // end_time
                    ...uint64LE(0n), // decompressed size
                    ...uint32LE(crc32(new Uint8Array(channel))), // decompressed crc32
                    ...string(""), // compression
                    ...uint64LE(BigInt(channel.byteLength)),
                    ...channel,
                  ]),
                  ...record(Opcode.CHUNK, [
                    ...uint64LE(0n), // start_time
                    ...uint64LE(0n), // end_time
                    ...uint64LE(0n), // decompressed size
                    ...uint32LE(crc32(new Uint8Array(channelInfo2))), // decompressed crc32
                    ...string(""), // compression
                    ...uint64LE(BigInt(channelInfo2.byteLength)),
                    ...channelInfo2,
                  ]),
                ]
              : []),

            ...record(Opcode.FOOTER, [
              ...uint64LE(0n), // summary start
              ...uint64LE(0n), // summary offset start
              ...uint32LE(0), // summary crc
            ]),
            ...MCAP0_MAGIC,
          ]),
        );
        expect(reader.nextRecord()).toEqual({
          type: "Channel",
          id: 42,
          topic: "myTopic",
          messageEncoding: "utf12",
          schemaId: 1,
          metadata: new Map([["foo", "bar"]]),
        } as TypedMcapRecords["Channel"]);
        expect(() => reader.nextRecord()).toThrow("differing channel infos for 42");
      });
    },
  );

  it("validates attachment crc", () => {
    const reader = new Mcap0StreamReader();
    reader.append(
      new Uint8Array([
        ...MCAP0_MAGIC,
        ...record(
          Opcode.ATTACHMENT,
          crcSuffix([
            ...string("myFile"), // name
            ...uint64LE(1n), // created at
            ...uint64LE(2n), // log time
            ...string("text/plain"), // content type
            ...uint64PrefixedBytes(new TextEncoder().encode("hello")), // data
          ]),
        ),
        ...record(Opcode.FOOTER, [
          ...uint64LE(0n), // summary start
          ...uint64LE(0n), // summary offset start
          ...uint32LE(0), // summary crc
        ]),
        ...MCAP0_MAGIC,
      ]),
    );
    expect(reader.nextRecord()).toEqual({
      type: "Attachment",
      name: "myFile",
      createdAt: 1n,
      logTime: 2n,
      contentType: "text/plain",
      data: new TextEncoder().encode("hello"),
    } as TypedMcapRecords["Attachment"]);
    expect(reader.nextRecord()).toEqual({
      type: "Footer",
      summaryStart: 0n,
      summaryOffsetStart: 0n,
      summaryCrc: 0,
    });
    expect(reader.done()).toBe(true);
  });
});
