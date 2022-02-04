import { crc32 } from "@foxglove/crc";

import { TypedMcapRecords } from ".";
import Mcap0StreamReader from "./Mcap0StreamReader";
import { MCAP0_MAGIC, Opcode } from "./constants";
import { record, uint64LE, uint32LE, string, uint16LE, keyValues, crcSuffix } from "./testUtils";

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
          ...uint32LE(crc32(new Uint8Array([Opcode.CHANNEL_INFO]))), // decompressed crc32
          ...string(""), // compression

          Opcode.CHANNEL_INFO, // truncated record
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
          ...uint64LE(0n), // timestamp
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
      ...uint64LE(0n), // timestamp
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

  it("parses channel info at top level", () => {
    const reader = new Mcap0StreamReader();
    reader.append(
      new Uint8Array([
        ...MCAP0_MAGIC,

        ...record(Opcode.CHANNEL_INFO, [
          ...uint16LE(1), // channel id
          ...string("myTopic"), // topic
          ...string("utf12"), // message encoding
          ...string("json"), // schema encoding
          ...string("stuff"), // schema
          ...string("some data"), // schema name
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
      type: "ChannelInfo",
      id: 1,
      topic: "myTopic",
      messageEncoding: "utf12",
      schemaEncoding: "json",
      schemaName: "some data",
      schema: "stuff",
      metadata: [["foo", "bar"]],
    } as TypedMcapRecords["ChannelInfo"]);
    expect(reader.nextRecord()).toEqual({
      type: "Footer",
      summaryStart: 0n,
      summaryOffsetStart: 0n,
      summaryCrc: 0,
    });
    expect(reader.done()).toBe(true);
  });

  it.each([true, false])("parses channel info in chunk (compressed: %s)", (compressed) => {
    const channelInfo = record(Opcode.CHANNEL_INFO, [
      ...uint16LE(1), // channel id
      ...string("myTopic"), // topic
      ...string("utf12"), // message encoding
      ...string("json"), // schema encoding
      ...string("stuff"), // schema
      ...string("some data"), // schema name
      ...keyValues(string, string, [["foo", "bar"]]), // user data
    ]);
    const decompressHandlers = { xyz: () => channelInfo };
    const reader = new Mcap0StreamReader(compressed ? { decompressHandlers } : undefined);
    reader.append(
      new Uint8Array([
        ...MCAP0_MAGIC,

        ...record(Opcode.CHUNK, [
          ...uint64LE(0n), // start_time
          ...uint64LE(0n), // end_time
          ...uint64LE(0n), // decompressed size
          ...uint32LE(crc32(channelInfo)), // decompressed crc32
          ...string(compressed ? "xyz" : ""), // compression
          ...(compressed ? new TextEncoder().encode("compressed bytes") : channelInfo),
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
      type: "ChannelInfo",
      id: 1,
      topic: "myTopic",
      messageEncoding: "utf12",
      schemaEncoding: "json",
      schemaName: "some data",
      schema: "stuff",
      metadata: [["foo", "bar"]],
    } as TypedMcapRecords["ChannelInfo"]);
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
          channelInfo2: record(Opcode.CHANNEL_INFO, [
            ...uint16LE(42), // channel id
            ...string("XXXXXXXX"), // topic
            ...string("utf12"), // message encoding
            ...string("json"), // schema encoding
            ...string("stuff"), // schema
            ...string("some data"), // schema name
            ...keyValues(string, string, [["foo", "bar"]]), // user data
          ]),
        },
        {
          key: "encoding",
          channelInfo2: record(Opcode.CHANNEL_INFO, [
            ...uint16LE(42), // channel id
            ...string("myTopic"), // topic
            ...string("XXXXXXXX"), // message encoding
            ...string("json"), // schema encoding
            ...string("stuff"), // schema
            ...string("some data"), // schema name
            ...keyValues(string, string, [["foo", "bar"]]), // user data
          ]),
        },
        {
          key: "schema name",
          channelInfo2: record(Opcode.CHANNEL_INFO, [
            ...uint16LE(42), // channel id
            ...string("myTopic"), // topic
            ...string("utf12"), // message encoding
            ...string("json"), // schema encoding
            ...string("stuff"), // schema
            ...string("XXXXXXXX"), // schema name
            ...keyValues(string, string, [["foo", "bar"]]), // user data
          ]),
        },
        {
          key: "schema",
          channelInfo2: record(Opcode.CHANNEL_INFO, [
            ...uint16LE(42), // channel id
            ...string("myTopic"), // topic
            ...string("utf12"), // message encoding
            ...string("json"), // schema encoding
            ...string("XXXXXXXX"), // schema
            ...string("some data"), // schema name
            ...keyValues(string, string, [["foo", "bar"]]), // user data
          ]),
        },
        {
          key: "data",
          channelInfo2: record(Opcode.CHANNEL_INFO, [
            ...uint16LE(42), // channel id
            ...string("myTopic"), // topic
            ...string("utf12"), // message encoding
            ...string("json"), // schema encoding
            ...string("stuff"), // schema
            ...string("some data"), // schema name
            ...keyValues(string, string, [
              ["foo", "bar"],
              ["baz", "quux"],
            ]), // user data
          ]),
        },
      ])("differing in $key", ({ channelInfo2 }) => {
        const channelInfo = record(Opcode.CHANNEL_INFO, [
          ...uint16LE(42), // channel id
          ...string("myTopic"), // topic
          ...string("utf12"), // message encoding
          ...string("json"), // schema encoding
          ...string("stuff"), // schema
          ...string("some data"), // schema name
          ...keyValues(string, string, [["foo", "bar"]]), // user data
        ]);
        const reader = new Mcap0StreamReader();
        reader.append(
          new Uint8Array([
            ...MCAP0_MAGIC,

            ...(testType === "unchunked file"
              ? [...channelInfo, ...channelInfo2]
              : testType === "same chunk"
              ? record(Opcode.CHUNK, [
                  ...uint64LE(0n), // start_time
                  ...uint64LE(0n), // end_time
                  ...uint64LE(0n), // decompressed size
                  ...uint32LE(crc32(new Uint8Array([...channelInfo, ...channelInfo2]))), // decompressed crc32
                  ...string(""), // compression
                  ...channelInfo,
                  ...channelInfo2,
                ])
              : testType === "different chunks"
              ? [
                  ...record(Opcode.CHUNK, [
                    ...uint64LE(0n), // start_time
                    ...uint64LE(0n), // end_time
                    ...uint64LE(0n), // decompressed size
                    ...uint32LE(crc32(new Uint8Array(channelInfo))), // decompressed crc32
                    ...string(""), // compression
                    ...channelInfo,
                  ]),
                  ...record(Opcode.CHUNK, [
                    ...uint64LE(0n), // start_time
                    ...uint64LE(0n), // end_time
                    ...uint64LE(0n), // decompressed size
                    ...uint32LE(crc32(new Uint8Array(channelInfo2))), // decompressed crc32
                    ...string(""), // compression
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
          type: "ChannelInfo",
          id: 42,
          topic: "myTopic",
          messageEncoding: "utf12",
          schemaEncoding: "json",
          schemaName: "some data",
          schema: "stuff",
          metadata: [["foo", "bar"]],
        } as TypedMcapRecords["ChannelInfo"]);
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
            ...uint64LE(BigInt(new TextEncoder().encode("hello").byteLength)), // data length
            ...new TextEncoder().encode("hello"), // data
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
