import { crc32 } from "@foxglove/crc";

import McapStreamReader from "./McapStreamReader";
import { MCAP_MAGIC, Opcode } from "./constants";
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
import { TypedMcapRecords } from "./types";

describe("McapStreamReader", () => {
  it("rejects invalid header", () => {
    for (let i = 0; i < MCAP_MAGIC.length - 1; i++) {
      const reader = new McapStreamReader();
      const badMagic = MCAP_MAGIC.slice();
      badMagic[i] = 0x00;
      reader.append(new Uint8Array([...badMagic]));
      expect(() => reader.nextRecord()).toThrow("Expected MCAP magic");
    }
  });

  it("rejects invalid footer magic", () => {
    const reader = new McapStreamReader();
    reader.append(
      new Uint8Array([
        ...MCAP_MAGIC,
        ...record(Opcode.FOOTER, [
          ...uint64LE(0x0123456789abcdef), // summary start
          ...uint64LE(0x0123456789abcdef), // summary offset start
          ...uint32LE(0x01234567), // summary crc
        ]),
        ...MCAP_MAGIC.slice(0, MCAP_MAGIC.length - 1),
        0x00,
      ]),
    );
    expect(() => reader.nextRecord()).toThrow("Expected MCAP magic");
  });

  it("includes library in error messages", () => {
    const reader = new McapStreamReader();
    reader.append(
      new Uint8Array([
        ...MCAP_MAGIC,
        ...record(Opcode.HEADER, [...string("prof"), ...string("lib")]),
        ...record(Opcode.FOOTER, [
          ...uint64LE(0x0123456789abcdef), // summary start
          ...uint64LE(0x0123456789abcdef), // summary offset start
          ...uint32LE(0x01234567), // summary crc
        ]),
        ...[0, 0, 0, 0, 0, 0, 0, 0],
      ]),
    );
    expect(reader.nextRecord()).toEqual({ type: "Header", profile: "prof", library: "lib" });
    expect(() => reader.nextRecord()).toThrow(/Expected MCAP magic.+\[library=lib\]/);
  });

  it("includes 'no header' in error messages if there is no header", () => {
    const reader = new McapStreamReader();
    reader.append(
      new Uint8Array([
        ...MCAP_MAGIC,
        ...record(Opcode.FOOTER, [
          ...uint64LE(0x0123456789abcdef), // summary start
          ...uint64LE(0x0123456789abcdef), // summary offset start
          ...uint32LE(0x01234567), // summary crc
        ]),
        ...[0, 0, 0, 0, 0, 0, 0, 0],
      ]),
    );
    expect(() => reader.nextRecord()).toThrow(/Expected MCAP magic.+\[no header\]/);
  });

  it("parses empty file", () => {
    const reader = new McapStreamReader();
    reader.append(
      new Uint8Array([
        ...MCAP_MAGIC,
        ...record(Opcode.FOOTER, [
          ...uint64LE(0x0123456789abcdef), // summary start
          ...uint64LE(0x0123456789abcdef), // summary offset start
          ...uint32LE(0x01234567), // summary crc
        ]),
        ...MCAP_MAGIC,
      ]),
    );
    expect(reader.nextRecord()).toEqual({
      type: "Footer",
      summaryStart: 0x0123456789abcdef,
      summaryOffsetStart: 0x0123456789abcdef,
      summaryCrc: 0x01234567,
    });
    expect(reader.done()).toBe(true);
  });

  it("accepts empty chunks", () => {
    const reader = new McapStreamReader();
    reader.append(
      new Uint8Array([
        ...MCAP_MAGIC,
        ...record(Opcode.CHUNK, [
          ...uint64LE(0), // start_time
          ...uint64LE(0), // end_time
          ...uint64LE(0), // decompressed size
          ...uint32LE(0), // decompressed crc32
          ...string("lz4"), // compression
          ...uint64LE(0),
          // no chunk data
        ]),
        ...record(Opcode.DATA_END, [
          ...uint32LE(0), // data section crc
        ]),
        ...record(Opcode.FOOTER, [
          ...uint64LE(0), // summary start
          ...uint64LE(0), // summary offset start
          ...uint32LE(0), // summary crc
        ]),
        ...MCAP_MAGIC,
      ]),
    );
    expect(reader.nextRecord()).toEqual({
      type: "DataEnd",
      dataSectionCrc: 0,
    });
    expect(reader.nextRecord()).toEqual({
      type: "Footer",
      summaryStart: 0,
      summaryOffsetStart: 0,
      summaryCrc: 0,
    });
    expect(reader.done()).toBe(true);
  });

  it("waits patiently to parse one byte at a time, and rejects new data after read completed", () => {
    const reader = new McapStreamReader();
    const data = new Uint8Array([
      ...MCAP_MAGIC,
      ...record(Opcode.FOOTER, [
        ...uint64LE(0x0123456789abcdef), // summary start
        ...uint64LE(0x0123456789abcdef), // summary offset start
        ...uint32LE(0x01234567), // summary crc
      ]),
      ...MCAP_MAGIC,
    ]);
    for (let i = 0; i < data.length - 1; i++) {
      reader.append(new Uint8Array(data.buffer, i, 1));
      expect(reader.nextRecord()).toBeUndefined();
      expect(reader.done()).toBe(false);
    }
    reader.append(new Uint8Array(data.buffer, data.length - 1, 1));
    expect(reader.nextRecord()).toEqual({
      type: "Footer",
      summaryStart: 0x0123456789abcdef,
      summaryOffsetStart: 0x0123456789abcdef,
      summaryCrc: 0x01234567,
    });
    expect(reader.done()).toBe(true);
    expect(() => {
      reader.append(new Uint8Array([42]));
    }).toThrow("Already done reading");
  });

  it("rejects extraneous data at end of file", () => {
    const reader = new McapStreamReader();
    reader.append(
      new Uint8Array([
        ...MCAP_MAGIC,
        ...record(Opcode.FOOTER, [
          ...uint64LE(0x0123456789abcdef), // summary start
          ...uint64LE(0x0123456789abcdef), // summary offset start
          ...uint32LE(0x01234567), // summary crc
        ]),
        ...MCAP_MAGIC,
        42,
      ]),
    );
    expect(() => reader.nextRecord()).toThrow("bytes remaining after MCAP footer");
  });

  it("parses file with empty chunk", () => {
    const reader = new McapStreamReader();
    reader.append(
      new Uint8Array([
        ...MCAP_MAGIC,

        ...record(Opcode.CHUNK, [
          ...uint64LE(0), // start_time
          ...uint64LE(0), // end_time
          ...uint64LE(0), // decompressed size
          ...uint32LE(0), // decompressed crc32
          ...string(""), // compression
          ...uint64LE(0),
          // (no chunk data)
        ]),

        ...record(Opcode.FOOTER, [
          ...uint64LE(0), // summary start
          ...uint64LE(0), // summary offset start
          ...uint32LE(0), // summary crc
        ]),
        ...MCAP_MAGIC,
      ]),
    );
    expect(reader.nextRecord()).toEqual({
      type: "Footer",
      summaryStart: 0,
      summaryOffsetStart: 0,
      summaryCrc: 0,
    });
    expect(reader.done()).toBe(true);
  });

  it("rejects chunk with incomplete record", () => {
    const reader = new McapStreamReader();
    reader.append(
      new Uint8Array([
        ...MCAP_MAGIC,

        ...record(Opcode.CHUNK, [
          ...uint64LE(0), // start_time
          ...uint64LE(0), // end_time
          ...uint64LE(1), // decompressed size
          ...uint32LE(crc32(new Uint8Array([Opcode.CHANNEL]))), // decompressed crc32
          ...string(""), // compression
          ...uint64LE(1),
          Opcode.CHANNEL, // truncated record
        ]),

        ...record(Opcode.FOOTER, [
          ...uint64LE(0), // summary start
          ...uint64LE(0), // summary offset start
          ...uint32LE(0), // summary crc
        ]),
        ...MCAP_MAGIC,
      ]),
    );
    expect(() => reader.nextRecord()).toThrow("bytes remaining in chunk");
  });

  it("rejects message at top level with no prior channel", () => {
    const reader = new McapStreamReader();
    reader.append(
      new Uint8Array([
        ...MCAP_MAGIC,

        ...record(Opcode.MESSAGE, [
          ...uint16LE(42), // channel id
          ...uint32LE(0), // sequence
          ...uint64LE(0), // log time
          ...uint64LE(0), // publish time
        ]),

        ...record(Opcode.FOOTER, [
          ...uint64LE(0), // summary start
          ...uint64LE(0), // summary offset start
          ...uint32LE(0), // summary crc
        ]),
        ...MCAP_MAGIC,
      ]),
    );
    expect(() => reader.nextRecord()).toThrow(
      "Encountered message on channel 42 without prior channel record",
    );
  });

  it("rejects message in chunk with no prior channel", () => {
    const message = record(Opcode.MESSAGE, [
      ...uint16LE(42), // channel id
      ...uint32LE(0), // sequence
      ...uint64LE(0), // log time
      ...uint64LE(0), // publish time
    ]);
    const reader = new McapStreamReader();
    reader.append(
      new Uint8Array([
        ...MCAP_MAGIC,

        ...record(Opcode.CHUNK, [
          ...uint64LE(0), // start_time
          ...uint64LE(0), // end_time
          ...uint64LE(0), // decompressed size
          ...uint32LE(crc32(message)), // decompressed crc32
          ...string(""), // compression
          ...uint64LE(message.byteLength),
          ...message,
        ]),

        ...record(Opcode.FOOTER, [
          ...uint64LE(0), // summary start
          ...uint64LE(0), // summary offset start
          ...uint32LE(0), // summary crc
        ]),
        ...MCAP_MAGIC,
      ]),
    );
    expect(() => reader.nextRecord()).toThrow(
      "Encountered message on channel 42 without prior channel",
    );
  });

  it("rejects schema data with incorrect length prefix", () => {
    const reader = new McapStreamReader();
    reader.append(
      new Uint8Array([
        ...MCAP_MAGIC,
        ...record(Opcode.SCHEMA, [
          ...uint16LE(42), // id
          ...string("name"), // name
          ...string("encoding"), // encoding
          ...uint32LE(3), // length prefix
          10,
          11,
        ]),
        ...record(Opcode.FOOTER, [
          ...uint64LE(0), // summary start
          ...uint64LE(0), // summary offset start
          ...uint32LE(0), // summary crc
        ]),
        ...MCAP_MAGIC,
      ]),
    );
    expect(() => reader.nextRecord()).toThrow("Schema data length 3 exceeds bounds of record");
  });

  it("rejects attachment data with incorrect length prefix", () => {
    const reader = new McapStreamReader();
    reader.append(
      new Uint8Array([
        ...MCAP_MAGIC,
        ...record(
          Opcode.ATTACHMENT,
          crcSuffix([
            ...uint64LE(2), // log time
            ...uint64LE(1), // create time
            ...string("myFile"), // name
            ...string("text/plain"), // media type
            ...uint64LE(3), // data length
            10,
            11,
          ]),
        ),
        ...record(Opcode.FOOTER, [
          ...uint64LE(0), // summary start
          ...uint64LE(0), // summary offset start
          ...uint32LE(0), // summary crc
        ]),
        ...MCAP_MAGIC,
      ]),
    );
    expect(() => reader.nextRecord()).toThrow("Attachment data length 3 exceeds bounds of record");
  });

  it("parses channel at top level", () => {
    const reader = new McapStreamReader();
    reader.append(
      new Uint8Array([
        ...MCAP_MAGIC,

        ...record(Opcode.CHANNEL, [
          ...uint16LE(1), // channel id
          ...uint16LE(2), // schema id
          ...string("myTopic"), // topic
          ...string("utf12"), // message encoding
          ...keyValues(string, string, [["foo", "bar"]]), // user data
        ]),

        ...record(Opcode.FOOTER, [
          ...uint64LE(0), // summary start
          ...uint64LE(0), // summary offset start
          ...uint32LE(0), // summary crc
        ]),
        ...MCAP_MAGIC,
      ]),
    );
    expect(reader.nextRecord()).toEqual({
      type: "Channel",
      id: 1,
      schemaId: 2,
      topic: "myTopic",
      messageEncoding: "utf12",
      metadata: new Map([["foo", "bar"]]),
    } as TypedMcapRecords["Channel"]);
    expect(reader.nextRecord()).toEqual({
      type: "Footer",
      summaryStart: 0,
      summaryOffsetStart: 0,
      summaryCrc: 0,
    });
    expect(reader.done()).toBe(true);
  });

  it.each([true, false])("parses channel in chunk (compressed: %s)", (compressed) => {
    const channel = record(Opcode.CHANNEL, [
      ...uint16LE(1), // channel id
      ...uint16LE(2), // schema id
      ...string("myTopic"), // topic
      ...string("utf12"), // message encoding
      ...keyValues(string, string, [["foo", "bar"]]), // user data
    ]);
    const decompressHandlers = { xyz: () => channel };
    const reader = new McapStreamReader(compressed ? { decompressHandlers } : undefined);

    const payload = compressed ? new TextEncoder().encode("compressed bytes") : channel;
    reader.append(
      new Uint8Array([
        ...MCAP_MAGIC,

        ...record(Opcode.CHUNK, [
          ...uint64LE(0), // start_time
          ...uint64LE(0), // end_time
          ...uint64LE(0), // decompressed size
          ...uint32LE(crc32(channel)), // decompressed crc32
          ...string(compressed ? "xyz" : ""), // compression
          ...uint64LE(payload.byteLength),
          ...payload,
        ]),

        ...record(Opcode.FOOTER, [
          ...uint64LE(0), // summary start
          ...uint64LE(0), // summary offset start
          ...uint32LE(0), // summary crc
        ]),
        ...MCAP_MAGIC,
      ]),
    );
    expect(reader.nextRecord()).toEqual({
      type: "Channel",
      id: 1,
      schemaId: 2,
      topic: "myTopic",
      messageEncoding: "utf12",
      metadata: new Map([["foo", "bar"]]),
    } as TypedMcapRecords["Channel"]);
    expect(reader.nextRecord()).toEqual({
      type: "Footer",
      summaryStart: 0,
      summaryOffsetStart: 0,
      summaryCrc: 0,
    });
    expect(reader.done()).toBe(true);
  });

  describe.each(["unchunked file", "same chunk", "different chunks"] as const)(
    "rejects channel with the same id in %s",
    (testType) => {
      it.each([
        {
          key: "topic",
          channel2: record(Opcode.CHANNEL, [
            ...uint16LE(42), // channel id
            ...uint16LE(1), // schema id
            ...string("XXXXXXXX"), // topic
            ...string("utf12"), // message encoding
            ...keyValues(string, string, [["foo", "bar"]]), // user data
          ]),
        },
        {
          key: "encoding",
          channel2: record(Opcode.CHANNEL, [
            ...uint16LE(42), // channel id
            ...uint16LE(1), // schema id
            ...string("myTopic"), // topic
            ...string("XXXXXXXX"), // message encoding
            ...keyValues(string, string, [["foo", "bar"]]), // user data
          ]),
        },
        {
          key: "schema_id",
          channel2: record(Opcode.CHANNEL, [
            ...uint16LE(42), // channel id
            ...uint16LE(0), // schema id
            ...string("myTopic"), // topic
            ...string("utf12"), // message encoding
            ...keyValues(string, string, [["foo", "bar"]]), // user data
          ]),
        },
      ])("differing in $key", ({ channel2 }) => {
        const channel = record(Opcode.CHANNEL, [
          ...uint16LE(42), // channel id
          ...uint16LE(1), // schema id
          ...string("myTopic"), // topic
          ...string("utf12"), // message encoding
          ...keyValues(string, string, [["foo", "bar"]]), // user data
        ]);
        const reader = new McapStreamReader();
        reader.append(
          new Uint8Array([
            ...MCAP_MAGIC,

            ...(testType === "unchunked file"
              ? [...channel, ...channel2]
              : testType === "same chunk"
                ? record(Opcode.CHUNK, [
                    ...uint64LE(0), // start_time
                    ...uint64LE(0), // end_time
                    ...uint64LE(0), // decompressed size
                    ...uint32LE(crc32(new Uint8Array([...channel, ...channel2]))), // decompressed crc32
                    ...string(""), // compression
                    ...uint64LE(channel.byteLength + channel2.byteLength),
                    ...channel,
                    ...channel2,
                  ])
                : [
                    ...record(Opcode.CHUNK, [
                      ...uint64LE(0), // start_time
                      ...uint64LE(0), // end_time
                      ...uint64LE(0), // decompressed size
                      ...uint32LE(crc32(new Uint8Array(channel))), // decompressed crc32
                      ...string(""), // compression
                      ...uint64LE(channel.byteLength),
                      ...channel,
                    ]),
                    ...record(Opcode.CHUNK, [
                      ...uint64LE(0), // start_time
                      ...uint64LE(0), // end_time
                      ...uint64LE(0), // decompressed size
                      ...uint32LE(crc32(new Uint8Array(channel2))), // decompressed crc32
                      ...string(""), // compression
                      ...uint64LE(channel2.byteLength),
                      ...channel2,
                    ]),
                  ]),

            ...record(Opcode.FOOTER, [
              ...uint64LE(0), // summary start
              ...uint64LE(0), // summary offset start
              ...uint32LE(0), // summary crc
            ]),
            ...MCAP_MAGIC,
          ]),
        );
        expect(reader.nextRecord()).toEqual({
          type: "Channel",
          id: 42,
          schemaId: 1,
          topic: "myTopic",
          messageEncoding: "utf12",
          metadata: new Map([["foo", "bar"]]),
        } as TypedMcapRecords["Channel"]);
        expect(() => reader.nextRecord()).toThrow(
          /Channel record for id 42 \(topic: (myTopic|XXXXXXXX)\) differs from previous channel record of the same id./,
        );
      });
    },
  );

  it("validates attachment crc", () => {
    const reader = new McapStreamReader();
    reader.append(
      new Uint8Array([
        ...MCAP_MAGIC,
        ...record(
          Opcode.ATTACHMENT,
          crcSuffix([
            ...uint64LE(2), // log time
            ...uint64LE(1), // create time
            ...string("myFile"), // name
            ...string("text/plain"), // media type
            ...uint64PrefixedBytes(new TextEncoder().encode("hello")), // data
          ]),
        ),
        ...record(Opcode.FOOTER, [
          ...uint64LE(0), // summary start
          ...uint64LE(0), // summary offset start
          ...uint32LE(0), // summary crc
        ]),
        ...MCAP_MAGIC,
      ]),
    );
    expect(reader.nextRecord()).toEqual({
      type: "Attachment",
      name: "myFile",
      logTime: 2,
      createTime: 1,
      mediaType: "text/plain",
      data: new TextEncoder().encode("hello"),
    } as TypedMcapRecords["Attachment"]);
    expect(reader.nextRecord()).toEqual({
      type: "Footer",
      summaryStart: 0,
      summaryOffsetStart: 0,
      summaryCrc: 0,
    });
    expect(reader.done()).toBe(true);
  });

  it("allows parsing to start at an offset into the file", () => {
    const channel = record(Opcode.CHANNEL, [
      ...uint16LE(1), // channel id
      ...uint16LE(0), // schema id
      ...string("myTopic"), // topic
      ...string("plain/text"), // message encoding
      ...keyValues(string, string, []), // user data
    ]);
    const fullMcap = new Uint8Array([
      ...MCAP_MAGIC,
      ...record(Opcode.CHUNK, [
        ...uint64LE(0), // start_time
        ...uint64LE(0), // end_time
        ...uint64LE(channel.byteLength), // decompressed size
        ...uint32LE(0), // decompressed crc32
        ...string(""), // compression
        ...uint64LE(channel.byteLength),
        ...channel,
      ]),
      ...record(Opcode.DATA_END, [
        ...uint32LE(0), // data section crc
      ]),
      ...record(Opcode.FOOTER, [
        ...uint64LE(0), // summary start
        ...uint64LE(0), // summary offset start
        ...uint32LE(0), // summary crc
      ]),
      ...MCAP_MAGIC,
    ]);

    const magicReader = new McapStreamReader();
    magicReader.append(fullMcap.slice(MCAP_MAGIC.length));
    expect(() => magicReader.nextRecord()).toThrow("Expected MCAP magic");

    const reader = new McapStreamReader({ noMagicPrefix: true, includeChunks: true });
    reader.append(fullMcap.slice(MCAP_MAGIC.length));
    expect(reader.nextRecord()).toEqual({
      type: "Chunk",
      messageStartTime: 0,
      messageEndTime: 0,
      uncompressedSize: channel.byteLength,
      uncompressedCrc: 0,
      compression: "",
      records: channel,
    });
    expect(reader.nextRecord()).toEqual({
      type: "Channel",
      id: 1,
      schemaId: 0,
      topic: "myTopic",
      messageEncoding: "plain/text",
      metadata: new Map(),
    });
    expect(reader.nextRecord()).toEqual({
      type: "DataEnd",
      dataSectionCrc: 0,
    });
    expect(reader.nextRecord()).toEqual({
      type: "Footer",
      summaryStart: 0,
      summaryOffsetStart: 0,
      summaryCrc: 0,
    });
    expect(reader.done()).toBe(true);
  });
});
