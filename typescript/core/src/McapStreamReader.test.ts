import { crc32 } from "@foxglove/crc";

import { McapRecordBuilder } from "./McapRecordBuilder.ts";
import McapStreamReader from "./McapStreamReader.ts";
import { MCAP_MAGIC, Opcode } from "./constants.ts";
import {
  record,
  uint64LE,
  uint32LE,
  string,
  uint16LE,
  keyValues,
  crcSuffix,
  uint64PrefixedBytes,
} from "./testUtils.ts";
import type { TypedMcapRecords } from "./types.ts";

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
          ...uint64LE(0x0123456789abcdefn), // summary start
          ...uint64LE(0x0123456789abcdefn), // summary offset start
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
          ...uint64LE(0x0123456789abcdefn), // summary start
          ...uint64LE(0x0123456789abcdefn), // summary offset start
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
          ...uint64LE(0x0123456789abcdefn), // summary start
          ...uint64LE(0x0123456789abcdefn), // summary offset start
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
          ...uint64LE(0x0123456789abcdefn), // summary start
          ...uint64LE(0x0123456789abcdefn), // summary offset start
          ...uint32LE(0x01234567), // summary crc
        ]),
        ...MCAP_MAGIC,
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
    const reader = new McapStreamReader();
    reader.append(
      new Uint8Array([
        ...MCAP_MAGIC,
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
        ...MCAP_MAGIC,
      ]),
    );
    expect(reader.nextRecord()).toEqual({
      type: "DataEnd",
      dataSectionCrc: 0,
    });
    expect(reader.nextRecord()).toEqual({
      type: "Footer",
      summaryStart: 0n,
      summaryOffsetStart: 0n,
      summaryCrc: 0,
    });
    expect(reader.done()).toBe(true);
  });

  it("waits patiently to parse one byte at a time, and rejects new data after read completed", () => {
    const reader = new McapStreamReader();
    const data = new Uint8Array([
      ...MCAP_MAGIC,
      ...record(Opcode.FOOTER, [
        ...uint64LE(0x0123456789abcdefn), // summary start
        ...uint64LE(0x0123456789abcdefn), // summary offset start
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
      summaryStart: 0x0123456789abcdefn,
      summaryOffsetStart: 0x0123456789abcdefn,
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
          ...uint64LE(0x0123456789abcdefn), // summary start
          ...uint64LE(0x0123456789abcdefn), // summary offset start
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
        ...MCAP_MAGIC,
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
    const reader = new McapStreamReader();
    reader.append(
      new Uint8Array([
        ...MCAP_MAGIC,

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
          ...uint64LE(0n), // log time
          ...uint64LE(0n), // publish time
        ]),

        ...record(Opcode.FOOTER, [
          ...uint64LE(0n), // summary start
          ...uint64LE(0n), // summary offset start
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
      ...uint64LE(0n), // log time
      ...uint64LE(0n), // publish time
    ]);
    const reader = new McapStreamReader();
    reader.append(
      new Uint8Array([
        ...MCAP_MAGIC,

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
          ...uint64LE(0n), // summary start
          ...uint64LE(0n), // summary offset start
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
            ...uint64LE(2n), // log time
            ...uint64LE(1n), // create time
            ...string("myFile"), // name
            ...string("text/plain"), // media type
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
          ...uint64LE(0n), // summary start
          ...uint64LE(0n), // summary offset start
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
      summaryStart: 0n,
      summaryOffsetStart: 0n,
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
      summaryStart: 0n,
      summaryOffsetStart: 0n,
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
                    ...uint64LE(0n), // start_time
                    ...uint64LE(0n), // end_time
                    ...uint64LE(0n), // decompressed size
                    ...uint32LE(crc32(new Uint8Array([...channel, ...channel2]))), // decompressed crc32
                    ...string(""), // compression
                    ...uint64LE(BigInt(channel.byteLength + channel2.byteLength)),
                    ...channel,
                    ...channel2,
                  ])
                : [
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
                      ...uint32LE(crc32(new Uint8Array(channel2))), // decompressed crc32
                      ...string(""), // compression
                      ...uint64LE(BigInt(channel2.byteLength)),
                      ...channel2,
                    ]),
                  ]),

            ...record(Opcode.FOOTER, [
              ...uint64LE(0n), // summary start
              ...uint64LE(0n), // summary offset start
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
            ...uint64LE(2n), // log time
            ...uint64LE(1n), // create time
            ...string("myFile"), // name
            ...string("text/plain"), // media type
            ...uint64PrefixedBytes(new TextEncoder().encode("hello")), // data
          ]),
        ),
        ...record(Opcode.FOOTER, [
          ...uint64LE(0n), // summary start
          ...uint64LE(0n), // summary offset start
          ...uint32LE(0), // summary crc
        ]),
        ...MCAP_MAGIC,
      ]),
    );
    expect(reader.nextRecord()).toEqual({
      type: "Attachment",
      name: "myFile",
      logTime: 2n,
      createTime: 1n,
      mediaType: "text/plain",
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
        ...uint64LE(0n), // start_time
        ...uint64LE(0n), // end_time
        ...uint64LE(BigInt(channel.byteLength)), // decompressed size
        ...uint32LE(0), // decompressed crc32
        ...string(""), // compression
        ...uint64LE(BigInt(channel.byteLength)),
        ...channel,
      ]),
      ...record(Opcode.DATA_END, [
        ...uint32LE(0), // data section crc
      ]),
      ...record(Opcode.FOOTER, [
        ...uint64LE(0n), // summary start
        ...uint64LE(0n), // summary offset start
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
      messageStartTime: 0n,
      messageEndTime: 0n,
      uncompressedSize: BigInt(channel.byteLength),
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
      summaryStart: 0n,
      summaryOffsetStart: 0n,
      summaryCrc: 0,
    });
    expect(reader.done()).toBe(true);
  });

  it("correctly appends new data to internal buffer", () => {
    const streamReader = new McapStreamReader({ includeChunks: true, noMagicPrefix: true });
    const recordBuilder = new McapRecordBuilder();

    const channel = {
      id: 0,
      messageEncoding: "json",
      schemaId: 0,
      topic: "foo",
      metadata: new Map(),
    };
    const messageSize = 1_000;
    const messageRecordBytes = 1 + 8 + 2 + 4 + 8 + 8 + messageSize;

    const makeMessage = (fillNumber: number) => ({
      channelId: 0,
      data: new Uint8Array(messageSize).fill(fillNumber),
      logTime: 0n,
      publishTime: 0n,
      sequence: 0,
    });

    const channelByteSize = recordBuilder.writeChannel(channel);
    streamReader.append(recordBuilder.buffer);
    expect(streamReader.bytesRemaining()).toBe(Number(channelByteSize));
    expect(streamReader.nextRecord()).toEqual({ ...channel, type: "Channel" });
    expect(streamReader.bytesRemaining()).toBe(0);

    // Add some messages and append them to the reader.
    recordBuilder.reset();
    recordBuilder.writeMessage(makeMessage(1));
    recordBuilder.writeMessage(makeMessage(2));
    streamReader.append(recordBuilder.buffer);
    expect(streamReader.bytesRemaining()).toBe(2 * messageRecordBytes);

    // Add one more message. Nothing has been consumed yet, but the internal buffer should be
    // large enough to simply append the new data.
    recordBuilder.reset();
    recordBuilder.writeMessage(makeMessage(3));
    streamReader.append(recordBuilder.buffer);
    expect(streamReader.bytesRemaining()).toBe(3 * messageRecordBytes);

    // Read some (but not all) messages to forward the reader's internal offset
    expect(streamReader.nextRecord()).toEqual({ ...makeMessage(1), type: "Message" });
    expect(streamReader.nextRecord()).toEqual({ ...makeMessage(2), type: "Message" });
    expect(streamReader.bytesRemaining()).toBe(1 * messageRecordBytes);

    // Add more messages. This will cause existing data to be shifted to the beginning of the buffer.
    recordBuilder.reset();
    recordBuilder.writeMessage(makeMessage(4));
    recordBuilder.writeMessage(makeMessage(5));
    streamReader.append(recordBuilder.buffer);
    expect(streamReader.bytesRemaining()).toBe(3 * messageRecordBytes);

    expect(streamReader.nextRecord()).toEqual({ ...makeMessage(3), type: "Message" });
    expect(streamReader.nextRecord()).toEqual({ ...makeMessage(4), type: "Message" });
    expect(streamReader.nextRecord()).toEqual({ ...makeMessage(5), type: "Message" });
    expect(streamReader.bytesRemaining()).toBe(0);
  });

  it("yields records with unknown opcodes", () => {
    const chunkedUnknownRecord = record(0x81 as Opcode, [5, 6, 7, 8]);

    const fullMcap = new Uint8Array([
      ...MCAP_MAGIC,
      // custom op code
      ...record(0x80 as Opcode, [1, 2, 3, 4]),
      ...record(Opcode.CHUNK, [
        ...uint64LE(0n), // start_time
        ...uint64LE(0n), // end_time
        ...uint64LE(BigInt(chunkedUnknownRecord.byteLength)), // decompressed size
        ...uint32LE(crc32(chunkedUnknownRecord)), // decompressed crc32
        ...string(""), // compression
        ...uint64LE(BigInt(chunkedUnknownRecord.byteLength)),
        ...chunkedUnknownRecord,
      ]),
      ...record(Opcode.FOOTER, [
        ...uint64LE(0n), // summary start
        ...uint64LE(0n), // summary offset start
        ...uint32LE(0), // summary crc
      ]),
      ...MCAP_MAGIC,
    ]);

    const reader = new McapStreamReader();
    reader.append(fullMcap);

    expect(reader.nextRecord()).toEqual({
      type: "Unknown",
      opcode: 0x80,
      data: new Uint8Array([1, 2, 3, 4]),
    });

    expect(reader.nextRecord()).toEqual({
      type: "Unknown",
      opcode: 0x81,
      data: new Uint8Array([5, 6, 7, 8]),
    });

    expect(reader.nextRecord()).toEqual({
      type: "Footer",
      summaryStart: 0n,
      summaryOffsetStart: 0n,
      summaryCrc: 0,
    });

    expect(reader.nextRecord()).toBeUndefined();
  });
});
