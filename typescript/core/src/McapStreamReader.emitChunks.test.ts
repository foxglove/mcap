import { crc32 } from "@foxglove/crc";
import { deflateSync, inflateSync } from "node:zlib";

import { McapRecordBuilder } from "./McapRecordBuilder.ts";
import McapStreamReader from "./McapStreamReader.ts";
import { Opcode } from "./constants.ts";
import { record } from "./testUtils.ts";
import type { TypedMcapRecord, TypedMcapRecords } from "./types.ts";

function fixture(compression = "deflate") {
  const header = { type: "Header", profile: "", library: "test" } as const;
  const channel = {
    type: "Channel",
    id: 1,
    schemaId: 0,
    topic: "/test",
    messageEncoding: "",
    metadata: new Map<string, string>(),
  } as const;
  const message = {
    type: "Message",
    channelId: 1,
    sequence: 0,
    logTime: 1n,
    publishTime: 1n,
    data: new Uint8Array([42]),
  } as const;
  const contents = new McapRecordBuilder();
  contents.writeChannel(channel);
  contents.writeChannel({ ...channel, id: 2 });
  const offset1 = BigInt(contents.length);
  contents.writeMessage(message);
  const offset2 = BigInt(contents.length);
  contents.writeMessage({ ...message, channelId: 2 });
  const chunk: TypedMcapRecords["Chunk"] = {
    type: "Chunk",
    messageStartTime: 1n,
    messageEndTime: 1n,
    compression,
    uncompressedSize: BigInt(contents.length),
    uncompressedCrc: crc32(contents.buffer),
    records:
      compression === "" ? contents.buffer.slice() : new Uint8Array(deflateSync(contents.buffer)),
  };
  const indexes: TypedMcapRecords["MessageIndex"][] = [
    { type: "MessageIndex", channelId: 1, records: [[1n, offset1]] },
    { type: "MessageIndex", channelId: 2, records: [[1n, offset2]] },
  ];
  const footer = {
    type: "Footer",
    summaryStart: 0n,
    summaryOffsetStart: 0n,
    summaryCrc: 0,
  } as const;
  const builder = new McapRecordBuilder();
  builder.writeMagic();
  builder.writeHeader(header);
  builder.writeChunk(chunk);
  for (const index of indexes) {
    builder.writeMessageIndex(index);
  }
  // Consecutive chunks without indexes, then a message whose channel is defined only in a chunk.
  builder.writeChunk(chunk);
  builder.writeChunk(chunk);
  builder.writeMessage(message);
  builder.writeDataEnd({ dataSectionCrc: 0 });
  builder.writeFooter(footer);
  builder.writeMagic();
  return {
    data: builder.buffer,
    chunk,
    expected: [
      header,
      chunk,
      ...indexes,
      chunk,
      chunk,
      message,
      { type: "DataEnd", dataSectionCrc: 0 },
      footer,
    ],
  };
}

function drain(reader: McapStreamReader): TypedMcapRecord[] {
  const records: TypedMcapRecord[] = [];
  for (let next; (next = reader.nextRecord()) != undefined; ) {
    records.push(next);
  }
  return records;
}

describe("McapStreamReader emitChunks", () => {
  it.each(["", "deflate", "future-codec"])(
    "emits only outer records with compression %j",
    (compression) => {
      const { data, expected } = fixture(compression);
      const reader = new McapStreamReader({ emitChunks: true });
      reader.append(data);
      expect(drain(reader)).toEqual(expected);
      expect(reader.done()).toBe(true);
    },
  );

  it.each([false, true])(
    "takes precedence over includeChunks=%s and ignores handlers",
    (includeChunks) => {
      const { data, expected } = fixture();
      const decompress = jest.fn(() => {
        throw new Error("must not decompress");
      });
      const reader = new McapStreamReader({
        emitChunks: true,
        includeChunks,
        decompressHandlers: { deflate: decompress },
      });
      reader.append(data);
      expect(drain(reader)).toEqual(expected);
      expect(decompress).not.toHaveBeenCalled();
    },
  );

  it.each([false, true])(
    "preserves chunk expansion with emitChunks=false, includeChunks=%s",
    (includeChunks) => {
      const { data } = fixture();
      const reader = new McapStreamReader({
        emitChunks: false,
        includeChunks,
        decompressHandlers: { deflate: (bytes) => new Uint8Array(inflateSync(bytes)) },
      });
      reader.append(data);
      const contents = [
        ...(includeChunks ? ["Chunk"] : []),
        "Channel",
        "Channel",
        "Message",
        "Message",
      ];
      expect(drain(reader).map((r) => r.type)).toEqual([
        "Header",
        ...contents,
        "MessageIndex",
        "MessageIndex",
        ...contents,
        ...contents,
        "Message",
        "DataEnd",
        "Footer",
      ]);
      expect(reader.done()).toBe(true);
    },
  );

  it("handles every two-part split and detects incomplete prefixes", () => {
    const { data, expected } = fixture();
    for (let split = 0; split < data.length; split++) {
      const reader = new McapStreamReader({ emitChunks: true });
      reader.append(data.subarray(0, split));
      const records = drain(reader);
      expect(reader.done()).toBe(false);
      reader.append(data.subarray(split));
      expect([...records, ...drain(reader)]).toEqual(expected);
      expect(reader.done()).toBe(true);
      expect(reader.bytesRemaining()).toBe(0);
    }
  });

  it("handles byte-by-byte input", () => {
    const { data, expected } = fixture();
    const reader = new McapStreamReader({ emitChunks: true });
    const records: TypedMcapRecord[] = [];
    for (const byte of data) {
      reader.append(new Uint8Array([byte]));
      records.push(...drain(reader));
    }
    expect(records).toEqual(expected);
    expect(reader.done()).toBe(true);
  });

  it("does not validate chunk contents or uncompressed CRCs", () => {
    const { chunk } = fixture("");
    const invalid = { ...chunk, records: new Uint8Array([Opcode.HEADER]), uncompressedCrc: 1 };
    const builder = new McapRecordBuilder();
    builder.writeMagic();
    builder.writeChunk(invalid);
    const reader = new McapStreamReader({ emitChunks: true });
    reader.append(builder.buffer);
    expect(reader.nextRecord()).toEqual(invalid);
    expect(reader.nextRecord()).toBeUndefined();
  });

  it.each(["prefix", "suffix"])("still rejects malformed %s magic", (which) => {
    const { data } = fixture();
    data[which === "prefix" ? 0 : data.length - 1] = 0;
    const reader = new McapStreamReader({ emitChunks: true });
    reader.append(data);
    expect(() => drain(reader)).toThrow("Expected MCAP magic");
  });

  it("retains compressed payloads through subsequent appends, compaction, and growth", () => {
    const { chunk } = fixture();
    const builder = new McapRecordBuilder();
    builder.writeMagic();
    builder.writeChunk(chunk);
    const input = builder.buffer;
    const reader = new McapStreamReader({ emitChunks: true });
    reader.append(input);
    input.fill(0);
    const retained = reader.nextRecord();
    expect(retained).toEqual(chunk);
    for (const size of [builder.length - 9, builder.length, builder.length * 10]) {
      reader.append(record(0x80 as Opcode, new Array<number>(size).fill(0xaa)));
      expect(reader.nextRecord()).toEqual({
        type: "Unknown",
        opcode: 0x80,
        data: new Uint8Array(size).fill(0xaa),
      });
      expect(retained).toEqual(chunk);
    }
  });
});
