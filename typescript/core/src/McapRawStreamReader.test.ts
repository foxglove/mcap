import { crc32 } from "@foxglove/crc";
import { deflateSync, inflateSync } from "node:zlib";

import { McapRecordBuilder } from "./McapRecordBuilder.ts";
import McapStreamReader from "./McapStreamReader.ts";
import { MCAP_MAGIC, Opcode } from "./constants.ts";
import { McapRawStreamReader } from "./index.ts";
import { record, string, uint32LE, uint64LE } from "./testUtils.ts";
import type { TypedMcapRecord, TypedMcapRecords } from "./types.ts";

const header = { type: "Header", profile: "", library: "raw-reader-test" } as const;
const footer = { type: "Footer", summaryStart: 0n, summaryOffsetStart: 0n, summaryCrc: 0 } as const;
const schema = {
  type: "Schema",
  id: 1,
  name: "example",
  encoding: "jsonschema",
  data: new Uint8Array([123, 125]),
} as const;
const channel = {
  type: "Channel",
  id: 1,
  schemaId: 1,
  topic: "/example",
  messageEncoding: "json",
  metadata: new Map<string, string>(),
} as const;
const message = {
  type: "Message",
  channelId: 1,
  sequence: 0,
  logTime: 1n,
  publishTime: 1n,
  data: new Uint8Array([49]),
} as const;

function chunk(records: Uint8Array, compression = ""): TypedMcapRecords["Chunk"] {
  return {
    type: "Chunk",
    messageStartTime: 1n,
    messageEndTime: 2n,
    uncompressedSize: BigInt(records.byteLength),
    uncompressedCrc: crc32(records),
    compression,
    records: compression === "deflate" ? new Uint8Array(deflateSync(records)) : records,
  };
}

function finish(builder: McapRecordBuilder): Uint8Array {
  builder.writeDataEnd({ dataSectionCrc: 0 });
  builder.writeFooter(footer);
  builder.writeMagic();
  return builder.buffer;
}

function drain(reader: McapRawStreamReader | McapStreamReader): TypedMcapRecord[] {
  const records: TypedMcapRecord[] = [];
  for (let next; (next = reader.nextRecord()) != undefined; ) {
    records.push(next);
  }
  return records;
}

function fixture(): { data: Uint8Array; expected: TypedMcapRecord[] } {
  const contents = new McapRecordBuilder();
  contents.writeSchema(schema);
  contents.writeChannel(channel);
  const offset1 = BigInt(contents.length);
  contents.writeMessage(message);
  contents.writeChannel({ ...channel, id: 2 });
  const offset2 = BigInt(contents.length);
  contents.writeMessage({ ...message, channelId: 2 });
  const compressed = chunk(contents.buffer, "deflate");
  const index1: TypedMcapRecords["MessageIndex"] = {
    type: "MessageIndex",
    channelId: 1,
    records: [[1n, offset1]],
  };
  const index2: TypedMcapRecords["MessageIndex"] = {
    type: "MessageIndex",
    channelId: 2,
    records: [[1n, offset2]],
  };
  const empty = chunk(new Uint8Array());
  const builder = new McapRecordBuilder();
  builder.writeMagic();
  builder.writeHeader(header);
  builder.writeChunk(compressed);
  builder.writeMessageIndex(index1);
  builder.writeMessageIndex(index2);
  builder.writeChunk(empty);
  builder.writeChunk(compressed);
  // This channel was defined only inside the first chunk.
  builder.writeMessage(message);
  return {
    data: finish(builder),
    expected: [
      header,
      compressed,
      index1,
      index2,
      empty,
      compressed,
      message,
      { type: "DataEnd", dataSectionCrc: 0 },
      footer,
    ],
  };
}

describe("McapRawStreamReader", () => {
  it("returns compressed chunks, multiple indexes, consecutive chunks, and standalone messages in order", () => {
    const { data, expected } = fixture();
    const reader = new McapRawStreamReader();
    reader.append(data);
    expect(drain(reader)).toEqual(expected);
    expect(reader.done()).toBe(true);
    expect(reader.bytesRemaining()).toBe(0);
    expect(reader.nextRecord()).toBeUndefined();
    expect(() => {
      reader.append(new Uint8Array([0]));
    }).toThrow("Already done reading");
  });

  it("handles every two-part input split", () => {
    const { data, expected } = fixture();
    for (let split = 1; split < data.length; split++) {
      const reader = new McapRawStreamReader();
      reader.append(data.subarray(0, split));
      const records = drain(reader);
      expect(reader.done()).toBe(false);
      reader.append(data.subarray(split));
      records.push(...drain(reader));
      expect(records).toEqual(expected);
      expect(reader.done()).toBe(true);
      expect(reader.bytesRemaining()).toBe(0);
    }
  });

  it("handles one-byte appends interleaved with empty appends", () => {
    const { data, expected } = fixture();
    const reader = new McapRawStreamReader();
    const records: TypedMcapRecord[] = [];
    for (const byte of data) {
      reader.append(new Uint8Array());
      records.push(...drain(reader));
      reader.append(new Uint8Array([byte]));
      records.push(...drain(reader));
    }
    expect(records).toEqual(expected);
    expect(reader.done()).toBe(true);
  });

  it("does not expand even uncompressed chunks or validate their contents, size, or CRC", () => {
    const contents = new McapRecordBuilder();
    contents.writeSchema(schema);
    contents.writeChannel(channel);
    contents.writeMessage(message);
    const builder = new McapRecordBuilder();
    builder.writeMagic();
    const valid = chunk(contents.buffer);
    const invalid = {
      ...chunk(new Uint8Array([Opcode.HEADER])),
      uncompressedCrc: 42,
      uncompressedSize: 999n,
    };
    builder.writeChunk(valid);
    builder.writeChunk(invalid);
    const reader = new McapRawStreamReader();
    reader.append(finish(builder));
    expect(drain(reader)).toEqual([valid, invalid, { type: "DataEnd", dataSectionCrc: 0 }, footer]);
  });

  it("accepts unknown compression without decompression handlers", () => {
    const builder = new McapRecordBuilder();
    builder.writeMagic();
    const compressed = chunk(new Uint8Array([1, 2, 3]), "future-codec");
    builder.writeChunk(compressed);
    const reader = new McapRawStreamReader();
    reader.append(finish(builder));
    expect(drain(reader)).toEqual([compressed, { type: "DataEnd", dataSectionCrc: 0 }, footer]);
  });

  it("returns standalone records without message/channel relationship validation", () => {
    const builder = new McapRecordBuilder();
    builder.writeMagic();
    builder.writeHeader(header);
    builder.writeSchema(schema);
    builder.writeMessage(message);
    builder.writeChannel(channel);
    const otherChannel = { ...channel, topic: "/different" };
    builder.writeChannel(otherChannel);
    const attachment = {
      type: "Attachment",
      logTime: 0n,
      createTime: 0n,
      name: "file",
      mediaType: "application/octet-stream",
      data: new Uint8Array([4, 5]),
    } as const;
    builder.writeAttachment(attachment);
    const metadata = { type: "Metadata", name: "meta", metadata: new Map([["a", "b"]]) } as const;
    builder.writeMetadata(metadata);
    const reader = new McapRawStreamReader();
    reader.append(finish(builder));
    expect(drain(reader)).toEqual([
      header,
      schema,
      message,
      channel,
      otherChannel,
      attachment,
      metadata,
      { type: "DataEnd", dataSectionCrc: 0 },
      footer,
    ]);
  });

  it("leaves done false for every incomplete prefix, including at record boundaries", () => {
    const { data } = fixture();
    for (let end = 0; end < data.length; end++) {
      const reader = new McapRawStreamReader();
      reader.append(data.subarray(0, end));
      drain(reader);
      expect(reader.done()).toBe(false);
      expect(reader.nextRecord()).toBeUndefined();
    }
  });

  it("reports unparsed bytes and waits for the footer's trailing magic", () => {
    const builder = new McapRecordBuilder();
    builder.writeMagic();
    builder.writeFooter(footer);
    const reader = new McapRawStreamReader();
    reader.append(builder.buffer.subarray(0, MCAP_MAGIC.length + 3));
    expect(reader.nextRecord()).toBeUndefined();
    expect(reader.bytesRemaining()).toBe(3);
    reader.append(builder.buffer.subarray(MCAP_MAGIC.length + 3));
    expect(reader.nextRecord()).toBeUndefined();
    expect(reader.bytesRemaining()).toBe(0);
    expect(reader.done()).toBe(false);
    reader.append(new Uint8Array(MCAP_MAGIC));
    expect(reader.nextRecord()).toEqual(footer);
    expect(reader.done()).toBe(true);
  });

  it("supports noMagicPrefix while still requiring trailing magic", () => {
    const { data, expected } = fixture();
    const reader = new McapRawStreamReader({ noMagicPrefix: true });
    reader.append(data.subarray(MCAP_MAGIC.length, data.length - MCAP_MAGIC.length));
    expect(drain(reader)).toEqual(expected.slice(0, -1));
    expect(reader.done()).toBe(false);
    reader.append(new Uint8Array(MCAP_MAGIC));
    expect(reader.nextRecord()).toEqual(footer);
    expect(reader.done()).toBe(true);
  });

  it.each(["prefix", "suffix"])("rejects malformed %s magic", (which) => {
    const { data } = fixture();
    data[which === "prefix" ? 0 : data.length - 1] = 0;
    const reader = new McapRawStreamReader();
    reader.append(data);
    expect(() => drain(reader)).toThrow(
      which === "prefix"
        ? /Expected MCAP magic/
        : /Expected MCAP magic.*\[library=raw-reader-test\]/,
    );
  });

  it("rejects bytes following the trailing magic", () => {
    const { data } = fixture();
    const reader = new McapRawStreamReader();
    reader.append(new Uint8Array([...data, 0]));
    expect(() => drain(reader)).toThrow("1 bytes remaining after MCAP footer and trailing magic");
  });

  it("rejects duplicate headers", () => {
    const builder = new McapRecordBuilder();
    builder.writeMagic();
    builder.writeHeader(header);
    builder.writeHeader(header);
    const reader = new McapRawStreamReader();
    reader.append(builder.buffer);
    expect(() => drain(reader)).toThrow("Duplicate Header record");
  });

  it("rejects record lengths that cannot be represented safely", () => {
    const reader = new McapRawStreamReader();
    reader.append(new Uint8Array([...MCAP_MAGIC, Opcode.CHUNK, ...uint64LE(2n ** 53n)]));
    expect(() => drain(reader)).toThrow("Record content length 9007199254740992 is too large");
  });

  it("rejects a chunk payload length exceeding its record", () => {
    const reader = new McapRawStreamReader();
    reader.append(
      new Uint8Array([
        ...MCAP_MAGIC,
        ...record(Opcode.CHUNK, [
          ...uint64LE(0n),
          ...uint64LE(0n),
          ...uint64LE(0n),
          ...uint32LE(0),
          ...string(""),
          ...uint64LE(100n),
        ]),
      ]),
    );
    expect(() => drain(reader)).toThrow("Chunk records length exceeds remaining record size");
  });

  it("validates attachment CRCs by default and allows opting out", () => {
    const builder = new McapRecordBuilder();
    builder.writeMagic();
    builder.writeAttachment({
      logTime: 0n,
      createTime: 0n,
      name: "file",
      mediaType: "",
      data: new Uint8Array([1, 2]),
    });
    const bytes = builder.buffer.slice();
    bytes[bytes.length - 1] = bytes[bytes.length - 1]! ^ 0xff;
    const reader = new McapRawStreamReader();
    reader.append(bytes);
    expect(() => drain(reader)).toThrow("Attachment CRC32 mismatch");
    const unchecked = new McapRawStreamReader({ validateCrcs: false });
    unchecked.append(bytes);
    expect(unchecked.nextRecord()?.type).toBe("Attachment");
  });

  it("owns input and returned payloads across buffer appends, compaction, and growth", () => {
    const builder = new McapRecordBuilder();
    builder.writeMagic();
    builder.writeSchema(schema);
    builder.writeMessage(message);
    const compressed = chunk(new Uint8Array([1, 2, 3]), "deflate");
    builder.writeChunk(compressed);
    builder.writeAttachment({
      logTime: 0n,
      createTime: 0n,
      name: "file",
      mediaType: "",
      data: new Uint8Array([8, 9]),
    });
    const input = new Uint8Array([...builder.buffer, ...record(0x80 as Opcode, [7, 6, 5])]);
    const reader = new McapRawStreamReader();
    reader.append(input);
    input.fill(0);
    const retained = drain(reader);
    const expected = structuredClone(retained);
    expect(retained[2]).toEqual(compressed);
    expect(retained[4]).toEqual({ type: "Unknown", opcode: 0x80, data: new Uint8Array([7, 6, 5]) });
    // Similar-size appends first fill unused capacity, then force compaction; the large one grows it.
    for (const size of [input.length, input.length, input.length * 10]) {
      reader.append(record(0x81 as Opcode, new Array<number>(size).fill(0xaa)));
      expect(reader.nextRecord()?.type).toBe("Unknown");
      expect(retained).toEqual(expected);
    }
    const compressedRecord = retained[2];
    if (compressedRecord?.type !== "Chunk") {
      throw new Error("Expected chunk");
    }
    compressedRecord.records.fill(0);
    const end = new McapRecordBuilder();
    reader.append(finish(end));
    expect(drain(reader)).toEqual([{ type: "DataEnd", dataSectionCrc: 0 }, footer]);
  });

  it("preserves McapStreamReader chunk expansion, includeChunks ordering, and decompression timing", () => {
    const { data } = fixture();
    const decompress = jest.fn((bytes: Uint8Array) => new Uint8Array(inflateSync(bytes)));
    const reader = new McapStreamReader({
      includeChunks: true,
      decompressHandlers: { deflate: decompress },
    });
    reader.append(data);
    expect(reader.nextRecord()).toEqual(header);
    expect(reader.nextRecord()?.type).toBe("Chunk");
    expect(decompress).not.toHaveBeenCalled();
    expect(reader.nextRecord()).toEqual(schema);
    expect(decompress).toHaveBeenCalledTimes(1);
    expect(reader.nextRecord()).toEqual(channel);
    expect(reader.nextRecord()).toEqual(message);
    expect(drain(reader).map((r) => r.type)).toEqual([
      "Channel",
      "Message",
      "MessageIndex",
      "MessageIndex",
      "Chunk",
      "Chunk",
      "Schema",
      "Channel",
      "Message",
      "Channel",
      "Message",
      "Message",
      "DataEnd",
      "Footer",
    ]);
    expect(decompress).toHaveBeenCalledTimes(2);
    expect(reader.done()).toBe(true);
  });
});
