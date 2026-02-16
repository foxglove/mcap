import { crc32 } from "@foxglove/crc";

import { ChunkBuilder } from "./ChunkBuilder.ts";
import { McapIndexedReader } from "./McapIndexedReader.ts";
import { McapRecordBuilder } from "./McapRecordBuilder.ts";
import { McapWriter } from "./McapWriter.ts";
import { TempBuffer } from "./TempBuffer.ts";
import { MCAP_MAGIC, Opcode } from "./constants.ts";
import {
  record,
  uint64LE,
  uint32LE,
  string,
  keyValues,
  collect,
  uint16LE,
  uint32PrefixedBytes,
} from "./testUtils.ts";
import type { Channel, TypedMcapRecord, TypedMcapRecords } from "./types.ts";

/**
 * Create an IReadable from a buffer. Simulates small buffer reuse to help test that readers aren't
 * holding onto buffers without copying them.
 */
function makeReadable(data: Uint8Array) {
  let readCalls = 0;
  const reusableBuffer = new Uint8Array(data.byteLength);
  return {
    get readCalls() {
      return readCalls;
    },
    size: async () => BigInt(data.length),
    read: async (offset: bigint, size: bigint) => {
      ++readCalls;
      if (offset > Number.MAX_SAFE_INTEGER || size > Number.MAX_SAFE_INTEGER) {
        throw new Error(`Read too large: offset ${offset}, size ${size}`);
      }
      if (offset < 0 || size < 0 || offset + size > data.length) {
        throw new Error(
          `Read out of range: offset ${offset}, size ${size} (data.length: ${data.length})`,
        );
      }
      reusableBuffer.set(
        new Uint8Array(data.buffer, data.byteOffset + Number(offset), Number(size)),
      );
      reusableBuffer.fill(0xff, Number(size));
      return new Uint8Array(reusableBuffer.buffer, 0, Number(size));
    },
  };
}

function writeChunkWithMessageIndexes(
  builder: McapRecordBuilder,
  chunk: ChunkBuilder,
): TypedMcapRecords["ChunkIndex"] {
  const chunkStartOffset = BigInt(builder.length);
  const chunkLength = builder.writeChunk({
    messageStartTime: chunk.messageStartTime,
    messageEndTime: chunk.messageEndTime,
    uncompressedSize: BigInt(chunk.buffer.length),
    uncompressedCrc: 0,
    compression: "",
    records: chunk.buffer,
  });

  const messageIndexStart = BigInt(builder.length);
  let messageIndexLength = 0n;
  const chunkMessageIndexOffsets = new Map<number, bigint>();
  for (const messageIndex of chunk.indices) {
    chunkMessageIndexOffsets.set(messageIndex.channelId, messageIndexStart + messageIndexLength);
    messageIndexLength += builder.writeMessageIndex(messageIndex);
  }

  return {
    type: "ChunkIndex",
    messageStartTime: chunk.messageStartTime,
    messageEndTime: chunk.messageEndTime,
    chunkStartOffset,
    chunkLength,
    messageIndexOffsets: chunkMessageIndexOffsets,
    messageIndexLength,
    compression: "",
    compressedSize: BigInt(chunk.buffer.byteLength),
    uncompressedSize: BigInt(chunk.buffer.byteLength),
  };
}

describe("McapIndexedReader", () => {
  it("rejects files that are too small", async () => {
    await expect(
      McapIndexedReader.Initialize({
        readable: makeReadable(
          new Uint8Array([
            ...MCAP_MAGIC,
            ...record(Opcode.FOOTER, [
              ...uint64LE(0n), // summary start
              ...uint64LE(0n), // summary offset start
              ...uint32LE(0), // summary crc
            ]),
            ...MCAP_MAGIC,
          ]),
        ),
      }),
    ).rejects.toThrow("Unable to read header at beginning of file; found Footer");

    await expect(
      McapIndexedReader.Initialize({
        readable: makeReadable(
          new Uint8Array([
            ...MCAP_MAGIC,
            ...record(Opcode.HEADER, [
              ...string(""), // profile
              ...string(""), // library
            ]),
            ...MCAP_MAGIC,
          ]),
        ),
      }),
    ).rejects.toThrow("too small to be valid MCAP");
  });

  it("rejects unindexed file", async () => {
    const readable = makeReadable(
      new Uint8Array([
        ...MCAP_MAGIC,
        ...record(Opcode.HEADER, [
          ...string(""), // profile
          ...string(""), // library
        ]),
        ...record(Opcode.FOOTER, [
          ...uint64LE(0n), // summary start
          ...uint64LE(0n), // summary offset start
          ...uint32LE(0), // summary crc
        ]),
        ...MCAP_MAGIC,
      ]),
    );
    await expect(McapIndexedReader.Initialize({ readable })).rejects.toThrow("File is not indexed");
  });

  it("includes library in error messages", async () => {
    const readable = makeReadable(
      new Uint8Array([
        ...MCAP_MAGIC,
        ...record(Opcode.HEADER, [
          ...string(""), // profile
          ...string("lib"), // library
        ]),
        ...record(Opcode.FOOTER, [
          ...uint64LE(0n), // summary start
          ...uint64LE(0n), // summary offset start
          ...uint32LE(0), // summary crc
        ]),
        ...MCAP_MAGIC,
      ]),
    );
    await expect(McapIndexedReader.Initialize({ readable })).rejects.toThrow(
      "File is not indexed [library=lib]",
    );
  });

  it("rejects invalid index crc", async () => {
    const data = [
      ...MCAP_MAGIC,
      ...record(Opcode.HEADER, [
        ...string(""), // profile
        ...string(""), // library
      ]),
      ...record(Opcode.DATA_END, [...uint32LE(0)]),
    ];
    const summaryStart = data.length;

    data.push(
      ...record(Opcode.METADATA, [
        ...string("foobar"),
        ...keyValues(string, string, []), // metadata
      ]),
    );

    data.push(
      ...record(Opcode.FOOTER, [
        ...uint64LE(BigInt(summaryStart)), // summary start
        ...uint64LE(0n), // summary offset start
        ...uint32LE(crc32(new Uint8Array([42]))), // summary crc
      ]),
      ...MCAP_MAGIC,
    );
    const readable = makeReadable(new Uint8Array(data));
    await expect(McapIndexedReader.Initialize({ readable })).rejects.toThrow(
      "Incorrect summary CRC 1656343536 (expected 163128923)",
    );
  });

  it("parses index with schema and channel", async () => {
    const data = [
      ...MCAP_MAGIC,
      ...record(Opcode.HEADER, [
        ...string(""), // profile
        ...string(""), // library
      ]),
      ...record(Opcode.DATA_END, [...uint32LE(0)]),
    ];
    const summaryStart = data.length;
    data.push(
      ...record(Opcode.SCHEMA, [
        ...uint16LE(1), // schema id
        ...string("some data"), // schema name
        ...string("json"), // schema format
        ...uint32PrefixedBytes(new TextEncoder().encode("stuff")), // schema
      ]),
      ...record(Opcode.CHANNEL, [
        ...uint16LE(42), // channel id
        ...uint16LE(1), // schema id
        ...string("myTopic"), // topic
        ...string("utf12"), // encoding
        ...keyValues(string, string, [["foo", "bar"]]), // user data
      ]),
      ...record(Opcode.FOOTER, [
        ...uint64LE(BigInt(summaryStart)), // summary start
        ...uint64LE(0n), // summary offset start
        ...uint32LE(crc32(new Uint8Array(0))), // summary crc
      ]),
      ...MCAP_MAGIC,
    );
    const readable = makeReadable(new Uint8Array(data));
    const reader = await McapIndexedReader.Initialize({ readable });
    await expect(collect(reader.readMessages())).resolves.toEqual([]);
    expect(reader.channelsById).toEqual(
      new Map<number, TypedMcapRecords["Channel"]>([
        [
          42,
          {
            type: "Channel",
            id: 42,
            schemaId: 1,
            topic: "myTopic",
            messageEncoding: "utf12",
            metadata: new Map([["foo", "bar"]]),
          },
        ],
      ]),
    );
    expect(reader.schemasById).toEqual(
      new Map<number, TypedMcapRecords["Schema"]>([
        [
          1,
          {
            type: "Schema",
            id: 1,
            name: "some data",
            encoding: "json",
            data: new TextEncoder().encode("stuff"),
          },
        ],
      ]),
    );
    expect(readable.readCalls).toBe(4);
  });

  describe("indexed with single channel", () => {
    const message1: TypedMcapRecords["Message"] = {
      type: "Message",
      channelId: 42,
      sequence: 1,
      publishTime: 0n,
      logTime: 10n,
      data: new Uint8Array(),
    };
    const message2: TypedMcapRecords["Message"] = {
      type: "Message",
      channelId: 42,
      sequence: 2,
      publishTime: 1n,
      logTime: 11n,
      data: new Uint8Array(),
    };
    const message3: TypedMcapRecords["Message"] = {
      type: "Message",
      channelId: 42,
      sequence: 3,
      publishTime: 2n,
      logTime: 12n,
      data: new Uint8Array(),
    };
    it.each([
      { startTime: undefined, endTime: undefined, expected: [message1, message2, message3] },
      { startTime: 11n, endTime: 11n, expected: [message2] },
      { startTime: 11n, endTime: undefined, expected: [message2, message3] },
      { startTime: undefined, endTime: 11n, expected: [message1, message2] },
      { startTime: 10n, endTime: 12n, expected: [message1, message2, message3] },
    ])(
      "fetches chunk data and reads requested messages between $startTime and $endTime",
      async ({ startTime, endTime, expected }) => {
        const schema = record(Opcode.SCHEMA, [
          ...uint16LE(1), // schema id
          ...string("some data"), // schema name
          ...string("json"), // schema format
          ...uint32PrefixedBytes(new TextEncoder().encode("stuff")), // schema
        ]);
        const channel = record(Opcode.CHANNEL, [
          ...uint16LE(42), // channel id
          ...uint16LE(1), // schema id
          ...string("myTopic"), // topic
          ...string("utf12"), // message encoding
          ...keyValues(string, string, [["foo", "bar"]]), // user data
        ]);
        const message1Data = record(Opcode.MESSAGE, [
          ...uint16LE(message1.channelId),
          ...uint32LE(message1.sequence),
          ...uint64LE(message1.logTime),
          ...uint64LE(message1.publishTime),
        ]);
        const message2Data = record(Opcode.MESSAGE, [
          ...uint16LE(message2.channelId),
          ...uint32LE(message2.sequence),
          ...uint64LE(message2.logTime),
          ...uint64LE(message2.publishTime),
        ]);
        const message3Data = record(Opcode.MESSAGE, [
          ...uint16LE(message3.channelId),
          ...uint32LE(message3.sequence),
          ...uint64LE(message3.logTime),
          ...uint64LE(message3.publishTime),
        ]);
        const chunkContents = [...schema, ...channel];
        const message1Offset = BigInt(chunkContents.length);
        chunkContents.push(...message1Data);
        const message2Offset = BigInt(chunkContents.length);
        chunkContents.push(...message2Data);
        const message3Offset = BigInt(chunkContents.length);
        chunkContents.push(...message3Data);

        const data = [
          ...MCAP_MAGIC,
          ...record(Opcode.HEADER, [
            ...string(""), // profile
            ...string(""), // library
          ]),
        ];
        const chunkOffset = BigInt(data.length);
        data.push(
          ...record(Opcode.CHUNK, [
            ...uint64LE(0n), // start time
            ...uint64LE(0n), // end time
            ...uint64LE(0n), // decompressed size
            ...uint32LE(crc32(new Uint8Array(chunkContents))), // decompressed crc32
            ...string(""), // compression
            ...uint64LE(BigInt(chunkContents.length)),
            ...chunkContents,
          ]),
        );
        const chunkLength = BigInt(data.length) - chunkOffset;
        const messageIndexOffset = BigInt(data.length);
        data.push(
          ...record(Opcode.MESSAGE_INDEX, [
            ...uint16LE(42), // channel id
            ...keyValues(uint64LE, uint64LE, [
              [message1.logTime, message1Offset],
              [message2.logTime, message2Offset],
              [message3.logTime, message3Offset],
            ]), // records
          ]),
        );
        const messageIndexLength = BigInt(data.length) - messageIndexOffset;
        data.push(...record(Opcode.DATA_END, [...uint32LE(0)]));
        const summaryStart = data.length;
        data.push(
          ...channel,
          ...record(Opcode.CHUNK_INDEX, [
            ...uint64LE(message1.logTime), // start time
            ...uint64LE(message3.logTime), // end time
            ...uint64LE(chunkOffset), // offset
            ...uint64LE(chunkLength), // chunk length
            ...keyValues(uint16LE, uint64LE, [[42, messageIndexOffset]]), // message index offsets
            ...uint64LE(messageIndexLength), // message index length
            ...string(""), // compression
            ...uint64LE(BigInt(chunkContents.length)), // compressed size
            ...uint64LE(BigInt(chunkContents.length)), // uncompressed size
          ]),
          ...record(Opcode.FOOTER, [
            ...uint64LE(BigInt(summaryStart)), // summary start
            ...uint64LE(0n), // summary offset start
            ...uint32LE(crc32(new Uint8Array(0))), // summary crc
          ]),
          ...MCAP_MAGIC,
        );

        {
          const readable = makeReadable(new Uint8Array(data));
          const reader = await McapIndexedReader.Initialize({ readable });
          const collected = await collect(reader.readMessages({ startTime, endTime }));
          expect(collected).toEqual(expected);
          expect(readable.readCalls).toBe(6);
        }

        {
          const readable = makeReadable(new Uint8Array(data));
          const reader = await McapIndexedReader.Initialize({ readable });
          const collected = await collect(
            reader.readMessages({ startTime, endTime, reverse: true }),
          );
          expect(collected).toEqual(expected.reverse());
          expect(readable.readCalls).toBe(6);
        }
      },
    );
  });

  describe("indexed with multiple channels", () => {
    const messages = [
      {
        type: "Message",
        sequence: 1,
        publishTime: 0n,
        logTime: 10n,
        data: new Uint8Array(),
      },
      {
        type: "Message",
        sequence: 2,
        publishTime: 1n,
        logTime: 11n,
        data: new Uint8Array(),
      },
    ];
    it.each([
      { startTime: undefined, endTime: undefined, expectedIndices: [0, 1] },
      { startTime: undefined, endTime: 10n, expectedIndices: [0] },
      { startTime: 11n, endTime: 11n, expectedIndices: [1] },
      { startTime: 11n, endTime: undefined, expectedIndices: [1] },
      { startTime: undefined, endTime: 11n, expectedIndices: [0, 1] },
      { startTime: 10n, endTime: 12n, expectedIndices: [0, 1] },
    ])(
      "fetches chunk data and reads requested messages between $startTime and $endTime",
      async ({ startTime, endTime, expectedIndices }) => {
        const tempBuffer = new TempBuffer();
        const writer = new McapWriter({ writable: tempBuffer });
        await writer.start({ library: "", profile: "" });
        const channelId1 = await writer.registerChannel({
          topic: "test1",
          schemaId: 0,
          messageEncoding: "json",
          metadata: new Map(),
        });
        const channelId2 = await writer.registerChannel({
          topic: "test2",
          schemaId: 0,
          messageEncoding: "json",
          metadata: new Map(),
        });
        const channelIds = [channelId1, channelId2];
        await writer.addMessage({ channelId: channelId1, ...messages[0]! });
        await writer.addMessage({ channelId: channelId2, ...messages[1]! });
        await writer.end();

        {
          const reader = await McapIndexedReader.Initialize({ readable: tempBuffer });
          const collected = await collect(reader.readMessages({ startTime, endTime }));
          expect(collected).toEqual(
            expectedIndices.map((i) => ({ channelId: channelIds[i]!, ...messages[i]! })),
          );
        }

        {
          const reader = await McapIndexedReader.Initialize({ readable: tempBuffer });
          const collected = await collect(
            reader.readMessages({ startTime, endTime, reverse: true }),
          );
          expect(collected).toEqual(
            expectedIndices.map((i) => ({ channelId: channelIds[i]!, ...messages[i]! })).reverse(),
          );
        }
      },
    );
  });

  it("sorts and merges out-of-order and overlapping chunks", async () => {
    const channel1: TypedMcapRecord = {
      type: "Channel",
      id: 1,
      schemaId: 0,
      topic: "a",
      messageEncoding: "utf12",
      metadata: new Map(),
    };
    const makeMessage = (idx: number): TypedMcapRecords["Message"] => {
      return {
        type: "Message",
        channelId: channel1.id,
        sequence: idx,
        logTime: BigInt(idx),
        publishTime: 0n,
        data: new Uint8Array(),
      };
    };
    const message1 = makeMessage(1);
    const message2 = makeMessage(2);
    const message3 = makeMessage(3);
    const message4 = makeMessage(4);
    const message5 = makeMessage(5);
    const message6 = makeMessage(6);

    const chunk1 = new ChunkBuilder({ useMessageIndex: true });
    chunk1.addChannel(channel1);
    chunk1.addMessage(message6);

    const chunk2 = new ChunkBuilder({ useMessageIndex: true });
    chunk2.addChannel(channel1);
    chunk2.addMessage(message2);
    chunk2.addMessage(message5);

    const chunk3 = new ChunkBuilder({ useMessageIndex: true });
    chunk3.addChannel(channel1);
    chunk3.addMessage(message4);
    chunk3.addMessage(message3);

    const chunk4 = new ChunkBuilder({ useMessageIndex: true });
    chunk4.addChannel(channel1);
    chunk4.addMessage(message1);

    const builder = new McapRecordBuilder();
    builder.writeMagic();
    builder.writeHeader({ profile: "", library: "" });

    const chunkIndexes: TypedMcapRecords["ChunkIndex"][] = [];
    chunkIndexes.push(writeChunkWithMessageIndexes(builder, chunk1));
    chunkIndexes.push(writeChunkWithMessageIndexes(builder, chunk2));
    chunkIndexes.push(writeChunkWithMessageIndexes(builder, chunk3));
    chunkIndexes.push(writeChunkWithMessageIndexes(builder, chunk4));

    builder.writeDataEnd({ dataSectionCrc: 0 });

    const summaryStart = BigInt(builder.length);

    for (const index of chunkIndexes) {
      builder.writeChunkIndex(index);
    }

    builder.writeFooter({ summaryStart, summaryOffsetStart: 0n, summaryCrc: 0 });
    builder.writeMagic();

    const reader = await McapIndexedReader.Initialize({ readable: makeReadable(builder.buffer) });
    await expect(collect(reader.readMessages())).resolves.toEqual([
      message1,
      message2,
      message3,
      message4,
      message5,
      message6,
    ]);
  });

  it("supports reading topics that only occur in some chunks", async () => {
    const channel1: TypedMcapRecords["Channel"] = {
      type: "Channel",
      id: 1,
      schemaId: 0,
      topic: "a",
      messageEncoding: "utf12",
      metadata: new Map(),
    };
    const channel2: TypedMcapRecords["Channel"] = {
      type: "Channel",
      id: 2,
      schemaId: 0,
      topic: "b",
      messageEncoding: "utf13",
      metadata: new Map(),
    };

    const message1: TypedMcapRecords["Message"] = {
      type: "Message",
      channelId: channel1.id,
      sequence: 0,
      logTime: 0n,
      publishTime: 0n,
      data: new Uint8Array(),
    };
    const message2: TypedMcapRecords["Message"] = {
      type: "Message",
      channelId: channel2.id,
      sequence: 0,
      logTime: 1n,
      publishTime: 1n,
      data: new Uint8Array(),
    };

    const chunk1 = new ChunkBuilder({ useMessageIndex: true });
    chunk1.addChannel(channel1);
    chunk1.addMessage(message1);

    const chunk2 = new ChunkBuilder({ useMessageIndex: true });
    chunk2.addChannel(channel2);
    chunk2.addMessage(message2);

    const builder = new McapRecordBuilder();
    builder.writeMagic();
    builder.writeHeader({ profile: "", library: "" });

    const chunkIndexes: TypedMcapRecords["ChunkIndex"][] = [];
    chunkIndexes.push(writeChunkWithMessageIndexes(builder, chunk1));
    chunkIndexes.push(writeChunkWithMessageIndexes(builder, chunk2));

    builder.writeDataEnd({ dataSectionCrc: 0 });

    const summaryStart = BigInt(builder.length);
    builder.writeChannel(channel1);
    builder.writeChannel(channel2);
    for (const index of chunkIndexes) {
      builder.writeChunkIndex(index);
    }

    builder.writeFooter({ summaryStart, summaryOffsetStart: 0n, summaryCrc: 0 });
    builder.writeMagic();

    const reader = await McapIndexedReader.Initialize({ readable: makeReadable(builder.buffer) });
    await expect(collect(reader.readMessages({ topics: ["b"] }))).resolves.toEqual([message2]);
  });

  it("uses stable sort when loading overlapping chunks", async () => {
    const channel1: TypedMcapRecord = {
      type: "Channel",
      id: 1,
      schemaId: 0,
      topic: "a",
      messageEncoding: "utf12",
      metadata: new Map(),
    };
    const makeMessage = (idx: number): TypedMcapRecords["Message"] => {
      return {
        type: "Message",
        channelId: channel1.id,
        sequence: idx,
        logTime: BigInt(idx),
        publishTime: 0n,
        data: new Uint8Array(),
      };
    };

    const message3 = makeMessage(3);
    const message4 = makeMessage(4);
    const message5 = makeMessage(5);
    const message6 = makeMessage(6);

    /* Chunks have this layout:
      1: [3       6]
      2:   [4]
      3:      [5]
    */

    const chunk1 = new ChunkBuilder({ useMessageIndex: true });
    chunk1.addChannel(channel1);
    chunk1.addMessage(message3);
    chunk1.addMessage(message6);

    const chunk2 = new ChunkBuilder({ useMessageIndex: true });
    chunk2.addChannel(channel1);
    chunk2.addMessage(message4);

    const chunk3 = new ChunkBuilder({ useMessageIndex: true });
    chunk3.addChannel(channel1);
    chunk3.addMessage(message5);

    const builder = new McapRecordBuilder();
    builder.writeMagic();
    builder.writeHeader({ profile: "", library: "" });

    const chunkIndexes: TypedMcapRecords["ChunkIndex"][] = [];
    chunkIndexes.push(writeChunkWithMessageIndexes(builder, chunk1));
    chunkIndexes.push(writeChunkWithMessageIndexes(builder, chunk2));
    chunkIndexes.push(writeChunkWithMessageIndexes(builder, chunk3));

    builder.writeDataEnd({ dataSectionCrc: 0 });

    const summaryStart = BigInt(builder.length);

    for (const index of chunkIndexes) {
      builder.writeChunkIndex(index);
    }

    builder.writeFooter({ summaryStart, summaryOffsetStart: 0n, summaryCrc: 0 });
    builder.writeMagic();

    for (const reverse of [true, false]) {
      let expected = [message3, message4, message5, message6];
      if (reverse) {
        expected = expected.reverse();
      }

      const readable = makeReadable(builder.buffer);
      const reader = await McapIndexedReader.Initialize({ readable });
      expect(readable.readCalls).toEqual(4);

      const messageIter = reader.readMessages({ reverse });
      expect(readable.readCalls).toEqual(4);

      const collected = [];

      collected.push((await messageIter.next()).value);
      expect(readable.readCalls).toEqual(6);
      collected.push((await messageIter.next()).value);
      expect(readable.readCalls).toEqual(8);
      collected.push((await messageIter.next()).value);
      expect(readable.readCalls).toEqual(10);
      collected.push((await messageIter.next()).value);
      expect(readable.readCalls).toEqual(10);

      expect(collected).toEqual(expected);
    }
  });

  it("handles multiple messages at same timestamp", async () => {
    const tempBuffer = new TempBuffer();
    const writer = new McapWriter({ writable: tempBuffer });
    await writer.start({ library: "", profile: "" });
    const channelId1 = await writer.registerChannel({
      topic: "test1",
      schemaId: 0,
      messageEncoding: "",
      metadata: new Map(),
    });
    const message1: TypedMcapRecords["Message"] = {
      type: "Message",
      channelId: channelId1,
      sequence: 1,
      logTime: 0n,
      publishTime: 0n,
      data: new Uint8Array([1]),
    };
    const message2: TypedMcapRecords["Message"] = {
      type: "Message",
      channelId: channelId1,
      sequence: 2,
      logTime: 0n,
      publishTime: 0n,
      data: new Uint8Array([2]),
    };
    const message3: TypedMcapRecords["Message"] = {
      type: "Message",
      channelId: channelId1,
      sequence: 3,
      logTime: 0n,
      publishTime: 0n,
      data: new Uint8Array([3]),
    };
    await writer.addMessage(message1);
    await writer.addMessage(message2);
    await writer.addMessage(message3);
    await writer.end();

    for (const reverse of [true, false]) {
      let expected = [message1, message2, message3];
      if (reverse) {
        expected = expected.reverse();
      }

      const reader = await McapIndexedReader.Initialize({ readable: tempBuffer });

      expect(await collect(reader.readMessages({ reverse }))).toEqual(expected);
    }
  });

  it("ensure that chunks are loaded only when needed", async () => {
    const channelA: TypedMcapRecord = {
      type: "Channel",
      id: 1,
      schemaId: 0,
      topic: "a",
      messageEncoding: "utf12",
      metadata: new Map(),
    };
    const channelB: TypedMcapRecord = {
      type: "Channel",
      id: 2,
      schemaId: 0,
      topic: "b",
      messageEncoding: "utf12",
      metadata: new Map(),
    };
    const makeMessage = (channel: Channel, idx: number): TypedMcapRecords["Message"] => {
      return {
        type: "Message",
        channelId: channel.id,
        sequence: idx,
        logTime: BigInt(idx),
        publishTime: 0n,
        data: new Uint8Array(),
      };
    };

    const messageA1 = makeMessage(channelA, 1);
    const messageB1 = makeMessage(channelB, 1);
    const messageB2 = makeMessage(channelB, 2);

    const chunk1 = new ChunkBuilder({ useMessageIndex: true });
    chunk1.addChannel(channelA);
    chunk1.addChannel(channelB);
    chunk1.addMessage(messageA1);
    chunk1.addMessage(messageB2);

    const chunk2 = new ChunkBuilder({ useMessageIndex: true });
    chunk2.addChannel(channelB);
    chunk2.addMessage(messageB1);

    const builder = new McapRecordBuilder();
    builder.writeMagic();
    builder.writeHeader({ profile: "", library: "" });

    const chunkIndexes: TypedMcapRecords["ChunkIndex"][] = [];
    chunkIndexes.push(writeChunkWithMessageIndexes(builder, chunk1));
    chunkIndexes.push(writeChunkWithMessageIndexes(builder, chunk2));

    builder.writeDataEnd({ dataSectionCrc: 0 });

    const summaryStart = BigInt(builder.length);

    builder.writeChannel(channelA);
    builder.writeChannel(channelB);

    for (const index of chunkIndexes) {
      builder.writeChunkIndex(index);
    }

    builder.writeFooter({ summaryStart, summaryOffsetStart: 0n, summaryCrc: 0 });
    builder.writeMagic();

    {
      const readable = makeReadable(builder.buffer);
      const reader = await McapIndexedReader.Initialize({ readable });
      expect(readable.readCalls).toEqual(4);

      const messageIter = reader.readMessages({ topics: [channelB.topic] });
      expect(readable.readCalls).toEqual(4);

      const collected = [];

      collected.push((await messageIter.next()).value);
      expect(readable.readCalls).toEqual(7);
      collected.push((await messageIter.next()).value);
      expect(readable.readCalls).toEqual(8);

      expect(collected).toEqual([messageB1, messageB2]);
    }

    {
      const readable = makeReadable(builder.buffer);
      const reader = await McapIndexedReader.Initialize({ readable });
      expect(readable.readCalls).toEqual(4);

      const messageIter = reader.readMessages({ topics: [channelB.topic], reverse: true });
      expect(readable.readCalls).toEqual(4);

      const collected = [];

      collected.push((await messageIter.next()).value);
      expect(readable.readCalls).toEqual(6);
      collected.push((await messageIter.next()).value);
      expect(readable.readCalls).toEqual(8);

      expect(collected).toEqual([messageB1, messageB2].reverse());
    }
  });

  it("reads metadata records", async () => {
    const data = [
      ...MCAP_MAGIC,
      ...record(Opcode.HEADER, [
        ...string(""), // profile
        ...string(""), // library
      ]),
    ];
    const metadata1Start = data.length;
    data.push(
      ...record(Opcode.METADATA, [
        ...string("foo"), // name
        ...keyValues(string, string, [
          ["a", "1"],
          ["b", "2"],
        ]), // metadata
      ]),
    );
    const metadata2Start = data.length;
    data.push(
      ...record(Opcode.METADATA, [
        ...string("bar"), // name
        ...keyValues(string, string, [
          ["x", "10"],
          ["y", "20"],
        ]), // metadata
      ]),
    );
    const metadata3Start = data.length;
    data.push(
      ...record(Opcode.METADATA, [
        ...string("foo"), // name
        ...keyValues(string, string, [["b", "4"]]), // metadata
      ]),
    );
    const dataEndStart = data.length;
    data.push(
      ...record(Opcode.DATA_END, [
        ...uint32LE(0), // data crc
      ]),
    );
    const summaryStart = data.length;
    data.push(
      ...record(Opcode.METADATA_INDEX, [
        ...uint64LE(BigInt(metadata1Start)), // offset
        ...uint64LE(BigInt(metadata2Start - metadata1Start)), // length
        ...string("foo"), // name
      ]),
      ...record(Opcode.METADATA_INDEX, [
        ...uint64LE(BigInt(metadata2Start)), // offset
        ...uint64LE(BigInt(metadata3Start - metadata2Start)), // length
        ...string("bar"), // name
      ]),
      ...record(Opcode.METADATA_INDEX, [
        ...uint64LE(BigInt(metadata3Start)), // offset
        ...uint64LE(BigInt(dataEndStart - metadata3Start)), // length
        ...string("foo"), // name
      ]),
      ...record(Opcode.FOOTER, [
        ...uint64LE(BigInt(summaryStart)), // summary start
        ...uint64LE(0n), // summary offset start
        ...uint32LE(0), // summary crc
      ]),
      ...MCAP_MAGIC,
    );
    const readable = makeReadable(new Uint8Array(data));
    const reader = await McapIndexedReader.Initialize({ readable });

    expect(reader.metadataIndexes).toHaveLength(3);

    let metadata = await collect(reader.readMetadata());
    expect(metadata).toEqual([
      {
        name: "foo",
        metadata: new Map([
          ["a", "1"],
          ["b", "2"],
        ]),
        type: "Metadata",
      },
      {
        name: "bar",
        metadata: new Map([
          ["x", "10"],
          ["y", "20"],
        ]),
        type: "Metadata",
      },
      {
        name: "foo",
        metadata: new Map([["b", "4"]]),
        type: "Metadata",
      },
    ]);

    metadata = await collect(reader.readMetadata({ name: "bar" }));
    expect(metadata).toEqual([
      {
        name: "bar",
        metadata: new Map([
          ["x", "10"],
          ["y", "20"],
        ]),
        type: "Metadata",
      },
    ]);
  });

  it("reads attachment records", async () => {
    const data = [
      ...MCAP_MAGIC,
      ...record(Opcode.HEADER, [
        ...string(""), // profile
        ...string(""), // library
      ]),
    ];
    const attachment1Start = data.length;
    data.push(
      ...record(Opcode.ATTACHMENT, [
        ...uint64LE(1n), // log time
        ...uint64LE(2n), // create time
        ...string("foo"), // name
        ...string("text/plain"), // media type
        ...uint64LE(5n), // data length
        ...new Uint8Array([0x68, 0x65, 0x6c, 0x6c, 0x6f]), // data
        ...uint32LE(0), // data crc
      ]),
    );
    const attachment2Start = data.length;
    data.push(
      ...record(Opcode.ATTACHMENT, [
        ...uint64LE(4n), // log time
        ...uint64LE(5n), // create time
        ...string("bar"), // name
        ...string("application/octet-stream"), // media type
        ...uint64LE(3n), // data length
        ...new Uint8Array([1, 2, 3]), // data
        ...uint32LE(0), // data crc
      ]),
    );
    const attachment3Start = data.length;
    data.push(
      ...record(Opcode.ATTACHMENT, [
        ...uint64LE(6n), // log time
        ...uint64LE(7n), // create time
        ...string("foo"), // name
        ...string("application/json"), // media type
        ...uint64LE(2n), // data length
        ...new Uint8Array([0x7b, 0x7d]), // data
        ...uint32LE(0), // data crc
      ]),
    );
    const dataEndStart = data.length;
    data.push(
      ...record(Opcode.DATA_END, [
        ...uint32LE(0), // data crc
      ]),
    );
    const summaryStart = data.length;
    data.push(
      ...record(Opcode.ATTACHMENT_INDEX, [
        ...uint64LE(BigInt(attachment1Start)), // offset
        ...uint64LE(BigInt(attachment2Start - attachment1Start)), // length
        ...uint64LE(1n), // log time
        ...uint64LE(2n), // create time
        ...uint64LE(5n), // data size
        ...string("foo"), // name
        ...string("text/plain"), // media type
      ]),
      ...record(Opcode.ATTACHMENT_INDEX, [
        ...uint64LE(BigInt(attachment2Start)), // offset
        ...uint64LE(BigInt(attachment3Start - attachment2Start)), // length
        ...uint64LE(4n), // log time
        ...uint64LE(5n), // create time
        ...uint64LE(3n), // data size
        ...string("bar"), // name
        ...string("application/octet-stream"), // media type
      ]),
      ...record(Opcode.ATTACHMENT_INDEX, [
        ...uint64LE(BigInt(attachment3Start)), // offset
        ...uint64LE(BigInt(dataEndStart - attachment3Start)), // length
        ...uint64LE(6n), // log time
        ...uint64LE(7n), // create time
        ...uint64LE(2n), // data size
        ...string("foo"), // name
        ...string("application/json"), // media type
      ]),
      ...record(Opcode.FOOTER, [
        ...uint64LE(BigInt(summaryStart)), // summary start
        ...uint64LE(0n), // summary offset start
        ...uint32LE(0), // summary crc
      ]),
      ...MCAP_MAGIC,
    );
    const readable = makeReadable(new Uint8Array(data));
    const reader = await McapIndexedReader.Initialize({ readable });

    expect(reader.attachmentIndexes).toHaveLength(3);

    let attachments = await collect(reader.readAttachments());
    expect(attachments).toEqual([
      {
        name: "foo",
        logTime: 1n,
        createTime: 2n,
        mediaType: "text/plain",
        data: new Uint8Array([0x68, 0x65, 0x6c, 0x6c, 0x6f]),
        type: "Attachment",
      },
      {
        name: "bar",
        logTime: 4n,
        createTime: 5n,
        mediaType: "application/octet-stream",
        data: new Uint8Array([1, 2, 3]),
        type: "Attachment",
      },
      {
        name: "foo",
        logTime: 6n,
        createTime: 7n,
        mediaType: "application/json",
        data: new Uint8Array([0x7b, 0x7d]),
        type: "Attachment",
      },
    ]);

    attachments = await collect(reader.readAttachments({ name: "bar" }));
    expect(attachments).toEqual([
      {
        name: "bar",
        logTime: 4n,
        createTime: 5n,
        mediaType: "application/octet-stream",
        data: new Uint8Array([1, 2, 3]),
        type: "Attachment",
      },
    ]);

    attachments = await collect(reader.readAttachments({ mediaType: "application/json" }));
    expect(attachments).toEqual([
      {
        name: "foo",
        logTime: 6n,
        createTime: 7n,
        mediaType: "application/json",
        data: new Uint8Array([0x7b, 0x7d]),
        type: "Attachment",
      },
    ]);

    attachments = await collect(reader.readAttachments({ startTime: 3n, endTime: 5n }));
    expect(attachments).toEqual([
      {
        name: "bar",
        logTime: 4n,
        createTime: 5n,
        mediaType: "application/octet-stream",
        data: new Uint8Array([1, 2, 3]),
        type: "Attachment",
      },
    ]);
  });

  it("supports chunk index where message index is empty", async () => {
    const channel1: TypedMcapRecord = {
      type: "Channel",
      id: 1,
      schemaId: 0,
      topic: "a",
      messageEncoding: "utf12",
      metadata: new Map(),
    };
    const makeMessage = (idx: number): TypedMcapRecords["Message"] => {
      return {
        type: "Message",
        channelId: channel1.id,
        sequence: idx,
        logTime: BigInt(idx),
        publishTime: 0n,
        data: new Uint8Array(),
      };
    };
    const message1 = makeMessage(1);
    const message2 = makeMessage(2);

    const chunk1 = new ChunkBuilder({ useMessageIndex: true });
    chunk1.addChannel(channel1);
    chunk1.addMessage(message1);

    const emptyChunk = new ChunkBuilder({ useMessageIndex: true });

    const chunk2 = new ChunkBuilder({ useMessageIndex: true });
    chunk2.addChannel(channel1);
    chunk2.addMessage(message2);

    const builder = new McapRecordBuilder();
    builder.writeMagic();
    builder.writeHeader({ profile: "", library: "" });

    const chunkIndexes: TypedMcapRecords["ChunkIndex"][] = [];
    chunkIndexes.push(writeChunkWithMessageIndexes(builder, chunk1));
    chunkIndexes.push(writeChunkWithMessageIndexes(builder, emptyChunk));
    chunkIndexes.push(writeChunkWithMessageIndexes(builder, chunk2));

    builder.writeDataEnd({ dataSectionCrc: 0 });

    const summaryStart = BigInt(builder.length);

    for (const index of chunkIndexes) {
      builder.writeChunkIndex(index);
    }

    builder.writeFooter({ summaryStart, summaryOffsetStart: 0n, summaryCrc: 0 });
    builder.writeMagic();

    const reader = await McapIndexedReader.Initialize({ readable: makeReadable(builder.buffer) });
    await expect(collect(reader.readMessages())).resolves.toEqual([message1, message2]);
  });
});
