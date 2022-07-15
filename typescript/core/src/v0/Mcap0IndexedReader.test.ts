import { crc32 } from "@foxglove/crc";

import { ChunkBuilder } from "./ChunkBuilder";
import { Mcap0IndexedReader } from "./Mcap0IndexedReader";
import { Mcap0RecordBuilder } from "./Mcap0RecordBuilder";
import { Mcap0Writer } from "./Mcap0Writer";
import { TempBuffer } from "./TempBuffer";
import { MCAP0_MAGIC, Opcode } from "./constants";
import {
  record,
  uint64LE,
  uint32LE,
  string,
  keyValues,
  collect,
  uint16LE,
  uint32PrefixedBytes,
} from "./testUtils";
import { Channel, TypedMcapRecord, TypedMcapRecords } from "./types";

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

describe("Mcap0IndexedReader", () => {
  it("rejects files that are too small", async () => {
    await expect(
      Mcap0IndexedReader.Initialize({
        readable: makeReadable(
          new Uint8Array([
            ...MCAP0_MAGIC,
            ...record(Opcode.FOOTER, [
              ...uint64LE(0n), // summary offset
              ...uint64LE(0n), // summary start offset
              ...uint32LE(0), // summary crc
            ]),
            ...MCAP0_MAGIC,
          ]),
        ),
      }),
    ).rejects.toThrow("Unable to read header at beginning of file; found Footer");

    await expect(
      Mcap0IndexedReader.Initialize({
        readable: makeReadable(
          new Uint8Array([
            ...MCAP0_MAGIC,
            ...record(Opcode.HEADER, [
              ...string(""), // profile
              ...string(""), // library
            ]),
            ...MCAP0_MAGIC,
          ]),
        ),
      }),
    ).rejects.toThrow("too small to be valid MCAP");
  });

  it("rejects unindexed file", async () => {
    const readable = makeReadable(
      new Uint8Array([
        ...MCAP0_MAGIC,
        ...record(Opcode.HEADER, [
          ...string(""), // profile
          ...string(""), // library
        ]),
        ...record(Opcode.FOOTER, [
          ...uint64LE(0n), // summary offset
          ...uint64LE(0n), // summary start offset
          ...uint32LE(0), // summary crc
        ]),
        ...MCAP0_MAGIC,
      ]),
    );
    await expect(Mcap0IndexedReader.Initialize({ readable })).rejects.toThrow(
      "File is not indexed",
    );
  });

  it("includes library in error messages", async () => {
    const readable = makeReadable(
      new Uint8Array([
        ...MCAP0_MAGIC,
        ...record(Opcode.HEADER, [
          ...string(""), // profile
          ...string("lib"), // library
        ]),
        ...record(Opcode.FOOTER, [
          ...uint64LE(0n), // summary offset
          ...uint64LE(0n), // summary start offset
          ...uint32LE(0), // summary crc
        ]),
        ...MCAP0_MAGIC,
      ]),
    );
    await expect(Mcap0IndexedReader.Initialize({ readable })).rejects.toThrow(
      "File is not indexed [library=lib]",
    );
  });

  it("rejects invalid index crc", async () => {
    const data = [
      ...MCAP0_MAGIC,
      ...record(Opcode.HEADER, [
        ...string(""), // profile
        ...string(""), // library
      ]),
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
        ...uint64LE(BigInt(summaryStart)), // summary offset
        ...uint64LE(0n), // summary start offset
        ...uint32LE(crc32(new Uint8Array([42]))), // summary crc
      ]),
      ...MCAP0_MAGIC,
    );
    const readable = makeReadable(new Uint8Array(data));
    await expect(Mcap0IndexedReader.Initialize({ readable })).rejects.toThrow(
      "Incorrect summary CRC 491514153 (expected 163128923)",
    );
  });

  it("parses index with schema and channel", async () => {
    const data = [
      ...MCAP0_MAGIC,
      ...record(Opcode.HEADER, [
        ...string(""), // profile
        ...string(""), // library
      ]),
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
        ...uint64LE(BigInt(summaryStart)), // summary offset
        ...uint64LE(0n), // summary start offset
        ...uint32LE(crc32(new Uint8Array(0))), // summary crc
      ]),
      ...MCAP0_MAGIC,
    );
    const readable = makeReadable(new Uint8Array(data));
    const reader = await Mcap0IndexedReader.Initialize({ readable });
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
          ...MCAP0_MAGIC,
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
            ...uint64LE(BigInt(summaryStart)), // summary offset
            ...uint64LE(0n), // summary start offset
            ...uint32LE(crc32(new Uint8Array(0))), // summary crc
          ]),
          ...MCAP0_MAGIC,
        );

        {
          const readable = makeReadable(new Uint8Array(data));
          const reader = await Mcap0IndexedReader.Initialize({ readable });
          const collected = await collect(reader.readMessages({ startTime, endTime }));
          expect(collected).toEqual(expected);
          expect(readable.readCalls).toBe(6);
        }

        {
          const readable = makeReadable(new Uint8Array(data));
          const reader = await Mcap0IndexedReader.Initialize({ readable });
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
        const writer = new Mcap0Writer({ writable: tempBuffer });
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
          const reader = await Mcap0IndexedReader.Initialize({ readable: tempBuffer });
          const collected = await collect(reader.readMessages({ startTime, endTime }));
          expect(collected).toEqual(
            expectedIndices.map((i) => ({ channelId: channelIds[i]!, ...messages[i]! })),
          );
        }

        {
          const reader = await Mcap0IndexedReader.Initialize({ readable: tempBuffer });
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

    const builder = new Mcap0RecordBuilder();
    builder.writeMagic();
    builder.writeHeader({ profile: "", library: "" });

    const chunkIndexes: TypedMcapRecords["ChunkIndex"][] = [];
    function writeChunk(chunk: ChunkBuilder) {
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
        chunkMessageIndexOffsets.set(
          messageIndex.channelId,
          messageIndexStart + messageIndexLength,
        );
        messageIndexLength += builder.writeMessageIndex(messageIndex);
      }

      chunkIndexes.push({
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
      });
    }

    writeChunk(chunk1);
    writeChunk(chunk2);
    writeChunk(chunk3);
    writeChunk(chunk4);

    builder.writeDataEnd({ dataSectionCrc: 0 });

    const summaryStart = BigInt(builder.length);

    for (const index of chunkIndexes) {
      builder.writeChunkIndex(index);
    }

    builder.writeFooter({ summaryStart, summaryOffsetStart: 0n, summaryCrc: 0 });
    builder.writeMagic();

    const reader = await Mcap0IndexedReader.Initialize({ readable: makeReadable(builder.buffer) });
    await expect(collect(reader.readMessages())).resolves.toEqual([
      message1,
      message2,
      message3,
      message4,
      message5,
      message6,
    ]);
  });

  it("correctly reads overlapping chunks", async () => {
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

    const builder = new Mcap0RecordBuilder();
    builder.writeMagic();
    builder.writeHeader({ profile: "", library: "" });

    const chunkIndexes: TypedMcapRecords["ChunkIndex"][] = [];
    function writeChunk(chunk: ChunkBuilder) {
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
        chunkMessageIndexOffsets.set(
          messageIndex.channelId,
          messageIndexStart + messageIndexLength,
        );
        messageIndexLength += builder.writeMessageIndex(messageIndex);
      }

      chunkIndexes.push({
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
      });
    }

    writeChunk(chunk1);
    writeChunk(chunk2);
    writeChunk(chunk3);

    builder.writeDataEnd({ dataSectionCrc: 0 });

    const summaryStart = BigInt(builder.length);

    for (const index of chunkIndexes) {
      builder.writeChunkIndex(index);
    }

    builder.writeFooter({ summaryStart, summaryOffsetStart: 0n, summaryCrc: 0 });
    builder.writeMagic();

    {
      const readable = makeReadable(builder.buffer);
      const reader = await Mcap0IndexedReader.Initialize({ readable });
      expect(readable.readCalls).toEqual(4);

      const messageIter = reader.readMessages();
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

      expect(collected).toEqual([message3, message4, message5, message6]);
    }

    {
      const readable = makeReadable(builder.buffer);
      const reader = await Mcap0IndexedReader.Initialize({ readable });
      expect(readable.readCalls).toEqual(4);

      const messageIter = reader.readMessages({ reverse: true });
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

      expect(collected).toEqual([message3, message4, message5, message6].reverse());
    }
  });

  it.only("ensure that chunks are loaded only when needed", async () => {
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

    const builder = new Mcap0RecordBuilder();
    builder.writeMagic();
    builder.writeHeader({ profile: "", library: "" });

    const chunkIndexes: TypedMcapRecords["ChunkIndex"][] = [];
    function writeChunk(chunk: ChunkBuilder) {
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
        chunkMessageIndexOffsets.set(
          messageIndex.channelId,
          messageIndexStart + messageIndexLength,
        );
        messageIndexLength += builder.writeMessageIndex(messageIndex);
      }

      chunkIndexes.push({
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
      });
    }

    writeChunk(chunk1);
    writeChunk(chunk2);

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
      const reader = await Mcap0IndexedReader.Initialize({ readable });
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
      const reader = await Mcap0IndexedReader.Initialize({ readable });
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
});
