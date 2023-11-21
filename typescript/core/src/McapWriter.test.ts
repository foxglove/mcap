import { crc32 } from "@foxglove/crc";

import { McapIndexedReader } from "./McapIndexedReader";
import McapStreamReader from "./McapStreamReader";
import { McapWriter } from "./McapWriter";
import { TempBuffer } from "./TempBuffer";
import { Opcode } from "./constants";
import { parseMagic, parseRecord } from "./parse";
import { collect, keyValues, record, string, uint16LE, uint32LE, uint64LE } from "./testUtils";
import { TypedMcapRecord } from "./types";

describe("McapWriter", () => {
  it("supports messages with logTime 0", async () => {
    const tempBuffer = new TempBuffer();
    const writer = new McapWriter({ writable: tempBuffer });

    await writer.start({ library: "", profile: "" });
    const channelId = await writer.registerChannel({
      topic: "test",
      schemaId: 0,
      messageEncoding: "json",
      metadata: new Map(),
    });
    await writer.addMessage({
      channelId,
      data: new Uint8Array(),
      sequence: 0,
      logTime: 0n,
      publishTime: 0n,
    });
    await writer.addMessage({
      channelId,
      data: new Uint8Array(),
      sequence: 1,
      logTime: 1n,
      publishTime: 1n,
    });
    await writer.end();

    const reader = await McapIndexedReader.Initialize({ readable: tempBuffer });

    expect(reader.chunkIndexes).toMatchObject([{ messageStartTime: 0n, messageEndTime: 1n }]);

    await expect(collect(reader.readMessages())).resolves.toEqual([
      {
        type: "Message",
        channelId,
        data: new Uint8Array(),
        sequence: 0,
        logTime: 0n,
        publishTime: 0n,
      },
      {
        type: "Message",
        channelId,
        data: new Uint8Array(),
        sequence: 1,
        logTime: 1n,
        publishTime: 1n,
      },
    ]);
    await expect(collect(reader.readMessages({ endTime: 0n }))).resolves.toEqual([
      {
        type: "Message",
        channelId,
        data: new Uint8Array(),
        sequence: 0,
        logTime: 0n,
        publishTime: 0n,
      },
    ]);
    await expect(collect(reader.readMessages({ startTime: 1n }))).resolves.toEqual([
      {
        type: "Message",
        channelId,
        data: new Uint8Array(),
        sequence: 1,
        logTime: 1n,
        publishTime: 1n,
      },
    ]);
  });

  it("supports multiple chunks", async () => {
    const tempBuffer = new TempBuffer();
    const writer = new McapWriter({ writable: tempBuffer, chunkSize: 0 });

    await writer.start({ library: "", profile: "" });
    const channelId = await writer.registerChannel({
      topic: "test",
      schemaId: 0,
      messageEncoding: "json",
      metadata: new Map(),
    });
    await writer.addMessage({
      channelId,
      data: new Uint8Array(),
      sequence: 0,
      logTime: 0n,
      publishTime: 0n,
    });
    await writer.addMessage({
      channelId,
      data: new Uint8Array(),
      sequence: 1,
      logTime: 1n,
      publishTime: 1n,
    });
    await writer.end();

    const reader = new McapStreamReader();
    reader.append(tempBuffer.get());
    const records: TypedMcapRecord[] = [];
    for (let rec; (rec = reader.nextRecord()); ) {
      records.push(rec);
    }

    expect(records).toEqual<TypedMcapRecord[]>([
      {
        type: "Header",
        library: "",
        profile: "",
      },
      {
        type: "Channel",
        id: 0,
        messageEncoding: "json",
        metadata: new Map(),
        schemaId: 0,
        topic: "test",
      },
      {
        type: "Message",
        channelId: 0,
        data: new Uint8Array(),
        logTime: 0n,
        publishTime: 0n,
        sequence: 0,
      },
      {
        type: "MessageIndex",
        channelId: 0,
        records: [[0n, 33n]],
      },
      {
        type: "Message",
        channelId: 0,
        data: new Uint8Array(),
        logTime: 1n,
        publishTime: 1n,
        sequence: 1,
      },
      {
        type: "MessageIndex",
        channelId: 0,
        records: [[1n, 0n]],
      },
      {
        type: "DataEnd",
        dataSectionCrc: 4132032003,
      },
      {
        type: "Channel",
        id: 0,
        messageEncoding: "json",
        metadata: new Map(),
        schemaId: 0,
        topic: "test",
      },
      {
        type: "Statistics",
        attachmentCount: 0,
        channelCount: 1,
        channelMessageCounts: new Map([[0, 2n]]),
        chunkCount: 2,
        messageCount: 2n,
        messageEndTime: 1n,
        messageStartTime: 0n,
        metadataCount: 0,
        schemaCount: 0,
      },
      {
        type: "ChunkIndex",
        chunkLength: 113n,
        chunkStartOffset: 25n,
        compressedSize: 64n,
        compression: "",
        messageEndTime: 0n,
        messageIndexLength: 31n,
        messageIndexOffsets: new Map([[0, 138n]]),
        messageStartTime: 0n,
        uncompressedSize: 64n,
      },
      {
        type: "ChunkIndex",
        chunkLength: 80n,
        chunkStartOffset: 169n,
        compressedSize: 31n,
        compression: "",
        messageEndTime: 1n,
        messageIndexLength: 31n,
        messageIndexOffsets: new Map([[0, 249n]]),
        messageStartTime: 1n,
        uncompressedSize: 31n,
      },
      {
        type: "SummaryOffset",
        groupLength: 33n,
        groupOpcode: Opcode.CHANNEL,
        groupStart: 293n,
      },
      {
        type: "SummaryOffset",
        groupLength: 65n,
        groupOpcode: Opcode.STATISTICS,
        groupStart: 326n,
      },
      {
        type: "SummaryOffset",
        groupLength: 166n,
        groupOpcode: Opcode.CHUNK_INDEX,
        groupStart: 391n,
      },
      {
        type: "Footer",
        summaryCrc: 3779440972,
        summaryOffsetStart: 557n,
        summaryStart: 293n,
      },
    ]);
  });

  it("supports chunk compression", async () => {
    function reverse(data: Uint8Array): Uint8Array {
      return Uint8Array.from(data, (_, i) => data[data.byteLength - 1 - i]!);
    }
    function reverseDouble(data: Uint8Array): Uint8Array {
      return new Uint8Array([...reverse(data), ...reverse(data)]);
    }

    const tempBuffer = new TempBuffer();
    const writer = new McapWriter({
      writable: tempBuffer,
      useStatistics: false,
      useSummaryOffsets: false,
      compressChunk: (data) => ({
        compression: "reverse double",
        compressedData: reverseDouble(data),
      }),
    });

    await writer.start({ library: "", profile: "" });
    const channelId = await writer.registerChannel({
      topic: "test",
      schemaId: 0,
      messageEncoding: "json",
      metadata: new Map(),
    });
    await writer.addMessage({
      channelId,
      data: new Uint8Array(),
      sequence: 0,
      logTime: 0n,
      publishTime: 0n,
    });
    await writer.end();

    const array = tempBuffer.get();
    const view = new DataView(array.buffer, array.byteOffset, array.byteLength);
    const records: TypedMcapRecord[] = [];
    for (
      let offset = parseMagic(view, 0).usedBytes, result;
      (result = parseRecord({ view, startOffset: offset, validateCrcs: true })), result.record;
      offset += result.usedBytes
    ) {
      records.push(result.record);
    }

    const expectedChunkData = new Uint8Array([
      ...record(Opcode.CHANNEL, [
        ...uint16LE(channelId), // channel id
        ...uint16LE(0), // schema id
        ...string("test"), // topic
        ...string("json"), // message encoding
        ...keyValues(string, string, []), // metadata
      ]),
      ...record(Opcode.MESSAGE, [
        ...uint16LE(channelId), // channel id
        ...uint32LE(0), // sequence
        ...uint64LE(0n), // log time
        ...uint64LE(0n), // publish time
      ]),
    ]);

    expect(records).toEqual<TypedMcapRecord[]>([
      {
        type: "Header",
        library: "",
        profile: "",
      },
      {
        type: "Chunk",
        compression: "reverse double",
        messageStartTime: 0n,
        messageEndTime: 0n,
        uncompressedCrc: crc32(expectedChunkData),
        uncompressedSize: BigInt(expectedChunkData.byteLength),
        records: reverseDouble(expectedChunkData),
      },
      {
        type: "MessageIndex",
        channelId: 0,
        records: [[0n, 33n]],
      },
      {
        type: "DataEnd",
        dataSectionCrc: 475180730,
      },
      {
        type: "Channel",
        id: 0,
        messageEncoding: "json",
        metadata: new Map(),
        schemaId: 0,
        topic: "test",
      },
      {
        type: "ChunkIndex",
        chunkLength: expect.any(BigInt) as bigint,
        chunkStartOffset: 25n,
        compressedSize: BigInt(2 * expectedChunkData.byteLength),
        compression: "reverse double",
        messageEndTime: 0n,
        messageIndexLength: 31n,
        messageIndexOffsets: new Map([[0, expect.any(BigInt) as bigint]]),
        messageStartTime: 0n,
        uncompressedSize: BigInt(expectedChunkData.byteLength),
      },
      {
        type: "Footer",
        summaryCrc: expect.any(Number) as number,
        summaryOffsetStart: expect.any(BigInt) as bigint,
        summaryStart: expect.any(BigInt) as bigint,
      },
    ]);
  });

  it("supports append mode", async () => {
    const tempBuffer = new TempBuffer();

    const writer = new McapWriter({ writable: tempBuffer, chunkSize: 0 });

    await writer.start({ library: "", profile: "" });
    const schemaId = await writer.registerSchema({
      name: "test1",
      encoding: "json",
      data: new Uint8Array(),
    });
    const channelId = await writer.registerChannel({
      topic: "test",
      schemaId,
      messageEncoding: "json",
      metadata: new Map(),
    });
    await writer.addMessage({
      channelId,
      data: new Uint8Array(),
      sequence: 0,
      logTime: 0n,
      publishTime: 0n,
    });
    await writer.end();

    const writerAppendMode = await McapWriter.InitializeInAppendMode(tempBuffer);

    await writerAppendMode.addAttachment({
      name: "attachment test",
      logTime: 0n,
      createTime: 0n,
      mediaType: "application/json",
      data: new TextEncoder().encode(`{"test": "testValue"}`),
    });
    await writerAppendMode.addMetadata({
      name: "metadata test",
      metadata: new Map<string, string>([["test", "testValue"]]),
    });
    await writerAppendMode.addMessage({
      channelId,
      data: new Uint8Array(),
      sequence: 1,
      logTime: 1n,
      publishTime: 1n,
    });
    const newChannelId = await writerAppendMode.registerChannel({
      topic: "test/append",
      schemaId,
      messageEncoding: "json",
      metadata: new Map(),
    });
    await writerAppendMode.addMessage({
      channelId: newChannelId,
      data: new Uint8Array(),
      sequence: 2,
      logTime: 2n,
      publishTime: 2n,
    });
    await writerAppendMode.end();

    const reader = new McapStreamReader();
    reader.append(tempBuffer.get());
    const records: TypedMcapRecord[] = [];
    for (let rec; (rec = reader.nextRecord()); ) {
      records.push(rec);
    }

    expect(reader.done()).toEqual(true);

    expect(records).toEqual<TypedMcapRecord[]>([
      {
        type: "Header",
        library: "",
        profile: "",
      },
      {
        type: "Schema",
        id: 1,
        encoding: "json",
        data: new Uint8Array(),
        name: "test1",
      },
      {
        type: "Channel",
        id: 0,
        messageEncoding: "json",
        metadata: new Map(),
        schemaId: 1,
        topic: "test",
      },
      {
        type: "Message",
        channelId: 0,
        data: new Uint8Array(),
        logTime: 0n,
        publishTime: 0n,
        sequence: 0,
      },
      {
        type: "MessageIndex",
        channelId: 0,
        records: [[0n, 65n]],
      },
      {
        type: "Attachment",
        name: "attachment test",
        logTime: 0n,
        createTime: 0n,
        mediaType: "application/json",
        data: new TextEncoder().encode(`{"test": "testValue"}`),
      },
      {
        type: "Metadata",
        name: "metadata test",
        metadata: new Map([["test", "testValue"]]),
      },
      {
        type: "Schema",
        id: 1,
        encoding: "json",
        data: new Uint8Array(),
        name: "test1",
      },
      {
        type: "Channel",
        id: 0,
        messageEncoding: "json",
        metadata: new Map(),
        schemaId: 1,
        topic: "test",
      },
      {
        type: "Message",
        channelId: 0,
        data: new Uint8Array(),
        logTime: 1n,
        publishTime: 1n,
        sequence: 1,
      },
      {
        type: "Channel",
        id: 1,
        messageEncoding: "json",
        metadata: new Map(),
        schemaId: 1,
        topic: "test/append",
      },
      {
        type: "Message",
        channelId: 1,
        data: new Uint8Array(),
        logTime: 2n,
        publishTime: 2n,
        sequence: 2,
      },
      {
        type: "MessageIndex",
        channelId: 0,
        records: [[1n, 65n]],
      },
      {
        type: "MessageIndex",
        channelId: 1,
        records: [[2n, 136n]],
      },
      {
        type: "DataEnd",
        dataSectionCrc: 1361490560,
      },
      {
        type: "Schema",
        id: 1,
        encoding: "json",
        data: new Uint8Array(),
        name: "test1",
      },
      {
        type: "Channel",
        id: 0,
        messageEncoding: "json",
        metadata: new Map(),
        schemaId: 1,
        topic: "test",
      },
      {
        type: "Channel",
        id: 1,
        messageEncoding: "json",
        metadata: new Map(),
        schemaId: 1,
        topic: "test/append",
      },
      {
        type: "Statistics",
        attachmentCount: 1,
        channelCount: 2,
        channelMessageCounts: new Map([
          [0, 2n],
          [1, 1n],
        ]),
        chunkCount: 2,
        messageCount: 3n,
        messageEndTime: 2n,
        messageStartTime: 0n,
        metadataCount: 1,
        schemaCount: 1,
      },
      {
        type: "MetadataIndex",
        offset: 298n,
        length: 51n,
        name: "metadata test",
      },
      {
        type: "AttachmentIndex",
        offset: 201n,
        length: 97n,
        logTime: 0n,
        createTime: 0n,
        dataSize: 21n,
        name: "attachment test",
        mediaType: "application/json",
      },
      {
        type: "ChunkIndex",
        chunkLength: 145n,
        chunkStartOffset: 25n,
        compressedSize: 96n,
        compression: "",
        messageEndTime: 0n,
        messageIndexLength: 31n,
        messageIndexOffsets: new Map([[0, 170n]]),
        messageStartTime: 0n,
        uncompressedSize: 96n,
      },
      {
        type: "ChunkIndex",
        chunkLength: 216n,
        chunkStartOffset: 349n,
        compressedSize: 167n,
        compression: "",
        messageEndTime: 2n,
        messageIndexLength: 62n,
        messageIndexOffsets: new Map([
          [0, 565n],
          [1, 596n],
        ]),
        messageStartTime: 1n,
        uncompressedSize: 167n,
      },
      {
        type: "SummaryOffset",
        groupLength: 32n,
        groupOpcode: Opcode.SCHEMA,
        groupStart: 640n,
      },
      {
        type: "SummaryOffset",
        groupLength: 73n,
        groupOpcode: Opcode.CHANNEL,
        groupStart: 672n,
      },
      {
        type: "SummaryOffset",
        groupLength: 75n,
        groupOpcode: Opcode.STATISTICS,
        groupStart: 745n,
      },
      {
        type: "SummaryOffset",
        groupLength: 42n,
        groupOpcode: Opcode.METADATA_INDEX,
        groupStart: 820n,
      },
      {
        type: "SummaryOffset",
        groupLength: 88n,
        groupOpcode: Opcode.ATTACHMENT_INDEX,
        groupStart: 862n,
      },
      {
        type: "SummaryOffset",
        groupLength: 176n,
        groupOpcode: Opcode.CHUNK_INDEX,
        groupStart: 950n,
      },
      {
        type: "Footer",
        summaryCrc: 1244986820,
        summaryOffsetStart: 1126n,
        summaryStart: 640n,
      },
    ]);

    // Confirm that the appended file is still readable
    const readAppendedFile = new McapStreamReader();
    readAppendedFile.append(tempBuffer.get());
    const appendedRecords: TypedMcapRecord[] = [];
    for (let rec; (rec = readAppendedFile.nextRecord()); ) {
      appendedRecords.push(rec);
    }

    expect(readAppendedFile.done()).toEqual(true);
  });
});
