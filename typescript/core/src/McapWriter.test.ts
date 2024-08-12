import { crc32 } from "@foxglove/crc";

import { McapIndexedReader } from "./McapIndexedReader";
import McapStreamReader from "./McapStreamReader";
import { McapWriter } from "./McapWriter";
import Reader from "./Reader";
import { TempBuffer } from "./TempBuffer";
import { MCAP_MAGIC, Opcode } from "./constants";
import { parseMagic, parseRecord } from "./parse";
import { collect, keyValues, record, string, uint16LE, uint32LE, uint64LE } from "./testUtils";
import { TypedMcapRecord } from "./types";

function readAsMcapStream(data: Uint8Array) {
  const reader = new McapStreamReader();
  reader.append(data);
  const records: TypedMcapRecord[] = [];
  for (let rec; (rec = reader.nextRecord()); ) {
    records.push(rec);
  }
  expect(reader.done()).toBe(true);
  return records;
}

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
      logTime: 0,
      publishTime: 0,
    });
    await writer.addMessage({
      channelId,
      data: new Uint8Array(),
      sequence: 1,
      logTime: 1,
      publishTime: 1,
    });
    await writer.end();

    const reader = await McapIndexedReader.Initialize({ readable: tempBuffer });

    expect(reader.chunkIndexes).toMatchObject([{ messageStartTime: 0, messageEndTime: 1 }]);

    await expect(collect(reader.readMessages())).resolves.toEqual([
      {
        type: "Message",
        channelId,
        data: new Uint8Array(),
        sequence: 0,
        logTime: 0,
        publishTime: 0,
      },
      {
        type: "Message",
        channelId,
        data: new Uint8Array(),
        sequence: 1,
        logTime: 1,
        publishTime: 1,
      },
    ]);
    await expect(collect(reader.readMessages({ endTime: 0 }))).resolves.toEqual([
      {
        type: "Message",
        channelId,
        data: new Uint8Array(),
        sequence: 0,
        logTime: 0,
        publishTime: 0,
      },
    ]);
    await expect(collect(reader.readMessages({ startTime: 1 }))).resolves.toEqual([
      {
        type: "Message",
        channelId,
        data: new Uint8Array(),
        sequence: 1,
        logTime: 1,
        publishTime: 1,
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
      logTime: 0,
      publishTime: 0,
    });
    await writer.addMessage({
      channelId,
      data: new Uint8Array(),
      sequence: 1,
      logTime: 1,
      publishTime: 1,
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
        logTime: 0,
        publishTime: 0,
        sequence: 0,
      },
      {
        type: "MessageIndex",
        channelId: 0,
        records: [[0, 33]],
      },
      {
        type: "Message",
        channelId: 0,
        data: new Uint8Array(),
        logTime: 1,
        publishTime: 1,
        sequence: 1,
      },
      {
        type: "MessageIndex",
        channelId: 0,
        records: [[1, 0]],
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
        channelMessageCounts: new Map([[0, 2]]),
        chunkCount: 2,
        messageCount: 2,
        messageEndTime: 1,
        messageStartTime: 0,
        metadataCount: 0,
        schemaCount: 0,
      },
      {
        type: "ChunkIndex",
        chunkLength: 113,
        chunkStartOffset: 25,
        compressedSize: 64,
        compression: "",
        messageEndTime: 0,
        messageIndexLength: 31,
        messageIndexOffsets: new Map([[0, 138]]),
        messageStartTime: 0,
        uncompressedSize: 64,
      },
      {
        type: "ChunkIndex",
        chunkLength: 80,
        chunkStartOffset: 169,
        compressedSize: 31,
        compression: "",
        messageEndTime: 1,
        messageIndexLength: 31,
        messageIndexOffsets: new Map([[0, 249]]),
        messageStartTime: 1,
        uncompressedSize: 31,
      },
      {
        type: "SummaryOffset",
        groupLength: 33,
        groupOpcode: Opcode.CHANNEL,
        groupStart: 293,
      },
      {
        type: "SummaryOffset",
        groupLength: 65,
        groupOpcode: Opcode.STATISTICS,
        groupStart: 326,
      },
      {
        type: "SummaryOffset",
        groupLength: 166,
        groupOpcode: Opcode.CHUNK_INDEX,
        groupStart: 391,
      },
      {
        type: "Footer",
        summaryCrc: 3779440972,
        summaryOffsetStart: 557,
        summaryStart: 293,
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
      logTime: 0,
      publishTime: 0,
    });
    await writer.end();

    const array = tempBuffer.get();
    const view = new DataView(array.buffer, array.byteOffset, array.byteLength);
    const reader = new Reader(view);
    const records: TypedMcapRecord[] = [];
    parseMagic(reader);
    let result;
    while ((result = parseRecord(reader, true))) {
      records.push(result);
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
        ...uint64LE(0), // log time
        ...uint64LE(0), // publish time
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
        messageStartTime: 0,
        messageEndTime: 0,
        uncompressedCrc: crc32(expectedChunkData),
        uncompressedSize: expectedChunkData.byteLength,
        records: reverseDouble(expectedChunkData),
      },
      {
        type: "MessageIndex",
        channelId: 0,
        records: [[0, 33]],
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
        chunkLength: expect.any(Number) as number,
        chunkStartOffset: 25,
        compressedSize: 2 * expectedChunkData.byteLength,
        compression: "reverse double",
        messageEndTime: 0,
        messageIndexLength: 31,
        messageIndexOffsets: new Map([[0, expect.any(Number) as number]]),
        messageStartTime: 0,
        uncompressedSize: expectedChunkData.byteLength,
      },
      {
        type: "Footer",
        summaryCrc: expect.any(Number) as number,
        summaryOffsetStart: expect.any(Number) as number,
        summaryStart: expect.any(Number) as number,
      },
    ]);
  });

  it("supports append mode", async () => {
    const tempBuffer = new TempBuffer();

    const writer = new McapWriter({ writable: tempBuffer });

    await writer.start({ library: "", profile: "" });
    const schemaId = await writer.registerSchema({
      name: "schema1",
      encoding: "json",
      data: new Uint8Array(),
    });
    const channelId1 = await writer.registerChannel({
      topic: "channel1",
      schemaId,
      messageEncoding: "json",
      metadata: new Map(),
    });
    await writer.addMessage({
      channelId: channelId1,
      data: new Uint8Array(),
      sequence: 0,
      logTime: 0,
      publishTime: 0,
    });
    await writer.end();

    // Records common to both the original and appended file
    const commonRecords: TypedMcapRecord[] = [
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
        name: "schema1",
      },
      {
        type: "Channel",
        id: 0,
        messageEncoding: "json",
        metadata: new Map(),
        schemaId: 1,
        topic: "channel1",
      },
      {
        type: "Message",
        channelId: 0,
        data: new Uint8Array(),
        logTime: 0,
        publishTime: 0,
        sequence: 0,
      },
      {
        type: "MessageIndex",
        channelId: 0,
        records: [[0, 71]],
      },
    ];

    const originalRecords = readAsMcapStream(tempBuffer.get());
    expect(originalRecords).toEqual<TypedMcapRecord[]>([
      ...commonRecords,
      {
        type: "DataEnd",
        dataSectionCrc: 2968501716,
      },
      {
        type: "Schema",
        id: 1,
        encoding: "json",
        data: new Uint8Array(),
        name: "schema1",
      },
      {
        type: "Channel",
        id: 0,
        messageEncoding: "json",
        metadata: new Map(),
        schemaId: 1,
        topic: "channel1",
      },
      {
        type: "Statistics",
        attachmentCount: 0,
        channelCount: 1,
        channelMessageCounts: new Map([[0, 1]]),
        chunkCount: 1,
        messageCount: 1,
        messageEndTime: 0,
        messageStartTime: 0,
        metadataCount: 0,
        schemaCount: 1,
      },
      {
        type: "ChunkIndex",
        chunkLength: 151,
        chunkStartOffset: 25,
        compressedSize: 102,
        compression: "",
        messageEndTime: 0,
        messageIndexLength: 31,
        messageIndexOffsets: new Map([[0, 176]]),
        messageStartTime: 0,
        uncompressedSize: 102,
      },
      {
        type: "SummaryOffset",
        groupLength: 34,
        groupOpcode: Opcode.SCHEMA,
        groupStart: 220,
      },
      {
        type: "SummaryOffset",
        groupLength: 37,
        groupOpcode: Opcode.CHANNEL,
        groupStart: 254,
      },
      {
        type: "SummaryOffset",
        groupLength: 65,
        groupOpcode: Opcode.STATISTICS,
        groupStart: 291,
      },
      {
        type: "SummaryOffset",
        groupLength: 83,
        groupOpcode: Opcode.CHUNK_INDEX,
        groupStart: 356,
      },
      {
        type: "Footer",
        summaryCrc: 2739614603,
        summaryOffsetStart: 439,
        summaryStart: 220,
      },
    ]);

    const appendWriter = await McapWriter.InitializeForAppending(tempBuffer, {});

    await appendWriter.addAttachment({
      name: "attachment1",
      logTime: 0,
      createTime: 0,
      mediaType: "text/plain",
      data: new TextEncoder().encode("foo"),
    });
    await appendWriter.addMetadata({
      name: "metadata1",
      metadata: new Map<string, string>([["test", "testValue"]]),
    });
    await appendWriter.addMessage({
      channelId: channelId1,
      data: new Uint8Array(),
      sequence: 1,
      logTime: 1,
      publishTime: 1,
    });
    const channelId2 = await appendWriter.registerChannel({
      topic: "channel2",
      schemaId,
      messageEncoding: "json",
      metadata: new Map(),
    });
    await appendWriter.addMessage({
      channelId: channelId2,
      data: new Uint8Array(),
      sequence: 2,
      logTime: 2,
      publishTime: 2,
    });
    await appendWriter.end();

    const appendedRecords = readAsMcapStream(tempBuffer.get());

    const newSummaryStart = 546;
    const dataEndLength = 1 + 8 + 4;
    const expectedDataCrc = crc32(
      tempBuffer.get().slice(0, Number(newSummaryStart - dataEndLength)),
    );

    expect(appendedRecords).toEqual<TypedMcapRecord[]>([
      ...commonRecords,
      {
        type: "Attachment",
        name: "attachment1",
        logTime: 0,
        createTime: 0,
        mediaType: "text/plain",
        data: new TextEncoder().encode("foo"),
      },
      {
        type: "Metadata",
        name: "metadata1",
        metadata: new Map([["test", "testValue"]]),
      },
      {
        type: "Message",
        channelId: 0,
        data: new Uint8Array(),
        logTime: 1,
        publishTime: 1,
        sequence: 1,
      },
      {
        type: "Channel",
        id: 1,
        messageEncoding: "json",
        metadata: new Map(),
        schemaId: 1,
        topic: "channel2",
      },
      {
        type: "Message",
        channelId: 1,
        data: new Uint8Array(),
        logTime: 2,
        publishTime: 2,
        sequence: 2,
      },
      {
        type: "MessageIndex",
        channelId: 0,
        records: [[1, 0]],
      },
      {
        type: "MessageIndex",
        channelId: 1,
        records: [[2, 68]],
      },
      {
        type: "DataEnd",
        dataSectionCrc: expectedDataCrc,
      },
      {
        type: "Schema",
        id: 1,
        encoding: "json",
        data: new Uint8Array(),
        name: "schema1",
      },
      {
        type: "Channel",
        id: 0,
        messageEncoding: "json",
        metadata: new Map(),
        schemaId: 1,
        topic: "channel1",
      },
      {
        type: "Channel",
        id: 1,
        messageEncoding: "json",
        metadata: new Map(),
        schemaId: 1,
        topic: "channel2",
      },
      {
        type: "Statistics",
        attachmentCount: 1,
        channelCount: 2,
        channelMessageCounts: new Map([
          [0, 2],
          [1, 1],
        ]),
        chunkCount: 2,
        messageCount: 3,
        messageEndTime: 2,
        messageStartTime: 0,
        metadataCount: 1,
        schemaCount: 1,
      },
      {
        type: "MetadataIndex",
        offset: 276,
        length: 47,
        name: "metadata1",
      },
      {
        type: "AttachmentIndex",
        offset: 207,
        length: 69,
        logTime: 0,
        createTime: 0,
        dataSize: 3,
        name: "attachment1",
        mediaType: "text/plain",
      },
      {
        type: "ChunkIndex",
        chunkLength: 151,
        chunkStartOffset: 25,
        compressedSize: 102,
        compression: "",
        messageEndTime: 0,
        messageIndexLength: 31,
        messageIndexOffsets: new Map([[0, 176]]),
        messageStartTime: 0,
        uncompressedSize: 102,
      },
      {
        type: "ChunkIndex",
        chunkLength: 148,
        chunkStartOffset: 323,
        compressedSize: 99,
        compression: "",
        messageEndTime: 2,
        messageIndexLength: 62,
        messageIndexOffsets: new Map([
          [0, 471],
          [1, 502],
        ]),
        messageStartTime: 1,
        uncompressedSize: 99,
      },
      {
        type: "SummaryOffset",
        groupLength: 34,
        groupOpcode: Opcode.SCHEMA,
        groupStart: 546,
      },
      {
        type: "SummaryOffset",
        groupLength: 74,
        groupOpcode: Opcode.CHANNEL,
        groupStart: 580,
      },
      {
        type: "SummaryOffset",
        groupLength: 75,
        groupOpcode: Opcode.STATISTICS,
        groupStart: 654,
      },
      {
        type: "SummaryOffset",
        groupLength: 38,
        groupOpcode: Opcode.METADATA_INDEX,
        groupStart: 729,
      },
      {
        type: "SummaryOffset",
        groupLength: 78,
        groupOpcode: Opcode.ATTACHMENT_INDEX,
        groupStart: 767,
      },
      {
        type: "SummaryOffset",
        groupLength: 176,
        groupOpcode: Opcode.CHUNK_INDEX,
        groupStart: 845,
      },
      {
        type: "Footer",
        summaryCrc: 758669511,
        summaryOffsetStart: 1021,
        summaryStart: newSummaryStart,
      },
    ]);
  });

  it.each([true, false])(
    "respects data_section_crc present=%s when appending",
    async (useDataSectionCrc) => {
      const originalDataSection = new Uint8Array([
        ...MCAP_MAGIC,
        ...record(Opcode.HEADER, [
          ...string(""), // profile
          ...string("lib"), // library
        ]),
      ]);
      const dataEndLength = 1 + 8 + 4;
      const tempBuffer = new TempBuffer(
        new Uint8Array([
          ...originalDataSection,
          ...record(Opcode.DATA_END, [
            ...uint32LE(useDataSectionCrc ? crc32(originalDataSection) : 0), // data crc
          ]),
          ...record(Opcode.STATISTICS, [
            ...uint64LE(0), // message count
            ...uint16LE(0), // schema count
            ...uint32LE(0), // channel count
            ...uint32LE(0), // attachment count
            ...uint32LE(0), // metadata count
            ...uint32LE(0), // chunk count
            ...uint64LE(0), // message start time
            ...uint64LE(0), // message end time
            ...uint32LE(0), // channel message counts length
          ]),
          ...record(Opcode.FOOTER, [
            ...uint64LE(originalDataSection.length + dataEndLength), // summary start
            ...uint64LE(0), // summary offset start
            ...uint32LE(0), // summary crc
          ]),
          ...MCAP_MAGIC,
        ]),
      );
      const appendWriter = await McapWriter.InitializeForAppending(tempBuffer, {
        repeatChannels: false,
        useSummaryOffsets: false,
        useChunks: false,
      });
      const chanId = await appendWriter.registerChannel({
        messageEncoding: "foo",
        metadata: new Map(),
        schemaId: 0,
        topic: "foo",
      });
      await appendWriter.addMessage({
        channelId: chanId,
        logTime: 0,
        publishTime: 0,
        sequence: 0,
        data: new Uint8Array([]),
      });
      await appendWriter.end();

      const summarySection = new Uint8Array([
        ...record(Opcode.STATISTICS, [
          ...uint64LE(1), // message count
          ...uint16LE(0), // schema count
          ...uint32LE(1), // channel count
          ...uint32LE(0), // attachment count
          ...uint32LE(0), // metadata count
          ...uint32LE(0), // chunk count
          ...uint64LE(0), // message start time
          ...uint64LE(0), // message end time
          ...keyValues(uint16LE, uint64LE, [[0, 1]]), // channel message counts length
        ]),
      ]);

      const newDataSection = new Uint8Array([
        ...originalDataSection,
        ...record(Opcode.CHANNEL, [
          ...uint16LE(0), // channel id
          ...uint16LE(0), // schema id
          ...string("foo"), // topic
          ...string("foo"), // message encoding
          ...keyValues(string, string, []), // user data
        ]),
        ...record(Opcode.MESSAGE, [
          ...uint16LE(chanId),
          ...uint32LE(0), // sequence
          ...uint64LE(0), // log time
          ...uint64LE(0), // publish time
        ]),
      ]);

      expect(tempBuffer.get()).toEqual(
        new Uint8Array([
          ...newDataSection,
          ...record(Opcode.DATA_END, [
            ...uint32LE(useDataSectionCrc ? crc32(newDataSection) : 0), // data crc
          ]),
          ...summarySection,
          ...record(Opcode.FOOTER, [
            ...uint64LE(newDataSection.length + dataEndLength), // summary start
            ...uint64LE(0), // summary offset start
            ...uint32LE(
              // summary crc
              crc32(
                new Uint8Array([
                  ...summarySection,
                  Opcode.FOOTER,
                  ...uint64LE(8 + 8 + 4), // footer record length
                  ...uint64LE(newDataSection.length + dataEndLength), // summary start
                  ...uint64LE(0), // summary offset start
                ]),
              ),
            ),
          ]),
          ...MCAP_MAGIC,
        ]),
      );
    },
  );
});
