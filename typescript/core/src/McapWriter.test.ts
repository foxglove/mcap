import { crc32 } from "@foxglove/crc";

import { McapIndexedReader } from "./McapIndexedReader.ts";
import McapStreamReader from "./McapStreamReader.ts";
import { McapWriter } from "./McapWriter.ts";
import Reader from "./Reader.ts";
import { TempBuffer } from "./TempBuffer.ts";
import { MCAP_MAGIC, Opcode } from "./constants.ts";
import { parseMagic, parseRecord } from "./parse.ts";
import { collect, keyValues, record, string, uint16LE, uint32LE, uint64LE } from "./testUtils.ts";
import type { TypedMcapRecord } from "./types.ts";

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
      logTime: 0n,
      publishTime: 0n,
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
        logTime: 0n,
        publishTime: 0n,
        sequence: 0,
      },
      {
        type: "MessageIndex",
        channelId: 0,
        records: [[0n, 71n]],
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
        channelMessageCounts: new Map([[0, 1n]]),
        chunkCount: 1,
        messageCount: 1n,
        messageEndTime: 0n,
        messageStartTime: 0n,
        metadataCount: 0,
        schemaCount: 1,
      },
      {
        type: "ChunkIndex",
        chunkLength: 151n,
        chunkStartOffset: 25n,
        compressedSize: 102n,
        compression: "",
        messageEndTime: 0n,
        messageIndexLength: 31n,
        messageIndexOffsets: new Map([[0, 176n]]),
        messageStartTime: 0n,
        uncompressedSize: 102n,
      },
      {
        type: "SummaryOffset",
        groupLength: 34n,
        groupOpcode: Opcode.SCHEMA,
        groupStart: 220n,
      },
      {
        type: "SummaryOffset",
        groupLength: 37n,
        groupOpcode: Opcode.CHANNEL,
        groupStart: 254n,
      },
      {
        type: "SummaryOffset",
        groupLength: 65n,
        groupOpcode: Opcode.STATISTICS,
        groupStart: 291n,
      },
      {
        type: "SummaryOffset",
        groupLength: 83n,
        groupOpcode: Opcode.CHUNK_INDEX,
        groupStart: 356n,
      },
      {
        type: "Footer",
        summaryCrc: 2739614603,
        summaryOffsetStart: 439n,
        summaryStart: 220n,
      },
    ]);

    const appendWriter = await McapWriter.InitializeForAppending(tempBuffer, {});

    await appendWriter.addAttachment({
      name: "attachment1",
      logTime: 0n,
      createTime: 0n,
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
      logTime: 1n,
      publishTime: 1n,
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
      logTime: 2n,
      publishTime: 2n,
    });
    await appendWriter.end();

    const appendedRecords = readAsMcapStream(tempBuffer.get());

    const newSummaryStart = 546n;
    const dataEndLength = 1n + 8n + 4n;
    const expectedDataCrc = crc32(
      tempBuffer.get().slice(0, Number(newSummaryStart - dataEndLength)),
    );

    expect(appendedRecords).toEqual<TypedMcapRecord[]>([
      ...commonRecords,
      {
        type: "Attachment",
        name: "attachment1",
        logTime: 0n,
        createTime: 0n,
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
        topic: "channel2",
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
        records: [[1n, 0n]],
      },
      {
        type: "MessageIndex",
        channelId: 1,
        records: [[2n, 68n]],
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
        offset: 276n,
        length: 47n,
        name: "metadata1",
      },
      {
        type: "AttachmentIndex",
        offset: 207n,
        length: 69n,
        logTime: 0n,
        createTime: 0n,
        dataSize: 3n,
        name: "attachment1",
        mediaType: "text/plain",
      },
      {
        type: "ChunkIndex",
        chunkLength: 151n,
        chunkStartOffset: 25n,
        compressedSize: 102n,
        compression: "",
        messageEndTime: 0n,
        messageIndexLength: 31n,
        messageIndexOffsets: new Map([[0, 176n]]),
        messageStartTime: 0n,
        uncompressedSize: 102n,
      },
      {
        type: "ChunkIndex",
        chunkLength: 148n,
        chunkStartOffset: 323n,
        compressedSize: 99n,
        compression: "",
        messageEndTime: 2n,
        messageIndexLength: 62n,
        messageIndexOffsets: new Map([
          [0, 471n],
          [1, 502n],
        ]),
        messageStartTime: 1n,
        uncompressedSize: 99n,
      },
      {
        type: "SummaryOffset",
        groupLength: 34n,
        groupOpcode: Opcode.SCHEMA,
        groupStart: 546n,
      },
      {
        type: "SummaryOffset",
        groupLength: 74n,
        groupOpcode: Opcode.CHANNEL,
        groupStart: 580n,
      },
      {
        type: "SummaryOffset",
        groupLength: 75n,
        groupOpcode: Opcode.STATISTICS,
        groupStart: 654n,
      },
      {
        type: "SummaryOffset",
        groupLength: 38n,
        groupOpcode: Opcode.METADATA_INDEX,
        groupStart: 729n,
      },
      {
        type: "SummaryOffset",
        groupLength: 78n,
        groupOpcode: Opcode.ATTACHMENT_INDEX,
        groupStart: 767n,
      },
      {
        type: "SummaryOffset",
        groupLength: 176n,
        groupOpcode: Opcode.CHUNK_INDEX,
        groupStart: 845n,
      },
      {
        type: "Footer",
        summaryCrc: 758669511,
        summaryOffsetStart: 1021n,
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
            ...uint64LE(0n), // message count
            ...uint16LE(0), // schema count
            ...uint32LE(0), // channel count
            ...uint32LE(0), // attachment count
            ...uint32LE(0), // metadata count
            ...uint32LE(0), // chunk count
            ...uint64LE(0n), // message start time
            ...uint64LE(0n), // message end time
            ...uint32LE(0), // channel message counts length
          ]),
          ...record(Opcode.FOOTER, [
            ...uint64LE(BigInt(originalDataSection.length + dataEndLength)), // summary start
            ...uint64LE(0n), // summary offset start
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
        logTime: 0n,
        publishTime: 0n,
        sequence: 0,
        data: new Uint8Array([]),
      });
      await appendWriter.end();

      const summarySection = new Uint8Array([
        ...record(Opcode.STATISTICS, [
          ...uint64LE(1n), // message count
          ...uint16LE(0), // schema count
          ...uint32LE(1), // channel count
          ...uint32LE(0), // attachment count
          ...uint32LE(0), // metadata count
          ...uint32LE(0), // chunk count
          ...uint64LE(0n), // message start time
          ...uint64LE(0n), // message end time
          ...keyValues(uint16LE, uint64LE, [[0, 1n]]), // channel message counts length
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
          ...uint64LE(0n), // log time
          ...uint64LE(0n), // publish time
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
            ...uint64LE(BigInt(newDataSection.length + dataEndLength)), // summary start
            ...uint64LE(0n), // summary offset start
            ...uint32LE(
              // summary crc
              crc32(
                new Uint8Array([
                  ...summarySection,
                  Opcode.FOOTER,
                  ...uint64LE(8n + 8n + 4n), // footer record length
                  ...uint64LE(BigInt(newDataSection.length + dataEndLength)), // summary start
                  ...uint64LE(0n), // summary offset start
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
