import { ChunkBuilder } from "./ChunkBuilder.ts";
import { ChunkCursor } from "./ChunkCursor.ts";
import { McapIndexedReader } from "./McapIndexedReader.ts";
import { McapRecordBuilder } from "./McapRecordBuilder.ts";
import McapStreamReader from "./McapStreamReader.ts";
import { McapWriter } from "./McapWriter.ts";
import { TempBuffer } from "./TempBuffer.ts";
import { collect } from "./testUtils.ts";
import type { TypedMcapRecords } from "./types.ts";

class TestChunkBuilder extends ChunkBuilder {
  messageIndexCount(): number {
    return this.messageIndices?.size ?? 0;
  }
}

class TestStreamReader extends McapStreamReader {
  channelCount(): number {
    return this.channelsById.size;
  }
}

class TestIndexedReader extends McapIndexedReader {
  loadChunkDataCalls = 0;

  protected override async loadChunkData(
    chunkIndex: TypedMcapRecords["ChunkIndex"],
    options?: { validateCrcs: boolean },
  ): Promise<DataView> {
    this.loadChunkDataCalls += 1;
    return await super.loadChunkData(chunkIndex, options);
  }

  messageTimeRange(): { start?: bigint; end?: bigint } {
    return { start: this.messageStartTime, end: this.messageEndTime };
  }
}

class TestWriter extends McapWriter {
  finalizeCalls = 0;

  protected override async finalizeChunk(): Promise<void> {
    this.finalizeCalls += 1;
    await super.finalizeChunk();
  }
}

class TestRecordBuilder extends McapRecordBuilder {
  writtenLength(): number {
    return this.bufferBuilder.length;
  }
}

class TestChunkCursor extends ChunkCursor {
  sortTime(): bigint {
    return this.getSortTime();
  }
}

describe("subclassing", () => {
  it("lets ChunkBuilder subclasses use protected members", () => {
    const builder = new TestChunkBuilder({ useMessageIndex: true });
    builder.addChannel({
      id: 1,
      schemaId: 0,
      topic: "test",
      messageEncoding: "json",
      metadata: new Map(),
    });
    builder.addMessage({
      channelId: 1,
      sequence: 0,
      logTime: 10n,
      publishTime: 10n,
      data: new Uint8Array([1]),
    });

    expect(builder.messageIndexCount()).toBe(1);
    expect(builder.numMessages).toBe(1);
    expect(builder.byteLength).toBeGreaterThan(0);
  });

  it("lets McapStreamReader subclasses use protected members", async () => {
    const tempBuffer = new TempBuffer();
    const writer = new McapWriter({ writable: tempBuffer, useChunks: false });

    await writer.start({ library: "", profile: "" });
    const channelId = await writer.registerChannel({
      topic: "test",
      schemaId: 0,
      messageEncoding: "json",
      metadata: new Map(),
    });
    await writer.addMessage({
      channelId,
      sequence: 0,
      logTime: 0n,
      publishTime: 0n,
      data: new Uint8Array(),
    });
    await writer.end();

    const reader = new TestStreamReader();
    reader.append(tempBuffer.get());
    while (reader.nextRecord()) {
      // Parse all records so the channel map is populated.
    }
    expect(reader.channelCount()).toBe(1);
    expect(reader.done()).toBe(true);
  });

  it("lets McapIndexedReader subclasses override protected methods", async () => {
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
      sequence: 0,
      logTime: 5n,
      publishTime: 5n,
      data: new Uint8Array([9]),
    });
    await writer.end();

    const reader = await TestIndexedReader.Initialize({ readable: tempBuffer });
    expect(reader).toBeInstanceOf(TestIndexedReader);

    const messages = await collect(reader.readMessages());
    expect(messages).toHaveLength(1);
    expect(reader.loadChunkDataCalls).toBe(1);
    expect(reader.messageTimeRange()).toEqual({ start: 5n, end: 5n });
  });

  it("lets McapWriter subclasses override protected methods", async () => {
    const tempBuffer = new TempBuffer();
    const writer = new TestWriter({ writable: tempBuffer, chunkSize: 0 });

    await writer.start({ library: "", profile: "" });
    const channelId = await writer.registerChannel({
      topic: "test",
      schemaId: 0,
      messageEncoding: "json",
      metadata: new Map(),
    });
    await writer.addMessage({
      channelId,
      sequence: 0,
      logTime: 0n,
      publishTime: 0n,
      data: new Uint8Array(),
    });
    await writer.addMessage({
      channelId,
      sequence: 1,
      logTime: 1n,
      publishTime: 1n,
      data: new Uint8Array(),
    });
    await writer.end();

    // chunkSize 0 finalizes after every message, plus end() finalizes any remainder.
    expect(writer.finalizeCalls).toBeGreaterThanOrEqual(2);
  });

  it("lets McapRecordBuilder subclasses use protected members", () => {
    const builder = new TestRecordBuilder();
    builder.writeMagic();
    expect(builder.writtenLength()).toBeGreaterThan(0);
    expect(builder.length).toBe(builder.writtenLength());
  });

  it("lets ChunkCursor subclasses use protected members", () => {
    const cursor = new TestChunkCursor({
      chunkIndex: {
        type: "ChunkIndex",
        messageStartTime: 0n,
        messageEndTime: 0n,
        chunkStartOffset: 1n,
        chunkLength: 1n,
        messageIndexOffsets: new Map(),
        messageIndexLength: 0n,
        compression: "",
        compressedSize: 0n,
        uncompressedSize: 0n,
      },
      relevantChannels: undefined,
      startTime: undefined,
      endTime: undefined,
      reverse: false,
    });

    expect(cursor.sortTime()).toBe(0n);
  });
});
