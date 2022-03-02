import Mcap0IndexedReader from "./Mcap0IndexedReader";
import Mcap0StreamReader from "./Mcap0StreamReader";
import { Mcap0Writer } from "./Mcap0Writer";
import { collect } from "./testUtils";
import { TypedMcapRecord } from "./types";

class TempBuffer {
  private _buffer = new ArrayBuffer(1024);
  private _size = 0;

  position() {
    return BigInt(this._size);
  }
  async write(data: Uint8Array) {
    if (this._size + data.byteLength > this._buffer.byteLength) {
      const newBuffer = new ArrayBuffer(this._size + data.byteLength);
      new Uint8Array(newBuffer).set(new Uint8Array(this._buffer));
      this._buffer = newBuffer;
    }
    new Uint8Array(this._buffer, this._size).set(data);
    this._size += data.byteLength;
  }

  async size() {
    return BigInt(this._size);
  }
  async read(offset: bigint, size: bigint) {
    if (offset < 0n || offset + size > BigInt(this._buffer.byteLength)) {
      throw new Error("read out of range");
    }
    return new Uint8Array(this._buffer, Number(offset), Number(size));
  }

  get() {
    return new Uint8Array(this._buffer, 0, this._size);
  }
}

describe("Mcap0Writer", () => {
  it("supports messages with logTime 0", async () => {
    const tempBuffer = new TempBuffer();
    const writer = new Mcap0Writer({ writable: tempBuffer });

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

    const reader = await Mcap0IndexedReader.Initialize({ readable: tempBuffer });

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
    const writer = new Mcap0Writer({ writable: tempBuffer, chunkSize: 0 });

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

    const reader = new Mcap0StreamReader();
    reader.append(tempBuffer.get());
    const records: TypedMcapRecord[] = [];
    for (let record; (record = reader.nextRecord()); ) {
      records.push(record);
    }

    expect(records).toEqual([
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
        dataSectionCrc: 0,
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
        groupOpcode: 4,
        groupStart: 293n,
      },
      {
        type: "SummaryOffset",
        groupLength: 65n,
        groupOpcode: 11,
        groupStart: 326n,
      },
      {
        type: "SummaryOffset",
        groupLength: 166n,
        groupOpcode: 8,
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
});
