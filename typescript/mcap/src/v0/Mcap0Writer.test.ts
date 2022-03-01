import Mcap0IndexedReader from "./Mcap0IndexedReader";
import { Mcap0Writer } from "./Mcap0Writer";
import { collect } from "./testUtils";

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
});
