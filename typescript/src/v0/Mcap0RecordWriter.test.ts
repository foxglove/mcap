import { IWritable } from ".";
import { Mcap0RecordWriter } from "./Mcap0RecordWriter";

class MemoryWritable implements IWritable {
  private fullBuffer: Uint8Array;
  private offset = 0;

  get length() {
    return this.offset;
  }

  get buffer(): Readonly<Uint8Array> {
    return this.fullBuffer.slice(0, this.offset);
  }

  constructor() {
    this.fullBuffer = new Uint8Array(4096);
  }

  async write(buffer: Uint8Array): Promise<void> {
    this.fullBuffer.set(buffer, this.offset);
    this.offset += buffer.length;
  }
}

describe("Mcap0RecordWriter", () => {
  it("writes magic", async () => {
    const memoryWritable = new MemoryWritable();
    const writer = new Mcap0RecordWriter(memoryWritable);

    await writer.writeMagic();
    expect(memoryWritable.buffer).toEqual(new Uint8Array([137, 77, 67, 65, 80, 48, 13, 10]));
  });

  it("writes header", async () => {
    const memoryWritable = new MemoryWritable();
    const writer = new Mcap0RecordWriter(memoryWritable);

    await writer.writeHeader({
      profile: "foo",
      library: "bar",
      metadata: [["something", "magical"]],
    });
    expect(memoryWritable.buffer).toEqual(
      new Uint8Array([
        1, 42, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 102, 111, 111, 3, 0, 0, 0, 98, 97, 114, 24, 0, 0, 0,
        9, 0, 0, 0, 115, 111, 109, 101, 116, 104, 105, 110, 103, 7, 0, 0, 0, 109, 97, 103, 105, 99,
        97, 108,
      ]),
    );
  });

  it("writes footer", async () => {
    const memoryWritable = new MemoryWritable();
    const writer = new Mcap0RecordWriter(memoryWritable);

    await writer.writeFooter({
      indexOffset: 0n,
      indexCrc: 0,
    });
    expect(memoryWritable.buffer).toEqual(
      new Uint8Array([2, 12, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]),
    );
  });

  it("writes channel info", async () => {
    const memoryWritable = new MemoryWritable();
    const writer = new Mcap0RecordWriter(memoryWritable);

    await writer.writeChannelInfo({
      channelId: 1,
      topicName: "topic",
      encoding: "enc",
      schemaName: "foo",
      schema: "bar",
      userData: [],
    });
    expect(memoryWritable.buffer).toEqual(
      new Uint8Array([
        3, 40, 0, 0, 0, 0, 0, 0, 0, 1, 0, 5, 0, 0, 0, 116, 111, 112, 105, 99, 3, 0, 0, 0, 101, 110,
        99, 3, 0, 0, 0, 102, 111, 111, 3, 0, 0, 0, 98, 97, 114, 0, 0, 0, 0, 0, 0, 0, 0,
      ]),
    );
  });

  it("writes messages", async () => {
    const memoryWritable = new MemoryWritable();
    const writer = new Mcap0RecordWriter(memoryWritable);

    await writer.writeMessage({
      channelId: 1,
      publishTime: 3n,
      recordTime: 5n,
      sequence: 7,
      messageData: new Uint8Array(),
    });
    expect(memoryWritable.buffer).toEqual(
      new Uint8Array([
        4, 22, 0, 0, 0, 0, 0, 0, 0, 1, 0, 7, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 5, 0, 0, 0, 0, 0, 0,
        0,
      ]),
    );
  });
});
