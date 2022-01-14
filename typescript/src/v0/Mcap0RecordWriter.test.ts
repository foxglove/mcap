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
        1, 3, 0, 0, 0, 102, 111, 111, 3, 0, 0, 0, 98, 97, 114, 24, 0, 0, 0, 9, 0, 0, 0, 115, 111,
        109, 101, 116, 104, 105, 110, 103, 7, 0, 0, 0, 109, 97, 103, 105, 99, 97, 108,
      ]),
    );
  });
});
