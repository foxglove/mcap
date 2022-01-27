import { BufferBuilder } from "./BufferBuilder";
import { Mcap0RecordBuilder } from "./Mcap0RecordBuilder";

describe("Mcap0BufferRecordBuilder", () => {
  it("writes magic", async () => {
    const writer = new Mcap0RecordBuilder();

    writer.writeMagic();
    expect(writer.buffer).toEqual(new Uint8Array([137, 77, 67, 65, 80, 48, 13, 10]));
  });

  it("writes header", async () => {
    const writer = new Mcap0RecordBuilder();

    const written = writer.writeHeader({
      profile: "foo",
      library: "bar",
      metadata: [["something", "magical"]],
    });

    const byteLength = 42;
    const buffer = new BufferBuilder();
    buffer
      .uint8(1) // opcode
      .uint64(BigInt(byteLength)) // record byte length
      .string("foo")
      .string("bar")
      .uint32(24) // metadata byte length
      .string("something")
      .string("magical");

    expect(buffer.length).toEqual(byteLength + 9);
    expect(writer.buffer).toEqual(buffer.buffer);
    expect(written).toEqual(buffer.length);
  });

  it("writes footer", async () => {
    const writer = new Mcap0RecordBuilder();

    writer.writeFooter({
      summaryStart: 0n,
      summaryOffsetStart: 0n,
      crc: 0,
    });

    const buffer = new BufferBuilder();
    buffer
      .uint8(2) // opcode
      .uint64(BigInt(20)) // record byte length
      .uint64(0n)
      .uint64(0n)
      .uint32(0);

    expect(buffer.length).toEqual(12 + 9);
    expect(writer.buffer).toEqual(buffer.buffer);
  });

  it("writes channel info", async () => {
    const writer = new Mcap0RecordBuilder();

    const written = writer.writeChannelInfo({
      channelId: 1,
      topicName: "/topic",
      messageEncoding: "encoding",
      schemaFormat: "someformat",
      schemaVersion: "",
      schemaName: "schema name",
      schema: "schema",
      userData: [],
    });

    const buffer = new BufferBuilder();
    buffer
      .uint8(3) // opcode
      .uint64(BigInt(57)) // record byte length
      .uint16(1)
      .string("/topic")
      .string("encoding")
      .string("someformat")
      .string("")
      .string("schema")
      .string("schema name")
      .uint32(0) // user data length
      .uint32(0); // crc

    expect(buffer.length).toEqual(57 + 9);
    expect(writer.buffer).toEqual(buffer.buffer);
    expect(written).toEqual(buffer.length);
  });

  it("writes messages", async () => {
    const writer = new Mcap0RecordBuilder();

    writer.writeMessage({
      channelId: 1,
      publishTime: 3n,
      recordTime: 5n,
      sequence: 7,
      messageData: new Uint8Array(),
    });

    const buffer = new BufferBuilder();
    buffer
      .uint8(4) // opcode
      .uint64(BigInt(22)) // record byte length
      .uint16(1)
      .uint32(7)
      .uint64(3n)
      .uint64(5n);

    expect(buffer.length).toEqual(22 + 9);
    expect(writer.buffer).toEqual(buffer.buffer);
  });
});
