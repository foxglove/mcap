import { BufferBuilder } from "./BufferBuilder";
import { Mcap0RecordBuilder } from "./Mcap0RecordBuilder";

describe("Mcap0RecordBuilder", () => {
  it("writes magic", () => {
    const writer = new Mcap0RecordBuilder();

    writer.writeMagic();
    expect(writer.buffer).toEqual(new Uint8Array([137, 77, 67, 65, 80, 48, 13, 10]));
  });

  it("writes header", () => {
    const writer = new Mcap0RecordBuilder();

    const written = writer.writeHeader({
      profile: "foo",
      library: "bar",
    });

    const buffer = new BufferBuilder();
    buffer
      .uint8(1) // opcode
      .uint64(BigInt(14)) // record content byte length
      .string("foo")
      .string("bar");

    expect(writer.buffer).toEqual(buffer.buffer);
    expect(written).toEqual(BigInt(buffer.length));
  });

  it("writes footer", () => {
    const writer = new Mcap0RecordBuilder();

    writer.writeFooter({
      summaryStart: 0n,
      summaryOffsetStart: 0n,
      summaryCrc: 0,
    });

    const buffer = new BufferBuilder();
    buffer
      .uint8(2) // opcode
      .uint64(BigInt(20)) // record content byte length
      .uint64(0n)
      .uint64(0n)
      .uint32(0);

    expect(writer.buffer).toEqual(buffer.buffer);
  });

  it("writes schema", () => {
    const writer = new Mcap0RecordBuilder();

    const written = writer.writeSchema({
      id: 1,
      encoding: "some format",
      name: "schema name",
      data: new TextEncoder().encode("schema"),
    });

    const buffer = new BufferBuilder();
    buffer
      .uint8(3) // opcode
      .uint64(BigInt(46)) // record content byte length
      .uint16(1)
      .string("schema name")
      .string("some format")
      .uint64(BigInt(new TextEncoder().encode("schema").byteLength))
      .bytes(new TextEncoder().encode("schema"));

    expect(writer.buffer).toEqual(buffer.buffer);
    expect(written).toEqual(BigInt(buffer.length));
  });

  it("writes channel info", () => {
    const writer = new Mcap0RecordBuilder();

    const written = writer.writeChannelInfo({
      id: 1,
      topic: "/topic",
      messageEncoding: "encoding",
      schemaId: 1,
      metadata: [],
    });

    const buffer = new BufferBuilder();
    buffer
      .uint8(4) // opcode
      .uint64(BigInt(30)) // record content byte length
      .uint16(1)
      .string("/topic")
      .string("encoding")
      .uint16(1)
      .uint32(0); // user data length

    expect(writer.buffer).toEqual(buffer.buffer);
    expect(written).toEqual(BigInt(buffer.length));
  });

  it("writes messages", () => {
    const writer = new Mcap0RecordBuilder();

    writer.writeMessage({
      channelId: 1,
      publishTime: 3n,
      logTime: 5n,
      sequence: 7,
      messageData: new Uint8Array(),
    });

    const buffer = new BufferBuilder();
    buffer
      .uint8(5) // opcode
      .uint64(BigInt(22)) // record content byte length
      .uint16(1)
      .uint32(7)
      .uint64(3n)
      .uint64(5n);

    expect(buffer.length).toEqual(22 + 9);
    expect(writer.buffer).toEqual(buffer.buffer);
  });

  it("writes metadata", () => {
    const writer = new Mcap0RecordBuilder();

    const written = writer.writeMetadata({
      name: "name",
      metadata: [["something", "magical"]],
    });

    const buffer = new BufferBuilder();
    buffer
      .uint8(0x0c) // opcode
      .uint64(BigInt(36)) // record content byte length
      .string("name")
      .uint32(24) // metadata byte length
      .string("something")
      .string("magical");

    expect(writer.buffer).toEqual(buffer.buffer);
    expect(written).toEqual(BigInt(buffer.length));
  });
});
