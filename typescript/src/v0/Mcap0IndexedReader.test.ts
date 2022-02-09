import { crc32 } from "@foxglove/crc";

import Mcap0IndexedReader from "./Mcap0IndexedReader";
import { MCAP0_MAGIC, Opcode } from "./constants";
import {
  record,
  uint64LE,
  uint32LE,
  string,
  keyValues,
  collect,
  uint16LE,
  uint32PrefixedBytes,
} from "./testUtils";
import { TypedMcapRecords } from "./types";

function makeReadable(data: Uint8Array) {
  let readCalls = 0;
  return {
    get readCalls() {
      return readCalls;
    },
    size: async () => BigInt(data.length),
    read: async (offset: bigint, size: bigint) => {
      ++readCalls;
      if (offset > Number.MAX_SAFE_INTEGER || size > Number.MAX_SAFE_INTEGER) {
        throw new Error(`Read too large: offset ${offset}, size ${size}`);
      }
      if (offset < 0 || size < 0 || offset + size > data.length) {
        throw new Error(
          `Read out of range: offset ${offset}, size ${size} (data.length: ${data.length})`,
        );
      }
      return data.slice(Number(offset), Number(offset + size));
    },
  };
}

describe("Mcap0IndexedReader", () => {
  it("rejects files that are too small", async () => {
    await expect(
      Mcap0IndexedReader.Initialize({
        readable: makeReadable(
          new Uint8Array([
            ...MCAP0_MAGIC,
            ...record(Opcode.FOOTER, [
              ...uint64LE(0n), // summary offset
              ...uint64LE(0n), // summary start offset
              ...uint32LE(0), // summary crc
            ]),
            ...MCAP0_MAGIC,
          ]),
        ),
      }),
    ).rejects.toThrow("Unable to read header at beginning of file; found Footer");

    await expect(
      Mcap0IndexedReader.Initialize({
        readable: makeReadable(
          new Uint8Array([
            ...MCAP0_MAGIC,
            ...record(Opcode.HEADER, [
              ...string(""), // profile
              ...string(""), // library
            ]),
            ...MCAP0_MAGIC,
          ]),
        ),
      }),
    ).rejects.toThrow("too small to be valid MCAP");
  });

  it("rejects unindexed file", async () => {
    const readable = makeReadable(
      new Uint8Array([
        ...MCAP0_MAGIC,
        ...record(Opcode.HEADER, [
          ...string(""), // profile
          ...string(""), // library
        ]),
        ...record(Opcode.FOOTER, [
          ...uint64LE(0n), // summary offset
          ...uint64LE(0n), // summary start offset
          ...uint32LE(0), // summary crc
        ]),
        ...MCAP0_MAGIC,
      ]),
    );
    await expect(Mcap0IndexedReader.Initialize({ readable })).rejects.toThrow(
      "File is not indexed",
    );
  });

  it("rejects invalid index crc", async () => {
    const data = [
      ...MCAP0_MAGIC,
      ...record(Opcode.HEADER, [
        ...string(""), // profile
        ...string(""), // library
      ]),
    ];
    const summaryStart = data.length;

    data.push(
      ...record(Opcode.METADATA, [
        ...string("foobar"),
        ...keyValues(string, string, []), // metadata
      ]),
    );

    data.push(
      ...record(Opcode.FOOTER, [
        ...uint64LE(BigInt(summaryStart)), // summary offset
        ...uint64LE(0n), // summary start offset
        ...uint32LE(crc32(new Uint8Array([42]))), // summary crc
      ]),
      ...MCAP0_MAGIC,
    );
    const readable = makeReadable(new Uint8Array(data));
    await expect(Mcap0IndexedReader.Initialize({ readable })).rejects.toThrow(
      "Incorrect index CRC 491514153 (expected 163128923)",
    );
  });

  it("parses index with schema and channel info", async () => {
    const data = [
      ...MCAP0_MAGIC,
      ...record(Opcode.HEADER, [
        ...string(""), // profile
        ...string(""), // library
      ]),
    ];
    const summaryStart = data.length;
    data.push(
      ...record(Opcode.SCHEMA, [
        ...uint16LE(1), // schema id
        ...string("some data"), // schema name
        ...string("json"), // schema format
        ...uint32PrefixedBytes(new TextEncoder().encode("stuff")), // schema
      ]),
      ...record(Opcode.CHANNEL, [
        ...uint16LE(42), // channel id
        ...string("myTopic"), // topic
        ...string("utf12"), // encoding
        ...uint16LE(1),
        ...keyValues(string, string, [["foo", "bar"]]), // user data
      ]),
      ...record(Opcode.FOOTER, [
        ...uint64LE(BigInt(summaryStart)), // summary offset
        ...uint64LE(0n), // summary start offset
        ...uint32LE(crc32(new Uint8Array(0))), // summary crc
      ]),
      ...MCAP0_MAGIC,
    );
    const readable = makeReadable(new Uint8Array(data));
    const reader = await Mcap0IndexedReader.Initialize({ readable });
    await expect(collect(reader.readMessages())).resolves.toEqual([]);
    expect(reader.channelsById).toEqual(
      new Map<number, TypedMcapRecords["Channel"]>([
        [
          42,
          {
            type: "Channel",
            id: 42,
            schemaId: 1,
            topic: "myTopic",
            messageEncoding: "utf12",
            metadata: new Map([["foo", "bar"]]),
          },
        ],
      ]),
    );
    expect(reader.schemasById).toEqual(
      new Map<number, TypedMcapRecords["Schema"]>([
        [
          1,
          {
            type: "Schema",
            id: 1,
            name: "some data",
            encoding: "json",
            data: new TextEncoder().encode("stuff"),
          },
        ],
      ]),
    );
    expect(readable.readCalls).toBe(4);
  });

  describe("indexed with single channel", () => {
    const message1: TypedMcapRecords["Message"] = {
      type: "Message",
      channelId: 42,
      sequence: 1,
      publishTime: 0n,
      logTime: 10n,
      data: new Uint8Array(),
    };
    const message2: TypedMcapRecords["Message"] = {
      type: "Message",
      channelId: 42,
      sequence: 2,
      publishTime: 1n,
      logTime: 11n,
      data: new Uint8Array(),
    };
    const message3: TypedMcapRecords["Message"] = {
      type: "Message",
      channelId: 42,
      sequence: 3,
      publishTime: 2n,
      logTime: 12n,
      data: new Uint8Array(),
    };
    it.each([
      { startTime: undefined, endTime: undefined, expected: [message1, message2, message3] },
      { startTime: 11n, endTime: 11n, expected: [message2] },
      { startTime: 11n, endTime: undefined, expected: [message2, message3] },
      { startTime: undefined, endTime: 11n, expected: [message1, message2] },
      { startTime: 10n, endTime: 12n, expected: [message1, message2, message3] },
    ])(
      "fetches chunk data and reads requested messages between $startTime and $endTime",
      async ({ startTime, endTime, expected }) => {
        const schema = record(Opcode.SCHEMA, [
          ...uint16LE(1), // schema id
          ...string("some data"), // schema name
          ...string("json"), // schema format
          ...uint32PrefixedBytes(new TextEncoder().encode("stuff")), // schema
        ]);
        const channelInfo = record(Opcode.CHANNEL, [
          ...uint16LE(42), // channel id
          ...string("myTopic"), // topic
          ...string("utf12"), // message encoding
          ...uint16LE(1), // schema id
          ...keyValues(string, string, [["foo", "bar"]]), // user data
        ]);
        const message1Data = record(Opcode.MESSAGE, [
          ...uint16LE(message1.channelId),
          ...uint32LE(message1.sequence),
          ...uint64LE(message1.publishTime),
          ...uint64LE(message1.logTime),
        ]);
        const message2Data = record(Opcode.MESSAGE, [
          ...uint16LE(message2.channelId),
          ...uint32LE(message2.sequence),
          ...uint64LE(message2.publishTime),
          ...uint64LE(message2.logTime),
        ]);
        const message3Data = record(Opcode.MESSAGE, [
          ...uint16LE(message3.channelId),
          ...uint32LE(message3.sequence),
          ...uint64LE(message3.publishTime),
          ...uint64LE(message3.logTime),
        ]);
        const chunkContents = [...schema, ...channelInfo];
        const message1Offset = BigInt(chunkContents.length);
        chunkContents.push(...message1Data);
        const message2Offset = BigInt(chunkContents.length);
        chunkContents.push(...message2Data);
        const message3Offset = BigInt(chunkContents.length);
        chunkContents.push(...message3Data);

        const data = [
          ...MCAP0_MAGIC,
          ...record(Opcode.HEADER, [
            ...string(""), // profile
            ...string(""), // library
          ]),
        ];
        const chunkOffset = BigInt(data.length);
        data.push(
          ...record(Opcode.CHUNK, [
            ...uint64LE(0n), // start time
            ...uint64LE(0n), // end time
            ...uint64LE(0n), // decompressed size
            ...uint32LE(crc32(new Uint8Array(chunkContents))), // decompressed crc32
            ...string(""), // compression
            ...uint64LE(BigInt(chunkContents.length)),
            ...chunkContents,
          ]),
        );
        const messageIndexOffset = BigInt(data.length);
        data.push(
          ...record(Opcode.MESSAGE_INDEX, [
            ...uint16LE(42), // channel id
            ...keyValues(uint64LE, uint64LE, [
              [message1.logTime, message1Offset],
              [message2.logTime, message2Offset],
              [message3.logTime, message3Offset],
            ]), // records
          ]),
        );
        const messageIndexLength = BigInt(data.length) - messageIndexOffset;
        const summaryStart = data.length;
        data.push(
          ...channelInfo,
          ...record(Opcode.CHUNK_INDEX, [
            ...uint64LE(message1.logTime), // start time
            ...uint64LE(message3.logTime), // end time
            ...uint64LE(chunkOffset), // offset
            ...uint64LE(0n), // chunk length
            ...keyValues(uint16LE, uint64LE, [[42, messageIndexOffset]]), // message index offsets
            ...uint64LE(messageIndexLength), // message index length
            ...string(""), // compression
            ...uint64LE(BigInt(chunkContents.length)), // compressed size
            ...uint64LE(BigInt(chunkContents.length)), // uncompressed size
          ]),
          ...record(Opcode.FOOTER, [
            ...uint64LE(BigInt(summaryStart)), // summary offset
            ...uint64LE(0n), // summary start offset
            ...uint32LE(crc32(new Uint8Array(0))), // summary crc
          ]),
          ...MCAP0_MAGIC,
        );
        const readable = makeReadable(new Uint8Array(data));
        const reader = await Mcap0IndexedReader.Initialize({ readable });
        await expect(collect(reader.readMessages({ startTime, endTime }))).resolves.toEqual(
          expected,
        );
        expect(readable.readCalls).toBe(6);
      },
    );
  });

  it("does not yet support overlapping chunks", async () => {
    const data = [
      ...MCAP0_MAGIC,
      ...record(Opcode.HEADER, [
        ...string(""), // profile
        ...string(""), // library
      ]),
    ];
    const summaryStart = BigInt(data.length);
    data.push(
      ...record(Opcode.CHUNK_INDEX, [
        ...uint64LE(0n), // start time
        ...uint64LE(2n), // end time
        ...uint64LE(0n), // offset
        ...uint64LE(0n), // chunk length
        ...keyValues(uint16LE, uint64LE, []), // message index offsets
        ...uint64LE(0n), // message index length
        ...string(""), // compression
        ...uint64LE(BigInt(0n)), // compressed size
        ...uint64LE(BigInt(0n)), // uncompressed size
      ]),
      ...record(Opcode.CHUNK_INDEX, [
        ...uint64LE(1n), // start time
        ...uint64LE(3n), // end time
        ...uint64LE(0n), // offset
        ...uint64LE(0n), // chunk length
        ...keyValues(uint16LE, uint64LE, []), // message index offsets
        ...uint64LE(0n), // message index length
        ...string(""), // compression
        ...uint64LE(BigInt(0n)), // compressed size
        ...uint64LE(BigInt(0n)), // uncompressed size
      ]),
      ...record(Opcode.FOOTER, [
        ...uint64LE(BigInt(summaryStart)), // summary offset
        ...uint64LE(0n), // summary start offset
        ...uint32LE(crc32(new Uint8Array(0))), // summary crc
      ]),
      ...MCAP0_MAGIC,
    );
    const reader = await Mcap0IndexedReader.Initialize({
      readable: makeReadable(new Uint8Array(data)),
    });
    await expect(collect(reader.readMessages())).rejects.toThrow(
      "Overlapping chunks are not currently supported",
    );
  });

  it.each<{ records: [bigint, bigint][]; shouldThrow: boolean }>([
    {
      records: [
        [0n, 0n],
        [0n, 0n],
        [0n, 0n],
      ],
      shouldThrow: false,
    },
    {
      records: [
        [0n, 0n],
        [1n, 0n],
        [1n, 0n],
      ],
      shouldThrow: false,
    },
    {
      records: [
        [0n, 0n],
        [2n, 0n],
        [1n, 0n],
      ],
      shouldThrow: true,
    },
  ])(
    "requires message index offsets to be in order of log time",
    async ({ records, shouldThrow }) => {
      const data = [
        ...MCAP0_MAGIC,
        ...record(Opcode.HEADER, [
          ...string(""), // profile
          ...string(""), // library
        ]),
      ];
      const chunkOffset = BigInt(data.length);
      data.push(
        ...record(Opcode.CHUNK, [
          ...uint64LE(0n), // start time
          ...uint64LE(0n), // end time
          ...uint64LE(0n), // decompressed size
          ...uint32LE(crc32(new Uint8Array([]))), // decompressed crc32
          ...string(""), // compression
          ...uint64LE(0n), // chunk data size
        ]),
      );
      const messageIndexOffset = BigInt(data.length);
      data.push(
        ...record(Opcode.MESSAGE_INDEX, [
          ...uint16LE(42), // channel id
          ...keyValues(uint64LE, uint64LE, records), // records
        ]),
      );
      const messageIndexLength = BigInt(data.length) - messageIndexOffset;
      const summaryStart = BigInt(data.length);
      data.push(
        ...record(Opcode.CHUNK_INDEX, [
          ...uint64LE(0n), // start time
          ...uint64LE(100n), // end time
          ...uint64LE(chunkOffset), // offset
          ...uint64LE(0n), // chunk length
          ...keyValues(uint16LE, uint64LE, [[42, messageIndexOffset]]), // message index offsets
          ...uint64LE(messageIndexLength), // message index length
          ...string(""), // compression
          ...uint64LE(BigInt(0n)), // compressed size
          ...uint64LE(BigInt(0n)), // uncompressed size
        ]),
        ...record(Opcode.FOOTER, [
          ...uint64LE(summaryStart), // summary offset
          ...uint64LE(0n), // summary start offset
          ...uint32LE(crc32(new Uint8Array(0))), // summary crc
        ]),
        ...MCAP0_MAGIC,
      );
      const reader = await Mcap0IndexedReader.Initialize({
        readable: makeReadable(new Uint8Array(data)),
      });
      if (shouldThrow) {
        // eslint-disable-next-line jest/no-conditional-expect
        await expect(collect(reader.readMessages())).rejects.toThrow(
          /Message index entries for channel 42 .+ must be sorted by log time/,
        );
      } else {
        // Still fails because messages are not actually present in the chunk
        // eslint-disable-next-line jest/no-conditional-expect
        await expect(collect(reader.readMessages())).rejects.toThrow(
          "Unable to parse record at offset",
        );
      }
    },
  );
});
