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
  crcSuffix,
  uint16LE,
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
              ...uint64LE(0n), // index offset
              ...uint32LE(0), // index crc
            ]),
            ...MCAP0_MAGIC,
          ]),
        ),
      }),
    ).rejects.toThrow("too small to be valid MCAP");

    await expect(
      Mcap0IndexedReader.Initialize({
        readable: makeReadable(
          new Uint8Array([
            ...MCAP0_MAGIC,
            ...record(Opcode.HEADER, [
              ...string(""), // profile
              ...string(""), // library
              ...keyValues(string, string, []), // metadata
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
          ...keyValues(string, string, []), // metadata
        ]),
        ...record(Opcode.FOOTER, [
          ...uint64LE(0n), // index offset
          ...uint32LE(0), // index crc
        ]),
        ...MCAP0_MAGIC,
      ]),
    );
    await expect(Mcap0IndexedReader.Initialize({ readable })).rejects.toThrow(
      "File is not indexed",
    );
  });

  it("parses file with empty index", async () => {
    const data = [
      ...MCAP0_MAGIC,
      ...record(Opcode.HEADER, [
        ...string(""), // profile
        ...string(""), // library
        ...keyValues(string, string, []), // metadata
      ]),
    ];
    const indexOffset = data.length;
    data.push(
      ...record(Opcode.FOOTER, [
        ...uint64LE(BigInt(indexOffset)), // index offset
        ...uint32LE(
          crc32(
            new Uint8Array([
              Opcode.FOOTER,
              ...uint64LE(/*index offset*/ 8n + /*index crc*/ 4n), //record length
              ...uint64LE(BigInt(indexOffset)), //index offset
            ]),
          ),
        ), // index crc
      ]),
      ...MCAP0_MAGIC,
    );
    const readable = makeReadable(new Uint8Array(data));
    const reader = await Mcap0IndexedReader.Initialize({ readable });
    await expect(collect(reader.readMessages())).resolves.toEqual([]);
    expect(readable.readCalls).toBe(2);
  });

  it("rejects invalid index crc", async () => {
    const data = [
      ...MCAP0_MAGIC,
      ...record(Opcode.HEADER, [
        ...string(""), // profile
        ...string(""), // library
        ...keyValues(string, string, []), // metadata
      ]),
    ];
    const indexOffset = data.length;
    data.push(
      ...record(Opcode.FOOTER, [
        ...uint64LE(BigInt(indexOffset)), // index offset
        ...uint32LE(crc32(new Uint8Array([42]))), // index crc
      ]),
      ...MCAP0_MAGIC,
    );
    const readable = makeReadable(new Uint8Array(data));
    await expect(Mcap0IndexedReader.Initialize({ readable })).rejects.toThrow(
      "Incorrect index CRC 1565496904 (expected 163128923)",
    );
  });

  it("parses index with channel info", async () => {
    const data = [
      ...MCAP0_MAGIC,
      ...record(Opcode.HEADER, [
        ...string(""), // profile
        ...string(""), // library
        ...keyValues(string, string, []), // metadata
      ]),
    ];
    const indexOffset = data.length;
    data.push(
      ...record(
        Opcode.CHANNEL_INFO,
        crcSuffix([
          ...uint16LE(42), // channel id
          ...string("mytopic"), // topic
          ...string("utf12"), // encoding
          ...string("some data"), // schema name
          ...string("stuff"), // schema
          ...keyValues(string, string, [["foo", "bar"]]), // user data
        ]),
      ),
      ...record(Opcode.FOOTER, [
        ...uint64LE(BigInt(indexOffset)), // index offset
        ...uint32LE(crc32(new Uint8Array(0))), // index crc
      ]),
      ...MCAP0_MAGIC,
    );
    const readable = makeReadable(new Uint8Array(data));
    const reader = await Mcap0IndexedReader.Initialize({ readable });
    await expect(collect(reader.readMessages())).resolves.toEqual([]);
    expect(reader.channelInfosById).toEqual(
      new Map<number, TypedMcapRecords["ChannelInfo"]>([
        [
          42,
          {
            type: "ChannelInfo",
            channelId: 42,
            schemaFormat: "myformat",
            schemaVersion: "version",
            topicName: "mytopic",
            messageEncoding: "utf12",
            schemaName: "some data",
            schema: "stuff",
            userData: [["foo", "bar"]],
          },
        ],
      ]),
    );
    expect(readable.readCalls).toBe(2);
  });

  describe("indexed with single channel", () => {
    const message1: TypedMcapRecords["Message"] = {
      type: "Message",
      channelId: 42,
      sequence: 1,
      publishTime: 0n,
      recordTime: 10n,
      messageData: new Uint8Array(),
    };
    const message2: TypedMcapRecords["Message"] = {
      type: "Message",
      channelId: 42,
      sequence: 2,
      publishTime: 1n,
      recordTime: 11n,
      messageData: new Uint8Array(),
    };
    const message3: TypedMcapRecords["Message"] = {
      type: "Message",
      channelId: 42,
      sequence: 3,
      publishTime: 2n,
      recordTime: 12n,
      messageData: new Uint8Array(),
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
        const channelInfo = record(
          Opcode.CHANNEL_INFO,
          crcSuffix([
            ...uint16LE(42), // channel id
            ...string("mytopic"), // topic
            ...string("utf12"), // encoding
            ...string("some data"), // schema name
            ...string("stuff"), // schema
            ...keyValues(string, string, [["foo", "bar"]]), // user data
          ]),
        );
        const message1Data = record(Opcode.MESSAGE, [
          ...uint16LE(message1.channelId), // channel id
          ...uint32LE(message1.sequence), // sequence
          ...uint64LE(message1.publishTime), // publish time
          ...uint64LE(message1.recordTime), // record time
        ]);
        const message2Data = record(Opcode.MESSAGE, [
          ...uint16LE(message2.channelId), // channel id
          ...uint32LE(message2.sequence), // sequence
          ...uint64LE(message2.publishTime), // publish time
          ...uint64LE(message2.recordTime), // record time
        ]);
        const message3Data = record(Opcode.MESSAGE, [
          ...uint16LE(message3.channelId), // channel id
          ...uint32LE(message3.sequence), // sequence
          ...uint64LE(message3.publishTime), // publish time
          ...uint64LE(message3.recordTime), // record time
        ]);
        const chunkContents = [...channelInfo];
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
            ...keyValues(string, string, []), // metadata
          ]),
        ];
        const chunkOffset = BigInt(data.length);
        data.push(
          ...record(Opcode.CHUNK, [
            ...uint64LE(0n), // decompressed size
            ...uint32LE(crc32(new Uint8Array(chunkContents))), // decompressed crc32
            ...string(""), // compression
            ...chunkContents,
          ]),
        );
        const messageIndexOffset = BigInt(data.length);
        data.push(
          ...record(
            Opcode.MESSAGE_INDEX,
            crcSuffix([
              ...uint16LE(42), // channel id
              ...uint32LE(1), // count
              ...keyValues(uint64LE, uint64LE, [
                [message1.recordTime, message1Offset],
                [message2.recordTime, message2Offset],
                [message3.recordTime, message3Offset],
              ]), // records
            ]),
          ),
        );
        const messageIndexLength = BigInt(data.length) - messageIndexOffset;
        const indexOffset = data.length;
        data.push(
          ...channelInfo,
          ...record(
            Opcode.CHUNK_INDEX,
            crcSuffix([
              ...uint64LE(message1.recordTime), // start time
              ...uint64LE(message3.recordTime), // end time
              ...uint64LE(chunkOffset), // offset
              ...keyValues(uint16LE, uint64LE, [[42, messageIndexOffset]]), // message index offsets
              ...uint64LE(messageIndexLength), // message index length
              ...string(""), // compression
              ...uint64LE(BigInt(chunkContents.length)), // compressed size
              ...uint64LE(BigInt(chunkContents.length)), // uncompressed size
            ]),
          ),
          ...record(Opcode.FOOTER, [
            ...uint64LE(BigInt(indexOffset)), // index offset
            ...uint32LE(crc32(new Uint8Array(0))), // index crc
          ]),
          ...MCAP0_MAGIC,
        );
        const readable = makeReadable(new Uint8Array(data));
        const reader = await Mcap0IndexedReader.Initialize({ readable });
        await expect(collect(reader.readMessages({ startTime, endTime }))).resolves.toEqual(
          expected,
        );
        expect(readable.readCalls).toBe(4);
      },
    );
  });

  it("does not yet support overlapping chunks", async () => {
    const data = [
      ...MCAP0_MAGIC,
      ...record(Opcode.HEADER, [
        ...string(""), // profile
        ...string(""), // library
        ...keyValues(string, string, []), // metadata
      ]),
    ];
    const indexOffset = BigInt(data.length);
    data.push(
      ...record(
        Opcode.CHUNK_INDEX,
        crcSuffix([
          ...uint64LE(0n), // start time
          ...uint64LE(2n), // end time
          ...uint64LE(0n), // offset
          ...keyValues(uint16LE, uint64LE, []), // message index offsets
          ...uint64LE(0n), // message index length
          ...string(""), // compression
          ...uint64LE(BigInt(0n)), // compressed size
          ...uint64LE(BigInt(0n)), // uncompressed size
        ]),
      ),
      ...record(
        Opcode.CHUNK_INDEX,
        crcSuffix([
          ...uint64LE(1n), // start time
          ...uint64LE(3n), // end time
          ...uint64LE(0n), // offset
          ...keyValues(uint16LE, uint64LE, []), // message index offsets
          ...uint64LE(0n), // message index length
          ...string(""), // compression
          ...uint64LE(BigInt(0n)), // compressed size
          ...uint64LE(BigInt(0n)), // uncompressed size
        ]),
      ),
      ...record(Opcode.FOOTER, [
        ...uint64LE(BigInt(indexOffset)), // index offset
        ...uint32LE(crc32(new Uint8Array(0))), // index crc
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
    "requires message index offsets to be in order of recordTime",
    async ({ records, shouldThrow }) => {
      const data = [
        ...MCAP0_MAGIC,
        ...record(Opcode.HEADER, [
          ...string(""), // profile
          ...string(""), // library
          ...keyValues(string, string, []), // metadata
        ]),
      ];
      const chunkOffset = BigInt(data.length);
      data.push(
        ...record(Opcode.CHUNK, [
          ...uint64LE(0n), // decompressed size
          ...uint32LE(crc32(new Uint8Array([]))), // decompressed crc32
          ...string(""), // compression
        ]),
      );
      const messageIndexOffset = BigInt(data.length);
      data.push(
        ...record(
          Opcode.MESSAGE_INDEX,
          crcSuffix([
            ...uint16LE(42), // channel id
            ...uint32LE(1), // count
            ...keyValues(uint64LE, uint64LE, records), // records
          ]),
        ),
      );
      const messageIndexLength = BigInt(data.length) - messageIndexOffset;
      const indexOffset = BigInt(data.length);
      data.push(
        ...record(
          Opcode.CHUNK_INDEX,
          crcSuffix([
            ...uint64LE(0n), // start time
            ...uint64LE(100n), // end time
            ...uint64LE(chunkOffset), // offset
            ...keyValues(uint16LE, uint64LE, [[42, messageIndexOffset]]), // message index offsets
            ...uint64LE(messageIndexLength), // message index length
            ...string(""), // compression
            ...uint64LE(BigInt(0n)), // compressed size
            ...uint64LE(BigInt(0n)), // uncompressed size
          ]),
        ),
        ...record(Opcode.FOOTER, [
          ...uint64LE(indexOffset), // index offset
          ...uint32LE(crc32(new Uint8Array(0))), // index crc
        ]),
        ...MCAP0_MAGIC,
      );
      const reader = await Mcap0IndexedReader.Initialize({
        readable: makeReadable(new Uint8Array(data)),
      });
      if (shouldThrow) {
        // eslint-disable-next-line jest/no-conditional-expect
        await expect(collect(reader.readMessages())).rejects.toThrow(
          /Message index entries for channel 42 .+ must be sorted by recordTime/,
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
