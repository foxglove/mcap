import type { TestDataRecord, TestInput } from "./types.ts";

const tenMessages: TestDataRecord[] = [0n, 2n, 1n, 3n, 3n, 5n, 4n, 7n, 8n, 9n].map(
  (logTime, sequence) => {
    return {
      type: "Message",
      channelId: 1,
      publishTime: logTime,
      logTime,
      data: new Uint8Array([1, 2, 3]),
      sequence,
    };
  },
);

const inputs: TestInput[] = [
  { baseName: "NoData", records: [] },
  {
    baseName: "OneSchemalessMessage",
    records: [
      {
        type: "Channel",
        id: 1,
        topic: "example",
        schemaId: 0,
        messageEncoding: "text",
        metadata: new Map([]),
      },
      {
        type: "Message",
        channelId: 1,
        publishTime: 1n,
        logTime: 2n,
        data: new Uint8Array([1, 2, 3]),
        sequence: 10,
      },
    ],
  },
  {
    baseName: "OneMessage",
    records: [
      {
        type: "Schema",
        id: 1,
        name: "Example",
        encoding: "c",
        data: new Uint8Array([4, 5, 6]),
      },
      {
        type: "Channel",
        id: 1,
        topic: "example",
        schemaId: 1,
        messageEncoding: "a",
        metadata: new Map([["foo", "bar"]]),
      },
      {
        type: "Message",
        channelId: 1,
        publishTime: 1n,
        logTime: 2n,
        data: new Uint8Array([1, 2, 3]),
        sequence: 10,
      },
    ],
  },
  {
    baseName: "OneAttachment",
    records: [
      {
        type: "Attachment",
        name: "myFile",
        mediaType: "application/octet-stream",
        logTime: 2n,
        createTime: 1n,
        data: new Uint8Array([1, 2, 3]),
      },
    ],
  },
  {
    baseName: "OneMetadata",
    records: [{ type: "Metadata", name: "myMetadata", metadata: new Map([["foo", "bar"]]) }],
  },
  {
    baseName: "TenMessages",
    records: [
      {
        type: "Schema",
        id: 1,
        name: "Example",
        encoding: "c",
        data: new Uint8Array([4, 5, 6]),
      },
      {
        type: "Channel",
        id: 1,
        topic: "example",
        schemaId: 1,
        messageEncoding: "a",
        metadata: new Map([["foo", "bar"]]),
      },
      ...tenMessages,
    ],
  },
];

export default inputs;
