import { TestInput } from "./types";

const inputs: TestInput[] = [
  { baseName: "NoData", records: [] },
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
        contentType: "application/octet-stream",
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
];

export default inputs;
