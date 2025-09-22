# MCAP reader/writer conformance test suite

A writer test is specified as a JSON input spec file paired with an expected MCAP output file. Given the input, a conforming writer implementation should output a byte-identical MCAP file.

The input spec consists of one object of type `TestData`, described below in TypeScript:

```ts
interface TestData {
  records: Record[];
  meta: {
    variants: TestFeatures[];
  };
}

interface Record {
  type: RecordType;
  fields: [string, string][]; // An array of [key, value] pairs.
}

type RecordType =
  | "Header"
  | "Footer"
  | "Schema"
  | "Channel"
  | "Message"
  | "Chunk"
  | "MessageIndex"
  | "ChunkIndex"
  | "Attachment"
  | "AttachmentIndex"
  | "Statistics"
  | "Metadata"
  | "MetadataIndex"
  | "SummaryOffset"
  | "DataEnd";

type TestFeatures =
  | "ch" // UseChunks
  | "mx" // UseMessageIndex
  | "st" // UseStatistics
  | "rsh" // UseRepeatedSchemas
  | "rch" // UseRepeatedChannelInfos
  | "ax" // UseAttachmentIndex
  | "mdx" // UseMetadataIndex
  | "chx" // UseChunkIndex
  | "sum" // UseSummaryOffset
  | "pad"; // AddExtraDataToRecords
```
