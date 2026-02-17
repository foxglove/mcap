import type { TypedMcapRecords } from "@mcap/core";

export type TestDataRecord = TypedMcapRecords[
  | "Message"
  | "Schema"
  | "Channel"
  | "Attachment"
  | "Metadata"];

export const TestFeatures = {
  UseChunks: "ch",
  UseMessageIndex: "mx",
  UseStatistics: "st",
  UseRepeatedSchemas: "rsh",
  UseRepeatedChannelInfos: "rch",
  UseAttachmentIndex: "ax",
  UseMetadataIndex: "mdx",
  UseChunkIndex: "chx",
  UseSummaryOffset: "sum",
  AddExtraDataToRecords: "pad",
} as const;

export type TestFeature = (typeof TestFeatures)[keyof typeof TestFeatures];

export type TestInput = {
  baseName: string;
  records: TestDataRecord[];
};

export type TestVariant = TestInput & {
  name: string;
  features: Set<TestFeature>;
};
