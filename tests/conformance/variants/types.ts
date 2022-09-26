import { McapTypes } from "@mcap/core";

export type TestDataRecord = McapTypes.TypedMcapRecords[
  | "Message"
  | "Schema"
  | "Channel"
  | "Attachment"
  | "Metadata"];

export enum TestFeatures {
  UseChunks = "ch",
  UseMessageIndex = "mx",
  UseStatistics = "st",
  UseRepeatedSchemas = "rsh",
  UseRepeatedChannelInfos = "rch",
  UseAttachmentIndex = "ax",
  UseMetadataIndex = "mdx",
  UseChunkIndex = "chx",
  UseSummaryOffset = "sum",
  AddExtraDataToRecords = "pad",
}

export type TestInput = {
  baseName: string;
  records: TestDataRecord[];
};

export type TestVariant = TestInput & {
  name: string;
  features: Set<TestFeatures>;
};
