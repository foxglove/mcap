import type { TestFeature } from "../../variants/types.ts";

export type SerializableMcapRecord = {
  type: string;
  fields: [string, string | string[] | Record<string, string>][];
};

export type IndexedReadTestResult = {
  schemas: SerializableMcapRecord[];
  channels: SerializableMcapRecord[];
  messages: SerializableMcapRecord[];
  statistics: SerializableMcapRecord[];
};

export type StreamedReadTestResult = {
  records: SerializableMcapRecord[];
};

export type TestCase = {
  records: SerializableMcapRecord[];
  meta?: { variant: { features: TestFeature[] } };
};
