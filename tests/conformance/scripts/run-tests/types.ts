import { TestFeatures } from "../../variants/types";

export type IndexedReadTestResult = {
  schemas: SerializableMcapRecord[];
  channels: SerializableMcapRecord[];
  messages: SerializableMcapRecord[];
  statistics: SerializableMcapRecord[];
};

export type StreamedReadTestResult = {
  records: SerializableMcapRecord[];
};

export type SerializableMcapRecord = {
  type: string;
  fields: [string, string | string[] | Record<string, string>][];
};

export type TestCase = {
  records: SerializableMcapRecord[];
  meta?: { variant: { features: TestFeatures[] } };
};
