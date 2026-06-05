import { McapStreamReader } from "@mcap/core";
import type { TypedMcapRecord } from "@mcap/core";
import { loadDecompressHandlers } from "@mcap/support";

import { compareStableStrings, stableStringify } from "./stableJson.ts";

type SerializableValue =
  | string
  | number
  | boolean
  | null
  | SerializableValue[]
  | { [key: string]: SerializableValue };

type SerializableRecord = {
  type: string;
  fields: [string, SerializableValue][];
};

type MessageRecord = {
  topic: string;
  messageEncoding: string;
  schema:
    | {
        id: string;
        name: string;
        encoding: string;
        data: number[];
      }
    | undefined;
  sequence: string;
  logTime: string;
  publishTime: string;
  data: number[];
};

type ContentRecord = {
  messages: MessageRecord[];
  metadata: {
    name: string;
    metadata: Record<string, string>;
  }[];
  attachments: {
    name: string;
    mediaType: string;
    logTime: string;
    createTime: string;
    data: number[];
  }[];
};

type CompareResult = {
  equal: boolean;
  mode: "byte-exact" | "semantic";
  expected: string;
  actual: string;
};

export type McapSummaryExpectation = {
  profile?: string;
  messageCount?: number;
  channelCount?: number;
  schemaCount?: number;
  metadata?: Array<{ name: string; values: Record<string, string> }>;
};

let decompressHandlersPromise: ReturnType<typeof loadDecompressHandlers> | undefined;

async function getDecompressHandlers(): ReturnType<typeof loadDecompressHandlers> {
  return await (decompressHandlersPromise ??= loadDecompressHandlers());
}

export async function compareMcapBuffers(
  expected: Buffer,
  actual: Buffer,
  options: { mode: "records" | "messages" | "content"; allowSemanticFallback: boolean },
): Promise<CompareResult> {
  if (expected.equals(actual)) {
    return { equal: true, mode: "byte-exact", expected: "", actual: "" };
  }

  const [expectedParse, actualParse] = await Promise.all([
    parseRecords("go", expected),
    parseRecords("rust", actual),
  ]);
  if ("error" in expectedParse || "error" in actualParse) {
    const expectedError = "error" in expectedParse ? expectedParse.error : "<parsed>";
    const actualError = "error" in actualParse ? actualParse.error : "<parsed>";
    return {
      equal: false,
      mode: options.allowSemanticFallback ? "semantic" : "byte-exact",
      expected: byteExactDiagnostic(expected.length, expectedError),
      actual: byteExactDiagnostic(actual.length, actualError),
    };
  }

  const expectedSemantic = canonicalString(semanticValue(expectedParse.records, options.mode));
  const actualSemantic = canonicalString(semanticValue(actualParse.records, options.mode));

  if (!options.allowSemanticFallback) {
    const semanticStatus =
      expectedSemantic === actualSemantic
        ? "semantic comparison matched"
        : `semantic diff:\n${expectedSemantic}`;
    return {
      equal: false,
      mode: "byte-exact",
      expected: byteExactDiagnostic(expected.length, semanticStatus),
      actual: byteExactDiagnostic(
        actual.length,
        expectedSemantic === actualSemantic
          ? "semantic comparison matched"
          : `semantic diff:\n${actualSemantic}`,
      ),
    };
  }

  return {
    equal: expectedSemantic === actualSemantic,
    mode: "semantic",
    expected: expectedSemantic,
    actual: actualSemantic,
  };
}

export async function compareMcapSummary(
  data: Buffer,
  expected: McapSummaryExpectation,
): Promise<string[]> {
  const parsed = await parseRecords("expected", data);
  if ("error" in parsed) {
    return [parsed.error];
  }

  const actual = summarizeRecords(parsed.records);
  const messages: string[] = [];
  if (expected.profile != undefined && actual.profile !== expected.profile) {
    messages.push(`expected MCAP profile ${expected.profile}, got ${actual.profile}`);
  }
  if (expected.messageCount != undefined && actual.messageCount !== expected.messageCount) {
    messages.push(`expected ${expected.messageCount} messages, got ${actual.messageCount}`);
  }
  if (expected.channelCount != undefined && actual.channelCount !== expected.channelCount) {
    messages.push(`expected ${expected.channelCount} channels, got ${actual.channelCount}`);
  }
  if (expected.schemaCount != undefined && actual.schemaCount !== expected.schemaCount) {
    messages.push(`expected ${expected.schemaCount} schemas, got ${actual.schemaCount}`);
  }
  for (const expectedEntry of expected.metadata ?? []) {
    const matching = actual.metadata.filter((entry) => entry.name === expectedEntry.name);
    if (matching.length === 0) {
      messages.push(`expected a metadata record named ${expectedEntry.name}, found none`);
      continue;
    }
    // Merge records sharing a name in file order (later values win), matching `get metadata`.
    const mergedValues = Object.assign({}, ...matching.map((entry) => entry.values)) as Record<
      string,
      string
    >;
    const expectedValues = stableStringify(expectedEntry.values);
    const actualValues = stableStringify(mergedValues);
    if (expectedValues !== actualValues) {
      messages.push(
        `expected metadata ${expectedEntry.name} values ${expectedValues}, got ${actualValues}`,
      );
    }
  }
  return messages;
}

function byteExactDiagnostic(byteLength: number, details: string): string {
  return `<${byteLength} bytes>\n${details}`;
}

function summarizeRecords(records: TypedMcapRecord[]): Required<McapSummaryExpectation> {
  let profile = "";
  let messageCount = 0;
  const channels = new Set<number>();
  const schemas = new Set<number>();
  const metadata: Array<{ name: string; values: Record<string, string> }> = [];

  for (const record of records) {
    const normalizedRecord = record as unknown as Record<string, unknown>;
    switch (record.type) {
      case "Header":
        profile = stringField(normalizedRecord, "profile");
        break;
      case "Message":
        messageCount++;
        break;
      case "Channel":
        channels.add(Number(normalizedRecord.id));
        break;
      case "Schema":
        schemas.add(Number(normalizedRecord.id));
        break;
      case "Metadata":
        metadata.push({
          name: stringField(normalizedRecord, "name"),
          values: stringMapField(normalizedRecord, "metadata"),
        });
        break;
      default:
        break;
    }
  }

  return {
    profile,
    messageCount,
    channelCount: channels.size,
    schemaCount: schemas.size,
    metadata,
  };
}

function semanticValue(
  records: TypedMcapRecord[],
  mode: "records" | "messages" | "content",
): unknown {
  switch (mode) {
    case "messages":
      return messagesFromRecords(records);
    case "content":
      return contentFromRecords(records);
    case "records":
      return records.map(toSerializableRecord);
  }
}

async function parseRecords(
  label: string,
  data: Uint8Array,
): Promise<{ records: TypedMcapRecord[] } | { error: string }> {
  try {
    const reader = new McapStreamReader({
      decompressHandlers: await getDecompressHandlers(),
      validateCrcs: true,
    });
    reader.append(data);
    const records: TypedMcapRecord[] = [];
    for (let record; (record = reader.nextRecord()) != undefined; ) {
      records.push(record);
    }
    return { records };
  } catch (error) {
    return {
      error: `${label} MCAP parse failed: ${
        error instanceof Error ? error.message : String(error)
      }`,
    };
  }
}

function messagesFromRecords(records: TypedMcapRecord[]): MessageRecord[] {
  const channels = new Map<number, Record<string, unknown>>();
  const schemas = new Map<number, Record<string, unknown>>();
  const messages: MessageRecord[] = [];

  for (const record of records) {
    const normalizedRecord = record as unknown as Record<string, unknown>;
    switch (record.type) {
      case "Channel":
        channels.set(Number(normalizedRecord.id), normalizedRecord);
        break;
      case "Schema":
        schemas.set(Number(normalizedRecord.id), normalizedRecord);
        break;
      case "Message": {
        const channelId = Number(normalizedRecord.channelId);
        const channel = channels.get(channelId);
        const schemaId = Number(channel?.schemaId ?? 0);
        const schema = schemas.get(schemaId);
        messages.push({
          topic: stringField(channel, "topic"),
          messageEncoding: stringField(channel, "messageEncoding"),
          schema:
            schema == undefined
              ? undefined
              : {
                  id: stringField(schema, "id"),
                  name: stringField(schema, "name"),
                  encoding: stringField(schema, "encoding"),
                  data: bytesField(schema, "data"),
                },
          sequence: stringField(normalizedRecord, "sequence"),
          logTime: stringField(normalizedRecord, "logTime"),
          publishTime: stringField(normalizedRecord, "publishTime"),
          data: bytesField(normalizedRecord, "data"),
        });
        break;
      }
      default:
        break;
    }
  }

  return messages;
}

function contentFromRecords(records: TypedMcapRecord[]): ContentRecord {
  const metadata: ContentRecord["metadata"] = [];
  const attachments: ContentRecord["attachments"] = [];

  for (const record of records) {
    const normalizedRecord = record as unknown as Record<string, unknown>;
    switch (record.type) {
      case "Metadata":
        metadata.push({
          name: stringField(normalizedRecord, "name"),
          metadata: stringMapField(normalizedRecord, "metadata"),
        });
        break;
      case "Attachment":
        attachments.push({
          name: stringField(normalizedRecord, "name"),
          mediaType: stringField(normalizedRecord, "mediaType"),
          logTime: stringField(normalizedRecord, "logTime"),
          createTime: stringField(normalizedRecord, "createTime"),
          data: bytesField(normalizedRecord, "data"),
        });
        break;
      default:
        break;
    }
  }

  metadata.sort((left, right) => compareStableStrings(`${left.name}`, `${right.name}`));
  attachments.sort((left, right) =>
    compareStableStrings(`${left.name}:${left.logTime}`, `${right.name}:${right.logTime}`),
  );

  return {
    messages: messagesFromRecords(records),
    metadata,
    attachments,
  };
}

function stringField(record: Record<string, unknown> | undefined, field: string): string {
  if (record == undefined) {
    return "";
  }
  const value = record[field];
  if (typeof value === "bigint" || typeof value === "number" || typeof value === "string") {
    return value.toString();
  }
  return "";
}

function bytesField(record: Record<string, unknown>, field: string): number[] {
  const value = record[field];
  if (value instanceof Uint8Array) {
    return Array.from(value);
  }
  return [];
}

function stringMapField(record: Record<string, unknown>, field: string): Record<string, string> {
  const value = record[field];
  if (value instanceof Map) {
    return Object.fromEntries(
      Array.from(value.entries()).map(([key, entryValue]) => [String(key), String(entryValue)]),
    );
  }
  if (typeof value === "object" && value != undefined) {
    return Object.fromEntries(
      Object.entries(value).map(([key, entryValue]) => [key, String(entryValue)]),
    );
  }
  return {};
}

function toSerializableRecord(record: TypedMcapRecord): SerializableRecord {
  const entries = Object.entries(record as unknown as Record<string, unknown>)
    .filter(([key]) => key !== "type")
    .map(
      ([key, value]) =>
        [toSnakeCase(key), toSerializableValue(value)] as [string, SerializableValue],
    )
    .sort(([left], [right]) => compareStableStrings(left, right));

  return { type: record.type, fields: entries };
}

function toSerializableValue(value: unknown): SerializableValue {
  if (value == undefined) {
    return null;
  }
  if (typeof value === "string" || typeof value === "boolean") {
    return value;
  }
  if (typeof value === "number" || typeof value === "bigint") {
    return value.toString();
  }
  if (value instanceof Uint8Array) {
    return Array.from(value);
  }
  if (value instanceof Map) {
    return Object.fromEntries(
      Array.from(value.entries()).map(([key, entryValue]) => [
        String(key),
        toSerializableValue(entryValue),
      ]),
    );
  }
  if (Array.isArray(value)) {
    return value.map(toSerializableValue);
  }
  if (typeof value === "object") {
    return Object.fromEntries(
      Object.entries(value).map(([key, entryValue]) => [key, toSerializableValue(entryValue)]),
    );
  }
  return String(value);
}

function canonicalString(value: unknown): string {
  return stableStringify(value);
}

function toSnakeCase(value: string): string {
  return value.replaceAll(/([a-z0-9])([A-Z])/g, "$1_$2").toLowerCase();
}
