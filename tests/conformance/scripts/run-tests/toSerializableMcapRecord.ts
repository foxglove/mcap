import type { McapTypes } from "@mcap/core";
import { chain, snakeCase } from "lodash";

import type { SerializableMcapRecord } from "./types.ts";

function replacer(_key: string, value: unknown): unknown {
  if (value instanceof Uint8Array) {
    return Array.from(value);
  }

  if (value instanceof Map) {
    return Object.fromEntries(value);
  }

  if (typeof value === "bigint" || typeof value === "number") {
    return String(value);
  }

  return value;
}

function normalizeRecord(record: McapTypes.TypedMcapRecord) {
  return chain(record)
    .toPairs()
    .filter(([k]) => k !== "type")
    .map(([k, v]) => [snakeCase(k), v])
    .sortBy((p) => p[0])
    .thru((p) => ({ type: record.type, fields: p }))
    .value();
}

export function toSerializableMcapRecord(
  record: McapTypes.TypedMcapRecord,
): SerializableMcapRecord {
  return JSON.parse(JSON.stringify(normalizeRecord(record), replacer)) as SerializableMcapRecord;
}
