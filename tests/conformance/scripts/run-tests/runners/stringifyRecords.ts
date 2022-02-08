import { Mcap0Types } from "@foxglove/mcap";
import stringify from "json-stringify-pretty-compact";
import { chain, snakeCase } from "lodash";

function replacer(_key: string, value: unknown): unknown {
  if (value instanceof Uint8Array) {
    return Array.from(value);
  }

  if (value instanceof Map) {
    return Object.fromEntries(value);
  }

  if (typeof value === "bigint") {
    return Number(value);
  }

  return value;
}

function normalizeRecord(record: Mcap0Types.TypedMcapRecord): { type: string; fields: unknown } {
  return chain(record)
    .toPairs()
    .filter(([k]) => k !== "type")
    .map(([k, v]) => [snakeCase(k), v])
    .sortBy((p) => p[0])
    .thru((p) => ({ type: record.type, fields: p }))
    .value();
}

export function stringifyRecords(records: Mcap0Types.TypedMcapRecord[]): string {
  return stringify(records.map(normalizeRecord), { replacer });
}
