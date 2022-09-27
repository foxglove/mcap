import { McapTypes } from "@mcap/core";
import stringify from "json-stringify-pretty-compact";
import { chain, snakeCase } from "lodash";
import { TestVariant } from "variants/types";

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

export function stringifyRecords(
  records: McapTypes.TypedMcapRecord[],
  variant: TestVariant,
): string {
  const normalizedRecords = records.map(normalizeRecord);
  const features = Array.from(variant.features.values());
  return (
    stringify(
      {
        records: normalizedRecords,
        meta: { variant: { features } },
      },
      { replacer },
    ) + "\n"
  );
}
