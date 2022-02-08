import { Mcap0StreamReader, Mcap0Types } from "@foxglove/mcap";
import fs from "fs/promises";

import ITestRunner from "./ITestRunner";

function stringifyRecord(record: Mcap0Types.TypedMcapRecord): string {
  function camelToSnake(str: string): string {
    return str.replace(/[A-Z]/g, (letter) => `_${letter.toLowerCase()}`);
  }

  function typePrefix(value: unknown): string {
    switch (typeof value) {
      case "bigint":
        return "n";
      case "number":
        return "i";
      case "string":
        return "s";
      case "object":
        if (Array.isArray(value)) {
          return "a";
        } else if (value instanceof Uint8Array) {
          return "b";
        } else {
          return "m";
        }
      default:
        throw new Error(`Unknown value type: ${typeof value}.`);
    }
  }

  function stringifyKeyValuePairs(pairs: [unknown, unknown][]): string {
    return (
      "{" +
      pairs
        .sort(([nameA], [nameB]) => String(nameA).localeCompare(String(nameB)))
        .map(([k, v]) => [stringifyTypedValue(k), stringifyTypedValue(v)].join("="))
        .join(",") +
      "}"
    );
  }

  function stringifyTypedValue(value: unknown): string {
    return [typePrefix(value), stringifyValue(value)].join(":");
  }

  function stringifyData(data: Uint8Array): string {
    return `<${Array.from(data, (value) => value.toString(16).padStart(2, "0")).join("")}>`;
  }

  function stringifyValue(value: unknown): string {
    if (value instanceof Uint8Array) {
      return stringifyData(value);
    }
    if (Array.isArray(value)) {
      return (
        "{" +
        value
          .map((item) => {
            if (!Array.isArray(item) || item.length !== 2) {
              throw new Error("Invalid array item, expected tuple of length 2");
            }
            return `${stringifyTypedValue(item[0])}=${stringifyTypedValue(item[1])}`;
          })
          .join(" ") +
        "}"
      );
    }
    if (value instanceof Map) {
      return stringifyKeyValuePairs(Array.from(value.entries()));
    }
    switch (typeof value) {
      case "string":
      case "bigint":
      case "number":
        return value === "" ? `""` : String(value);
      default:
        throw new Error(`Cannot stringify ${typeof value}: ${String(value)}`);
    }
  }
  function stringifyFields(fields: [unknown, unknown][]): string {
    return fields
      .sort(([nameA], [nameB]) => String(nameA).localeCompare(String(nameB)))
      .map(([name, value]) => `${camelToSnake(String(name))}=${stringifyTypedValue(value)}`)
      .join(" ");
  }
  const fields = Object.entries(record)
    .filter(([k]) => k !== "type")
    .sort(([k1], [k2]) => k1.localeCompare(k2));
  return `${record.type} ${stringifyFields(fields)}`;
}

export default class TypescriptStreamedTestRunner implements ITestRunner {
  name = "ts-stream";
  async run(filePath: string): Promise<string[]> {
    const result = [];
    const reader = new Mcap0StreamReader({ validateCrcs: true });
    reader.append(await fs.readFile(filePath));
    let record;
    while ((record = reader.nextRecord())) {
      result.push(stringifyRecord(record));
    }
    if (!reader.done()) {
      throw new Error("Reader not done");
    }
    return result;
  }
}
