import { Mcap0StreamReader } from "@mcap/core";
import fs from "fs/promises";
import { TestVariant } from "variants/types";

import { ReadTestRunner } from "./TestRunner";
import { stringifyRecords } from "./stringifyRecords";

export default class TypescriptStreamedReaderTestRunner extends ReadTestRunner {
  readonly name = "ts-streamed-reader";
  readonly readsDataEnd = true;

  supportsVariant(_variant: TestVariant): boolean {
    return true;
  }

  async runReadTest(filePath: string, variant: TestVariant): Promise<string> {
    const result = [];
    const reader = new Mcap0StreamReader({ validateCrcs: true });
    reader.append(await fs.readFile(filePath));
    let record;
    while ((record = reader.nextRecord())) {
      if (record.type === "MessageIndex") {
        continue;
      }
      result.push(record);
    }
    if (!reader.done()) {
      throw new Error("Reader not done");
    }

    return stringifyRecords(result, variant);
  }
}
