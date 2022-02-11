import { Mcap0StreamReader } from "@foxglove/mcap";
import fs from "fs/promises";
import { TestVariant } from "variants/types";

import { ReadTestRunner } from "./TestRunner";
import { stringifyRecords } from "./stringifyRecords";

export default class TypescriptStreamedReaderTestRunner extends ReadTestRunner {
  name = "ts-streamed-reader";

  supportsVariant(_variant: TestVariant): boolean {
    return true;
  }

  async runReadTest(filePath: string, variant: TestVariant): Promise<string> {
    const result = [];
    const reader = new Mcap0StreamReader({ validateCrcs: true });
    reader.append(await fs.readFile(filePath));
    let record;
    while ((record = reader.nextRecord())) {
      result.push(record);
    }
    if (!reader.done()) {
      throw new Error("Reader not done");
    }

    return stringifyRecords(result, variant);
  }
}
