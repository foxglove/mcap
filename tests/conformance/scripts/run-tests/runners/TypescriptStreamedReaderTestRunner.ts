import { Mcap0StreamReader } from "@foxglove/mcap";
import fs from "fs/promises";
import { TestVariant } from "variants/types";

import ITestRunner from "./ITestRunner";
import { stringifyRecords } from "./stringifyRecords";

export default class TypescriptStreamedReaderTestRunner implements ITestRunner {
  readonly name = "ts-streamed-reader";
  readonly mode = "read";

  supportsVariant(_variant: TestVariant): boolean {
    return true;
  }

  async run(filePath: string, variant: TestVariant): Promise<string> {
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
