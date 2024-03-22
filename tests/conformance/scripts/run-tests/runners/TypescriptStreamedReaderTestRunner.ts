import { McapStreamReader } from "@mcap/core";
import fs from "fs/promises";
import { TestVariant } from "variants/types";

import { StreamedReadTestRunner } from "./TestRunner";
import { toSerializableMcapRecord } from "../toSerializableMcapRecord";
import { StreamedReadTestResult } from "../types";

export default class TypescriptStreamedReaderTestRunner extends StreamedReadTestRunner {
  readonly name = "ts-streamed-reader";
  readonly readsDataEnd = true;
  readonly sortsMessages = false;

  supportsVariant(_variant: TestVariant): boolean {
    return true;
  }

  async runReadTest(filePath: string): Promise<StreamedReadTestResult> {
    const result = [];
    const reader = new McapStreamReader({ validateCrcs: true });
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

    return { records: result.map(toSerializableMcapRecord) };
  }
}
