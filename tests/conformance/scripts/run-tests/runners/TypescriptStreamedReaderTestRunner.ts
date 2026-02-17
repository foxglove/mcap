import { McapStreamReader } from "@mcap/core";
import fs from "node:fs/promises";
import type { TestVariant } from "../../../variants/types.ts";

import { StreamedReadTestRunner } from "./TestRunner.ts";
import { toSerializableMcapRecord } from "../toSerializableMcapRecord.ts";
import type { StreamedReadTestResult } from "../types.ts";

export default class TypescriptStreamedReaderTestRunner extends StreamedReadTestRunner {
  readonly name = "ts-streamed-reader";
  readonly readsDataEnd = true;

  supportsVariant(_variant: TestVariant): boolean {
    return true;
  }

  async runReadTest(filePath: string): Promise<StreamedReadTestResult> {
    const result = [];
    const reader = new McapStreamReader({ validateCrcs: true });
    reader.append(new Uint8Array(await fs.readFile(filePath)));
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
