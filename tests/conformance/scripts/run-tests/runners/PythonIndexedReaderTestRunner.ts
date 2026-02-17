import { exec } from "node:child_process";
import { promisify } from "node:util";

import { IndexedReadTestRunner } from "./TestRunner.ts";
import { TestFeatures } from "../../../variants/types.ts";
import type { TestVariant } from "../../../variants/types.ts";
import type { IndexedReadTestResult } from "../types.ts";

export default class PythonIndexedReaderTestRunner extends IndexedReadTestRunner {
  readonly name = "py-indexed-reader";

  async runReadTest(filePath: string): Promise<IndexedReadTestResult> {
    const { stdout } = await promisify(exec)(
      `python3 tests/run_reader_test.py ${filePath} indexed`,
      {
        cwd: "../../python/mcap",
      },
    );
    return JSON.parse(stdout.trim()) as IndexedReadTestResult;
  }
  supportsVariant({ records, features }: TestVariant): boolean {
    if (!records.some((record) => record.type === "Message")) {
      return false;
    }
    if (!features.has(TestFeatures.UseChunks)) {
      return false;
    }
    if (!features.has(TestFeatures.UseChunkIndex)) {
      return false;
    }
    if (!features.has(TestFeatures.UseRepeatedChannelInfos)) {
      return false;
    }
    if (!features.has(TestFeatures.UseRepeatedSchemas)) {
      return false;
    }
    if (!features.has(TestFeatures.UseMessageIndex)) {
      return false;
    }
    return true;
  }
}
