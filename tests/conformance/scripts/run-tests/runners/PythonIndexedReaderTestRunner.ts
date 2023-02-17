import { exec } from "child_process";
import { promisify } from "util";
import { TestVariant, TestFeatures } from "variants/types";

import { IndexedReadTestRunner } from "./TestRunner";
import { IndexedReadTestResult } from "../types";

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
