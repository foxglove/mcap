import { exec } from "child_process";
import { join } from "path";
import { promisify } from "util";
import { TestVariant, TestFeatures } from "variants/types";

import { IndexedReadTestRunner } from "./TestRunner";
import { IndexedReadTestResult } from "../types";

export default class GoIndexedReaderTestRunner extends IndexedReadTestRunner {
  readonly name = "go-indexed-reader";

  async runReadTest(filePath: string): Promise<IndexedReadTestResult> {
    const { stdout } = await promisify(exec)(`./bin/test-read-conformance ${filePath} indexed`, {
      cwd: join(__dirname, "../../../../../go/conformance"),
    });
    return JSON.parse(stdout) as IndexedReadTestResult;
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
