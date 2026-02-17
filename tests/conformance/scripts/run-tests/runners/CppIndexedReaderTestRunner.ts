import { exec } from "node:child_process";
import { join } from "node:path";
import { promisify } from "node:util";
import { TestFeatures } from "../../../variants/types.ts";
import type { TestVariant } from "../../../variants/types.ts";

import { IndexedReadTestRunner } from "./TestRunner.ts";
import type { IndexedReadTestResult } from "../types.ts";

export default class CppIndexedReaderTestRunner extends IndexedReadTestRunner {
  readonly name = "cpp-indexed-reader";

  async runReadTest(filePath: string): Promise<IndexedReadTestResult> {
    const { stdout } = await promisify(exec)(`./indexed-reader-conformance ${filePath}`, {
      cwd: join(__dirname, "../../../../../cpp/test/build/Debug/bin"),
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
