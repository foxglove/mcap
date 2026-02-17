import { exec } from "node:child_process";
import { join } from "node:path";
import { promisify } from "node:util";

import { IndexedReadTestRunner } from "./TestRunner.ts";
import { TestFeatures } from "../../../variants/types.ts";
import type { TestVariant } from "../../../variants/types.ts";
import type { IndexedReadTestResult } from "../types.ts";

export default class RustIndexedReaderTestRunner extends IndexedReadTestRunner {
  readonly name = "rust-indexed-reader";

  async runReadTest(filePath: string): Promise<IndexedReadTestResult> {
    const { stdout } = await promisify(exec)(`./conformance_indexed_reader ${filePath}`, {
      cwd: join(import.meta.dirname, "../../../../../rust/target/debug/examples"),
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
    return true;
  }
}
