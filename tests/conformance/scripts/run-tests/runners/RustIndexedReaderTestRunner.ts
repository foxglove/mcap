import { exec } from "child_process";
import { join } from "path";
import { promisify } from "util";
import { TestFeatures, TestVariant } from "variants/types";

import { IndexedReadTestRunner } from "./TestRunner";
import { IndexedReadTestResult } from "../types";

export default class RustIndexedReaderTestRunner extends IndexedReadTestRunner {
  readonly name = "rust-indexed-reader";

  async runReadTest(filePath: string): Promise<IndexedReadTestResult> {
    const { stdout } = await promisify(exec)(`./conformance_indexed_reader ${filePath}`, {
      cwd: join(__dirname, "../../../../../rust/target/debug/examples"),
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
