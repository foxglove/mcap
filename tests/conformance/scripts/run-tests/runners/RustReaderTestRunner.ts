import { exec } from "child_process";
import { join } from "path";
import { promisify } from "util";
import { TestFeatures, TestVariant } from "variants/types";

import { ReadTestRunner } from "./TestRunner";

export default class RustReaderTestRunner extends ReadTestRunner {
  readonly name = "rust-streamed-reader";
  readonly readsDataEnd = true;

  async runReadTest(filePath: string): Promise<string> {
    const { stdout } = await promisify(exec)(`./conformance_reader ${filePath}`, {
      cwd: join(__dirname, "../../../../../rust/target/debug/"),
    });
    return stdout.trim();
  }

  supportsVariant(variant: TestVariant): boolean {
    if (variant.features.has(TestFeatures.UseChunks)) {
      return false;
    }
    return true;
  }
}
