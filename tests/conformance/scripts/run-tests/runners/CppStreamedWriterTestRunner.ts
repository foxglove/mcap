import { exec } from "child_process";
import { join } from "path";
import { promisify } from "util";
import { TestFeatures, TestVariant } from "variants/types";

import { WriteTestRunner } from "./TestRunner";

export default class CppStreamedWriterTestRunner extends WriteTestRunner {
  name = "cpp-streamed-writer";

  async runWriteTest(filePath: string): Promise<Uint8Array> {
    const { stdout } = await promisify(exec)(`./streamed-writer-conformance ${filePath}`, {
      cwd: join(__dirname, "../../../../../cpp/test/build/Debug"),
      encoding: undefined,
    });
    return stdout as unknown as Uint8Array;
  }

  supportsVariant(variant: TestVariant): boolean {
    return !variant.features.has(TestFeatures.AddExtraDataToRecords);
  }
}
