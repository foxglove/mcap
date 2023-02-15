import { exec } from "child_process";
import { join } from "path";
import { promisify } from "util";
import { TestFeatures, TestVariant } from "variants/types";

import { WriteTestRunner } from "./TestRunner";

export default class GoStreamedWriterTestRunner extends WriteTestRunner {
  name = "go-streamed-writer";

  async runWriteTest(filePath: string): Promise<Uint8Array> {
    const { stdout } = await promisify(exec)(`./bin/test-write-conformance ${filePath}`, {
      cwd: join(__dirname, "../../../../../go/conformance"),
      encoding: undefined,
    });
    return stdout as unknown as Uint8Array;
  }

  supportsVariant(variant: TestVariant): boolean {
    return !variant.features.has(TestFeatures.AddExtraDataToRecords);
  }
}
