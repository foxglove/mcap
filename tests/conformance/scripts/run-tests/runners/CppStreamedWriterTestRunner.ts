import { exec } from "node:child_process";
import { join } from "node:path";
import { promisify } from "node:util";

import { WriteTestRunner } from "./TestRunner.ts";
import { TestFeatures } from "../../../variants/types.ts";
import type { TestVariant } from "../../../variants/types.ts";

export default class CppStreamedWriterTestRunner extends WriteTestRunner {
  name = "cpp-streamed-writer";

  async runWriteTest(filePath: string): Promise<Uint8Array> {
    const { stdout } = await promisify(exec)(`./streamed-writer-conformance ${filePath}`, {
      cwd: join(import.meta.dirname, "../../../../../cpp/test/build/Debug/bin"),
      encoding: undefined,
    });
    return stdout as unknown as Uint8Array;
  }

  supportsVariant(variant: TestVariant): boolean {
    return !variant.features.has(TestFeatures.AddExtraDataToRecords);
  }
}
