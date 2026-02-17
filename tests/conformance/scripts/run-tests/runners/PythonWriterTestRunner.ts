import { exec } from "node:child_process";
import { promisify } from "node:util";
import { TestFeatures } from "../../../variants/types.ts";
import type { TestVariant } from "../../../variants/types.ts";

import { WriteTestRunner } from "./TestRunner.ts";

export default class PythonWriterTestRunner extends WriteTestRunner {
  readonly name = "py-writer";

  async runWriteTest(filePath: string): Promise<Uint8Array> {
    const { stdout, stderr } = await promisify(exec)(
      `python3 tests/run_writer_test.py ${filePath}`,
      {
        cwd: "../../python/mcap",
        encoding: undefined,
      },
    );

    if (stderr instanceof Buffer) {
      const errText = new TextDecoder().decode(stderr);
      if (errText.length > 0) {
        console.error(errText);
      }
    }
    return stdout as unknown as Uint8Array;
  }

  supportsVariant(variant: TestVariant): boolean {
    if (variant.features.has(TestFeatures.AddExtraDataToRecords)) {
      return false;
    }

    return true;
  }
}
