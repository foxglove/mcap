import { exec } from "node:child_process";
import { join } from "node:path";
import { promisify } from "node:util";

import { WriteTestRunner } from "./TestRunner.ts";
import { TestFeatures } from "../../../variants/types.ts";
import type { TestVariant } from "../../../variants/types.ts";

export default class RustWriterTestRunner extends WriteTestRunner {
  readonly name = "rust-writer";

  async runWriteTest(filePath: string): Promise<Uint8Array> {
    const { stdout, stderr } = await promisify(exec)(`./conformance_writer ${filePath}`, {
      cwd: join(__dirname, "../../../../../rust/target/debug/examples"),
      encoding: undefined,
    });

    if (stderr instanceof Buffer) {
      const errText = new TextDecoder().decode(stderr);
      if (errText.length > 0) {
        console.error(errText);
      }
    }
    return stdout as unknown as Uint8Array;
  }

  supportsVariant(variant: TestVariant): boolean {
    return !variant.features.has(TestFeatures.AddExtraDataToRecords);
  }
}
