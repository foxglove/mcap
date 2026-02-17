import { exec } from "node:child_process";
import path from "node:path";
import { promisify } from "node:util";
import { TestFeatures } from "../../../variants/types.ts";
import type { TestVariant } from "../../../variants/types.ts";

import { WriteTestRunner } from "./TestRunner.ts";

export default class SwiftWriterTestRunner extends WriteTestRunner {
  readonly name = "swift-writer";

  async runWriteTest(filePath: string): Promise<Uint8Array> {
    const { stdout, stderr } = await promisify(exec)(
      `./.build/debug/conformance write ${filePath}`,
      {
        cwd: path.join(__dirname, "../../../../.."),
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
