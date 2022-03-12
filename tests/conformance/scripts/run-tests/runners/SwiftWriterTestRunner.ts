import { exec } from "child_process";
import path from "path";
import { promisify } from "util";
import { TestFeatures, TestVariant } from "variants/types";

import { WriteTestRunner } from "./TestRunner";

export default class SwiftWriterTestRunner extends WriteTestRunner {
  readonly name = "swift-writer";

  async runWriteTest(filePath: string): Promise<Uint8Array> {
    const { stdout, stderr } = await promisify(exec)(`./.build/debug/conformance ${filePath}`, {
      cwd: path.join(__dirname, "../../../../.."),
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
    if (variant.features.has(TestFeatures.UseChunks)) {
      return false;
    }
    if (variant.features.has(TestFeatures.AddExtraDataToRecords)) {
      return false;
    }

    return true;
  }
}
