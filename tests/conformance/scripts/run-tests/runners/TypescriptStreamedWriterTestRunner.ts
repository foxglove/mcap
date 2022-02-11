import fs from "fs/promises";
import { TestVariant } from "variants/types";

import { WriteTestRunner } from "./TestRunner";

export default class TypescriptStreamedWriterTestRunner extends WriteTestRunner {
  name = "ts-streamed-writer";

  supportsVariant(_variant: TestVariant): boolean {
    return true;
  }

  async runWriteTest(filePath: string, _variant: TestVariant): Promise<Uint8Array> {
    // Passthrough test for now.
    const mcapPath = filePath.replace(/\.[^.]+$/, ".mcap");
    const bytes = await fs.readFile(mcapPath);
    return bytes as Uint8Array;
  }
}
