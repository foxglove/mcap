import fs from "fs/promises";
import { TestVariant } from "variants/types";

import ITestRunner from "./ITestRunner";

export default class TypescriptStreamedWriterTestRunner implements ITestRunner {
  name = "ts-streamed-writer";
  mode = "write" as const;

  supportsVariant(_variant: TestVariant): boolean {
    return true;
  }

  async run(filePath: string, _variant: TestVariant): Promise<string> {
    // Passthrough test for now.
    const mcapPath = filePath.replace(/\.[^.]+$/, ".mcap");
    const bytes = await fs.readFile(mcapPath);
    return Array.from(bytes)
      .map((b) => b.toString(16).padStart(2, "0"))
      .join("");
  }
}
