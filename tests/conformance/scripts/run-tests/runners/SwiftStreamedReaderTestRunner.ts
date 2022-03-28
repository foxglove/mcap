import { exec } from "child_process";
import path from "path";
import { promisify } from "util";
import { TestVariant } from "variants/types";

import { ReadTestRunner } from "./TestRunner";

export default class SwiftStreamedReaderTestRunner extends ReadTestRunner {
  readonly name = "swift-streamed-reader";
  readonly readsDataEnd = true;

  async runReadTest(filePath: string): Promise<string> {
    const { stdout } = await promisify(exec)(`./.build/debug/conformance read ${filePath}`, {
      cwd: path.join(__dirname, "../../../../.."),
    });

    return stdout;
  }

  supportsVariant(_variant: TestVariant): boolean {
    return true;
  }
}
