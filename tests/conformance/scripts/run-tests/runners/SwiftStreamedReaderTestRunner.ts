import { exec } from "node:child_process";
import path from "node:path";
import { promisify } from "node:util";

import { StreamedReadTestRunner } from "./TestRunner.ts";
import type { TestVariant } from "../../../variants/types.ts";
import type { StreamedReadTestResult } from "../types.ts";

export default class SwiftStreamedReaderTestRunner extends StreamedReadTestRunner {
  readonly name = "swift-streamed-reader";

  async runReadTest(filePath: string): Promise<StreamedReadTestResult> {
    const { stdout } = await promisify(exec)(
      `./.build/debug/conformance read-streamed ${filePath}`,
      {
        cwd: path.join(import.meta.dirname, "../../../../.."),
      },
    );

    return JSON.parse(stdout) as StreamedReadTestResult;
  }

  supportsVariant(_variant: TestVariant): boolean {
    return true;
  }
}
