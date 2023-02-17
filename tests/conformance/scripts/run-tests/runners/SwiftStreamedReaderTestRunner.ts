import { exec } from "child_process";
import path from "path";
import { promisify } from "util";
import { TestVariant } from "variants/types";

import { StreamedReadTestRunner } from "./TestRunner";
import { StreamedReadTestResult } from "../types";

export default class SwiftStreamedReaderTestRunner extends StreamedReadTestRunner {
  readonly name = "swift-streamed-reader";

  async runReadTest(filePath: string): Promise<StreamedReadTestResult> {
    const { stdout } = await promisify(exec)(
      `./.build/debug/conformance read-streamed ${filePath}`,
      {
        cwd: path.join(__dirname, "../../../../.."),
      },
    );

    return JSON.parse(stdout) as StreamedReadTestResult;
  }

  supportsVariant(_variant: TestVariant): boolean {
    return true;
  }
}
