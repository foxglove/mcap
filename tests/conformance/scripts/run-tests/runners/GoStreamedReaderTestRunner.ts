import { exec } from "child_process";
import { join } from "path";
import { promisify } from "util";
import { TestVariant } from "variants/types";

import { StreamedReadTestResult } from "../types";
import { StreamedReadTestRunner } from "./TestRunner";

export default class GoStreamedReaderTestRunner extends StreamedReadTestRunner {
  readonly name = "go-streamed-reader";

  async runReadTest(filePath: string): Promise<StreamedReadTestResult> {
    const { stdout } = await promisify(exec)(`./bin/test-read-conformance ${filePath} streamed`, {
      cwd: join(__dirname, "../../../../../go/conformance"),
    });
    return JSON.parse(stdout) as StreamedReadTestResult;
  }

  supportsVariant(_variant: TestVariant): boolean {
    return true;
  }
}
