import { exec } from "child_process";
import { join } from "path";
import { promisify } from "util";
import { TestVariant } from "variants/types";

import { StreamedReadTestRunner } from "./TestRunner";
import { StreamedReadTestResult } from "../types";

export default class CppStreamedReaderTestRunner extends StreamedReadTestRunner {
  readonly name = "cpp-streamed-reader";

  async runReadTest(filePath: string): Promise<StreamedReadTestResult> {
    const { stdout } = await promisify(exec)(`./streamed-reader-conformance ${filePath}`, {
      cwd: join(__dirname, "../../../../../cpp/test/build/Debug"),
    });
    return JSON.parse(stdout) as StreamedReadTestResult;
  }

  supportsVariant(_variant: TestVariant): boolean {
    return true;
  }
}
