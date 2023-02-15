import { exec } from "child_process";
import { promisify } from "util";
import { TestVariant } from "variants/types";

import { StreamedReadTestResult } from "../types";
import { StreamedReadTestRunner } from "./TestRunner";

export default class PythonStreamedReaderTestRunner extends StreamedReadTestRunner {
  readonly name = "py-streamed-reader";

  async runReadTest(filePath: string): Promise<StreamedReadTestResult> {
    const { stdout } = await promisify(exec)(
      `python3 tests/run_reader_test.py ${filePath} streamed`,
      {
        cwd: "../../python/mcap",
      },
    );
    return JSON.parse(stdout.trim()) as StreamedReadTestResult;
  }

  supportsVariant(_variant: TestVariant): boolean {
    return true;
  }
}
