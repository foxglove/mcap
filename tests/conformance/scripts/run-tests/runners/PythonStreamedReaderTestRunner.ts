import { exec } from "node:child_process";
import { promisify } from "node:util";
import type { TestVariant } from "../../../variants/types.ts";

import { StreamedReadTestRunner } from "./TestRunner.ts";
import type { StreamedReadTestResult } from "../types.ts";

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
