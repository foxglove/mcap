import { exec } from "child_process";
import { promisify } from "util";
import { TestVariant } from "variants/types";

import { ITestRunner } from ".";

export default class PythonStreamedTestRunner implements ITestRunner {
  name = "py-stream";
  async run(filePath: string): Promise<string[]> {
    const { stdout } = await promisify(exec)(
      `python3 ../../python/tests/run_reader_test.py ${filePath}`,
    );
    return stdout.trim().split("\n");
  }

  supportsVariant(_variant: TestVariant): boolean {
    return true;
  }
}
