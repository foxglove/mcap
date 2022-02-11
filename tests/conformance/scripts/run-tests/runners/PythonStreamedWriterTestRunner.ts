import { exec } from "child_process";
import { promisify } from "util";
import { TestVariant } from "variants/types";

import { ITestRunner } from ".";

export default class PythonStreamedWriterTestRunner implements ITestRunner {
  readonly name = "py-streamed-writer";
  readonly mode = "write";

  async run(filePath: string): Promise<string> {
    const { stdout } = await promisify(exec)(`python3 tests/run_writer_test.py ${filePath}`, {
      cwd: "../../python",
    });
    return stdout.trim();
  }

  supportsVariant(_variant: TestVariant): boolean {
    return true;
  }
}
