import { exec } from "child_process";
import { promisify } from "util";
import { TestVariant } from "variants/types";

import { ReadTestRunner } from "./TestRunner";

export default class PythonStreamedReaderTestRunner extends ReadTestRunner {
  name = "py-streamed-reader";

  async runReadTest(filePath: string): Promise<string> {
    const { stdout } = await promisify(exec)(`python3 tests/run_reader_test.py ${filePath}`, {
      cwd: "../../python",
    });
    return stdout.trim();
  }

  supportsVariant(_variant: TestVariant): boolean {
    return true;
  }
}
