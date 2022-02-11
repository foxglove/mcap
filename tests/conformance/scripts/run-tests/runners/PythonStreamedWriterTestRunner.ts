import { exec } from "child_process";
import { promisify } from "util";
import { TestVariant } from "variants/types";

import { WriteTestRunner } from "./TestRunner";

export default class PythonStreamedWriterTestRunner implements WriteTestRunner {
  name = "py-streamed-writer";

  async runWriteTest(filePath: string): Promise<Uint8Array> {
    const { stdout } = await promisify(exec)(`python3 tests/run_writer_test.py ${filePath}`, {
      cwd: "../../python",
      encoding: "binary",
    });
    return stdout as unknown as Uint8Array;
  }

  supportsVariant(_variant: TestVariant): boolean {
    return true;
  }
}
