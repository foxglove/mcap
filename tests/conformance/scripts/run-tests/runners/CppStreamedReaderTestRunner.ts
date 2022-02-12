import { exec } from "child_process";
import { join } from "path";
import { promisify } from "util";
import { TestVariant } from "variants/types";

import { ReadTestRunner } from "./TestRunner";

export default class CppStreamedReaderTestRunner extends ReadTestRunner {
  name = "cpp-streamed-reader";

  async runReadTest(filePath: string): Promise<string> {
    const { stdout } = await promisify(exec)(`./streamed-reader-conformance ${filePath}`, {
      cwd: join(__dirname, "../../../../../cpp/test/build/Debug/bin"),
    });
    return stdout.trim();
  }

  supportsVariant(_variant: TestVariant): boolean {
    return true;
  }
}
