import { exec } from "child_process";
import { join } from "path";
import { promisify } from "util";
import { TestVariant } from "variants/types";

import { ITestRunner } from ".";

export default class CppStreamedTestRunner implements ITestRunner {
  name = "cpp-streamed";
  mode = "read" as const;

  async run(filePath: string): Promise<string> {
    const { stdout } = await promisify(exec)(`./streamed-reader-conformance ${filePath}`, {
      cwd: join(__dirname, "../../../../../cpp/test/build/Debug/bin"),
    });
    return stdout.trim();
  }

  supportsVariant(_variant: TestVariant): boolean {
    return true;
  }
}
