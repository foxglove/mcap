import { exec } from "child_process";
import { join } from "path";
import { promisify } from "util";
import { TestVariant } from "variants/types";

import { ITestRunner } from ".";

export default class GoStreamedTestRunner implements ITestRunner {
  name = "go-streamed-reader";
  mode = "read" as const;

  async run(filePath: string): Promise<string> {
    const { stdout } = await promisify(exec)(`./check-conformance ${filePath}`, {
      cwd: join(__dirname, "../../../../../go/conformance"),
    });
    return stdout.trim();
  }

  supportsVariant(_variant: TestVariant): boolean {
    return true;
  }
}
