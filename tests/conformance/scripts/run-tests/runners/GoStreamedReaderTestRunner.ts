import { exec } from "child_process";
import { join } from "path";
import { promisify } from "util";
import { TestVariant } from "variants/types";

import { ReadTestRunner } from "./TestRunner";

export default class GoStreamedReaderTestRunner extends ReadTestRunner {
  name = "go-streamed-reader";

  async runReadTest(filePath: string): Promise<string> {
    const { stdout } = await promisify(exec)(`./bin/test-streamed-read-conformance ${filePath}`, {
      cwd: join(__dirname, "../../../../../go/conformance"),
    });
    return stdout.trim();
  }

  supportsVariant(_variant: TestVariant): boolean {
    return true;
  }
}
