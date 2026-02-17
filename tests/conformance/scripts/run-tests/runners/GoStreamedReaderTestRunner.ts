import { exec } from "node:child_process";
import { join } from "node:path";
import { promisify } from "node:util";

import { StreamedReadTestRunner } from "./TestRunner.ts";
import type { TestVariant } from "../../../variants/types.ts";
import type { StreamedReadTestResult } from "../types.ts";

export default class GoStreamedReaderTestRunner extends StreamedReadTestRunner {
  readonly name = "go-streamed-reader";

  async runReadTest(filePath: string): Promise<StreamedReadTestResult> {
    const { stdout } = await promisify(exec)(`./bin/test-read-conformance ${filePath} streamed`, {
      cwd: join(import.meta.dirname, "../../../../../go/conformance"),
    });
    return JSON.parse(stdout) as StreamedReadTestResult;
  }

  supportsVariant(_variant: TestVariant): boolean {
    return true;
  }
}
