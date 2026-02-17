import { exec } from "node:child_process";
import { join } from "node:path";
import { promisify } from "node:util";

import { StreamedReadTestRunner } from "./TestRunner.ts";
import type { TestVariant } from "../../../variants/types.ts";
import type { StreamedReadTestResult } from "../types.ts";

export default class CppStreamedReaderTestRunner extends StreamedReadTestRunner {
  readonly name = "cpp-streamed-reader";

  async runReadTest(filePath: string): Promise<StreamedReadTestResult> {
    const { stdout } = await promisify(exec)(`./streamed-reader-conformance ${filePath}`, {
      cwd: join(import.meta.dirname, "../../../../../cpp/test/build/Debug/bin"),
    });
    return JSON.parse(stdout) as StreamedReadTestResult;
  }

  supportsVariant(_variant: TestVariant): boolean {
    return true;
  }
}
