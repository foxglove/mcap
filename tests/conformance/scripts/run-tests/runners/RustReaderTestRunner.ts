import { exec } from "node:child_process";
import { join } from "node:path";
import { promisify } from "node:util";
import type { TestVariant } from "../../../variants/types.ts";

import { StreamedReadTestRunner } from "./TestRunner.ts";
import type { StreamedReadTestResult } from "../types.ts";

export default class RustReaderTestRunner extends StreamedReadTestRunner {
  readonly name = "rust-streamed-reader";

  async runReadTest(filePath: string): Promise<StreamedReadTestResult> {
    const { stdout } = await promisify(exec)(`./conformance_reader ${filePath}`, {
      cwd: join(__dirname, "../../../../../rust/target/debug/examples"),
    });
    return JSON.parse(stdout.trim()) as StreamedReadTestResult;
  }

  supportsVariant(_variant: TestVariant): boolean {
    return true;
  }
}
