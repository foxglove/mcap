import { exec } from "child_process";
import { join } from "path";
import { promisify } from "util";
import { TestVariant } from "variants/types";

import { StreamedReadTestResult } from "../types";
import { StreamedReadTestRunner } from "./TestRunner";

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
