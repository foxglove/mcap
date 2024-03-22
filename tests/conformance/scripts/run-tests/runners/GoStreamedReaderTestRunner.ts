import { exec } from "child_process";
import { join } from "path";
import { promisify } from "util";
import { TestFeatures, TestVariant } from "variants/types";

import { StreamedReadTestRunner } from "./TestRunner";
import { StreamedReadTestResult } from "../types";

export default class GoStreamedReaderTestRunner extends StreamedReadTestRunner {
  readonly name = "go-streamed-reader";
  readonly sortsMessages = true;

  async runReadTest(filePath: string): Promise<StreamedReadTestResult> {
    const { stdout } = await promisify(exec)(`./bin/test-read-conformance ${filePath} streamed`, {
      cwd: join(__dirname, "../../../../../go/conformance"),
    });
    return JSON.parse(stdout) as StreamedReadTestResult;
  }

  supportsVariant(variant: TestVariant): boolean {
    return (
      variant.features.has(TestFeatures.UseChunkIndex) &&
      variant.features.has(TestFeatures.UseStatistics)
    );
  }
}
