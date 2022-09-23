import { exec } from "child_process";
import path from "path";
import { promisify } from "util";
import { TestFeatures, TestVariant } from "variants/types";

import { ReadTestRunner } from "./TestRunner";

export default class SwiftIndexedReaderTestRunner extends ReadTestRunner {
  readonly name = "swift-indexed-reader";
  readonly readsDataEnd = true;

  async runReadTest(filePath: string): Promise<string> {
    const { stdout } = await promisify(exec)(
      `./.build/debug/conformance read-indexed ${filePath}`,
      {
        cwd: path.join(__dirname, "../../../../.."),
      },
    );

    return stdout;
  }

  supportsVariant({ records, features }: TestVariant): boolean {
    if (!records.some((record) => record.type === "Message")) {
      return false;
    }
    if (!features.has(TestFeatures.UseChunks)) {
      return false;
    }
    if (!features.has(TestFeatures.UseChunkIndex)) {
      return false;
    }
    if (!features.has(TestFeatures.UseRepeatedChannelInfos)) {
      return false;
    }
    if (!features.has(TestFeatures.UseRepeatedSchemas)) {
      return false;
    }
    if (!features.has(TestFeatures.UseMessageIndex)) {
      return false;
    }
    return true;
  }
}
