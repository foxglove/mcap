import { exec } from "child_process";
import { intersection } from "lodash";
import { promisify } from "util";
import { TestFeatures, TestVariant } from "variants/types";

import { WriteTestRunner } from "./TestRunner";

export default class PythonStreamedWriterTestRunner extends WriteTestRunner {
  name = "py-streamed-writer";

  async runWriteTest(filePath: string): Promise<Uint8Array> {
    const { stdout } = await promisify(exec)(`python3 tests/run_writer_test.py ${filePath}`, {
      cwd: "../../python",
      encoding: undefined,
    });

    return stdout as unknown as Uint8Array;
  }

  supportsVariant(variant: TestVariant): boolean {
    const unsupported = [
      TestFeatures.AddExtraDataToRecords,
      TestFeatures.UseChunkIndex,
      TestFeatures.UseChunks,
      TestFeatures.UseMessageIndex,
      TestFeatures.UseMetadataIndex,
      TestFeatures.UseRepeatedChannelInfos,
      TestFeatures.UseRepeatedSchemas,
      TestFeatures.UseSummaryOffset,
    ];
    return intersection(Array.from(variant.features), unsupported).length === 0;
  }
}
