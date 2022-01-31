import { Mcap0StreamReader } from "@foxglove/mcap";
import fs from "fs/promises";

export interface TestRunner {
  readonly name: string;

  readonly supportsDataOnly: boolean;
  readonly supportsDataAndSummary: boolean;
  readonly supportsDataAndSummaryWithOffsets: boolean;
  run(filePath: string): Promise<string[]>;
}

class TypescriptStreamedTestRunner implements TestRunner {
  name = "ts-stream";
  supportsDataOnly = true;
  supportsDataAndSummary = true;
  supportsDataAndSummaryWithOffsets = true;
  async run(filePath: string): Promise<string[]> {
    const result = [];
    const reader = new Mcap0StreamReader({ validateCrcs: true });
    reader.append(await fs.readFile(filePath));
    let record;
    while ((record = reader.nextRecord())) {
      result.push(
        JSON.stringify(record, (_key, value) =>
          // eslint-disable-next-line @typescript-eslint/no-unsafe-return
          typeof value === "bigint" ? `BigInt(${value})` : value,
        ),
      );
    }
    if (!reader.done()) {
      throw new Error("Reader not done");
    }
    return result;
  }
}

export default [new TypescriptStreamedTestRunner()];
