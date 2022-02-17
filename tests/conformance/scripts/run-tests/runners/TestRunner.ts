import { TestVariant } from "../../../variants/types";

export abstract class ReadTestRunner {
  abstract readonly name: string;

  /**
   * Set this flag to true if the reader is expected to encounter the DataEnd record. Readers that
   * do not scan the file linearly can set this to false, and the test harness will then expect the
   * reader *not* to output a DataEnd record with a correct dataSectionCrc.
   */
  abstract readonly readsDataEnd: boolean;

  /**
   * @returns true if the test variant is supported; false if it is not. If this method returns
   * false, this test variant will be skipped.
   */
  abstract supportsVariant(variant: TestVariant): boolean;

  /**
   * Execute the reader test. This may involve calling out to separate process, e.g. with
   * `child_process.exec`.
   * @param filePath A path to a `.mcap` file that should be read.
   * @returns A JSON-encoded object representing the input file.
   */
  abstract runReadTest(filePath: string, variant: TestVariant): Promise<string>;
}

export abstract class WriteTestRunner {
  abstract readonly name: string;

  /**
   * @returns true if the test variant is supported; false if it is not. If this method returns
   * false, this test variant will be skipped.
   */
  abstract supportsVariant(variant: TestVariant): boolean;

  /**
   * Execute the writer test. This may involve calling out to separate process, e.g. with
   * `child_process.exec`.
   * @param filePath A path to a `.json` file that should be read.
   * @param variant Information about the
   * @returns A JSON-encoded object representing the input file.
   */
  abstract runWriteTest(filePath: string, variant: TestVariant): Promise<Uint8Array>;
}
