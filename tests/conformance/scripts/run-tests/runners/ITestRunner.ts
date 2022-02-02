export default interface ITestRunner {
  readonly name: string;

  readonly supportsDataOnly: boolean;
  readonly supportsDataAndSummary: boolean;
  readonly supportsDataAndSummaryWithOffsets: boolean;
  run(filePath: string): Promise<string[]>;
}
