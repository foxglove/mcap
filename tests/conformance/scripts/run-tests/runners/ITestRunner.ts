export default interface ITestRunner {
  readonly name: string;
  run(filePath: string): Promise<string[]>;
}
