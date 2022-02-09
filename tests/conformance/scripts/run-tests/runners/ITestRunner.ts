import { TestVariant } from "../../../variants/types";

export default interface ITestRunner {
  readonly name: string;
  supportsVariant(variant: TestVariant): boolean;
  run(filePath: string, variant: TestVariant): Promise<string>;
}
