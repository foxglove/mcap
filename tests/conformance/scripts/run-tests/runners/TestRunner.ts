import { TestVariant } from "../../../variants/types";

export abstract class ReadTestRunner {
  abstract readonly name: string;
  abstract supportsVariant(variant: TestVariant): boolean;
  abstract runReadTest(filePath: string, variant: TestVariant): Promise<string>;
}

export abstract class WriteTestRunner {
  abstract readonly name: string;
  abstract supportsVariant(variant: TestVariant): boolean;
  abstract runWriteTest(filePath: string, variant: TestVariant): Promise<Uint8Array>;
}
