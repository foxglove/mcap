import ITestRunner from "./ITestRunner";
import TypescriptStreamedTestRunner from "./TypescriptStreamedTestRunner";

export type { ITestRunner };

const runners: readonly ITestRunner[] = [new TypescriptStreamedTestRunner()];
export default runners;
