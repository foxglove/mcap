import ITestRunner from "./ITestRunner";
import TypescriptIndexedTestRunner from "./TypescriptIndexedTestRunner";
import TypescriptStreamedTestRunner from "./TypescriptStreamedTestRunner";

export type { ITestRunner };

const runners: readonly ITestRunner[] = [
  new TypescriptStreamedTestRunner(),
  new TypescriptIndexedTestRunner(),
];
export default runners;
