import ITestRunner from "./ITestRunner";
import PythonStreamedTestRunner from "./PythonStreamedTestRunner";
import TypescriptIndexedTestRunner from "./TypescriptIndexedTestRunner";
import TypescriptStreamedTestRunner from "./TypescriptStreamedTestRunner";

export type { ITestRunner };

const runners: readonly ITestRunner[] = [
  new TypescriptStreamedTestRunner(),
  new TypescriptIndexedTestRunner(),
  new PythonStreamedTestRunner(),
  new TypescriptStreamedTestRunner(),
];
export default runners;
