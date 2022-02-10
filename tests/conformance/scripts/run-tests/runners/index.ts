import CppStreamedTestRunner from "./CppStreamedTestRunner";
import ITestRunner from "./ITestRunner";
import PythonStreamedTestRunner from "./PythonStreamedTestRunner";
import TypescriptIndexedTestRunner from "./TypescriptIndexedTestRunner";
import TypescriptStreamedTestRunner from "./TypescriptStreamedTestRunner";

export type { ITestRunner };

const runners: readonly ITestRunner[] = [
  new CppStreamedTestRunner(),
  new PythonStreamedTestRunner(),
  new TypescriptIndexedTestRunner(),
  new TypescriptStreamedTestRunner(),
];
export default runners;
