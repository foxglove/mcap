import CppStreamedTestRunner from "./CppStreamedTestRunner";
import GoStreamedTestRunner from "./GoStreamedTestRunner";
import ITestRunner from "./ITestRunner";
import PythonStreamedReaderTestRunner from "./PythonStreamedReaderTestRunner";
import PythonStreamedWriterTestRunner from "./PythonStreamedWriterTestRunner";
import TypescriptIndexedTestRunner from "./TypescriptIndexedTestRunner";
import TypescriptStreamedReaderTestRunner from "./TypescriptStreamedReaderTestRunner";
import TypescriptStreamedWriterTestRunner from "./TypescriptStreamedWriterTestRunner";

export type { ITestRunner };

const runners: readonly ITestRunner[] = [
  new CppStreamedTestRunner(),
  new GoStreamedTestRunner(),
  new PythonStreamedReaderTestRunner(),
  new PythonStreamedWriterTestRunner(),
  new TypescriptIndexedTestRunner(),
  new TypescriptStreamedReaderTestRunner(),
  new TypescriptStreamedWriterTestRunner(),
];
export default runners;
