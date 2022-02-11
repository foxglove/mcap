import CppStreamedTestRunner from "./CppStreamedTestRunner";
import GoStreamedTestRunner from "./GoStreamedTestRunner";
import ITestRunner from "./ITestRunner";
import PythonStreamedReaderTestRunner from "./PythonStreamedReaderTestRunner";
import PythonStreamedWriterTestRunner from "./PythonStreamedWriterTestRunner";
import TypescriptIndexedReaderTestRunner from "./TypescriptIndexedReaderTestRunner";
import TypescriptStreamedReaderTestRunner from "./TypescriptStreamedReaderTestRunner";
import TypescriptWriterTestRunner from "./TypescriptWriterTestRunner";

export type { ITestRunner };

const runners: readonly ITestRunner[] = [
  new CppStreamedTestRunner(),
  new GoStreamedTestRunner(),
  new PythonStreamedReaderTestRunner(),
  new PythonStreamedWriterTestRunner(),
  new TypescriptIndexedReaderTestRunner(),
  new TypescriptStreamedReaderTestRunner(),
  new TypescriptWriterTestRunner(),
];
export default runners;
