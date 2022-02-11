import CppStreamedTestRunner from "./CppStreamedTestRunner";
import GoStreamedTestRunner from "./GoStreamedTestRunner";
import PythonStreamedReaderTestRunner from "./PythonStreamedReaderTestRunner";
import PythonStreamedWriterTestRunner from "./PythonStreamedWriterTestRunner";
import { ReadTestRunner, WriteTestRunner } from "./TestRunner";
import TypescriptIndexedReaderTestRunner from "./TypescriptIndexedReaderTestRunner";
import TypescriptStreamedReaderTestRunner from "./TypescriptStreamedReaderTestRunner";
import TypescriptWriterTestRunner from "./TypescriptWriterTestRunner";

const runners: readonly (ReadTestRunner | WriteTestRunner)[] = [
  new CppStreamedTestRunner(),
  new GoStreamedTestRunner(),
  new PythonStreamedReaderTestRunner(),
  new PythonStreamedWriterTestRunner(),
  new TypescriptIndexedReaderTestRunner(),
  new TypescriptStreamedReaderTestRunner(),
  new TypescriptWriterTestRunner(),
];

export default runners;
