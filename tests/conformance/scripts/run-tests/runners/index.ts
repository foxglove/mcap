import CppStreamedReaderTestRunner from "./CppStreamedReaderTestRunner";
import CppStreamedWriterTestRunner from "./CppStreamedWriterTestRunner";
import GoStreamedTestRunner from "./GoStreamedTestRunner";
import PythonStreamedReaderTestRunner from "./PythonStreamedReaderTestRunner";
import PythonWriterTestRunner from "./PythonWriterTestRunner";
import { ReadTestRunner, WriteTestRunner } from "./TestRunner";
import TypescriptIndexedReaderTestRunner from "./TypescriptIndexedReaderTestRunner";
import TypescriptStreamedReaderTestRunner from "./TypescriptStreamedReaderTestRunner";
import TypescriptWriterTestRunner from "./TypescriptWriterTestRunner";

const runners: readonly (ReadTestRunner | WriteTestRunner)[] = [
  new CppStreamedReaderTestRunner(),
  new CppStreamedWriterTestRunner(),
  new GoStreamedTestRunner(),
  new PythonStreamedReaderTestRunner(),
  new PythonWriterTestRunner(),
  new TypescriptIndexedReaderTestRunner(),
  new TypescriptStreamedReaderTestRunner(),
  new TypescriptWriterTestRunner(),
];

export default runners;
