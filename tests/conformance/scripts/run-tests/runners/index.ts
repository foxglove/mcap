import CppStreamedTestRunner from "./CppStreamedTestRunner";
import GoStreamedTestRunner from "./GoStreamedTestRunner";
import PythonStreamedReaderTestRunner from "./PythonStreamedReaderTestRunner";
import PythonStreamedWriterTestRunner from "./PythonStreamedWriterTestRunner";
import TypescriptIndexedTestRunner from "./TypescriptIndexedTestRunner";
import TypescriptStreamedReaderTestRunner from "./TypescriptStreamedReaderTestRunner";
import TypescriptStreamedWriterTestRunner from "./TypescriptStreamedWriterTestRunner";

const runners = [
  new CppStreamedTestRunner(),
  new GoStreamedTestRunner(),
  new PythonStreamedReaderTestRunner(),
  new PythonStreamedWriterTestRunner(),
  new TypescriptIndexedTestRunner(),
  new TypescriptStreamedReaderTestRunner(),
  new TypescriptStreamedWriterTestRunner(),
] as const;

export default runners;
