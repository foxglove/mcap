import CppStreamedReaderTestRunner from "./CppStreamedReaderTestRunner";
import CppStreamedWriterTestRunner from "./CppStreamedWriterTestRunner";
import GoStreamedReaderTestRunner from "./GoStreamedReaderTestRunner";
import GoStreamedWriterTestRunner from "./GoStreamedWriterTestRunner";
import PythonStreamedReaderTestRunner from "./PythonStreamedReaderTestRunner";
import PythonWriterTestRunner from "./PythonWriterTestRunner";
import SwiftStreamedReaderTestRunner from "./SwiftStreamedReaderTestRunner";
import SwiftWriterTestRunner from "./SwiftWriterTestRunner";
import { ReadTestRunner, WriteTestRunner } from "./TestRunner";
import TypescriptIndexedReaderTestRunner from "./TypescriptIndexedReaderTestRunner";
import TypescriptStreamedReaderTestRunner from "./TypescriptStreamedReaderTestRunner";
import TypescriptWriterTestRunner from "./TypescriptWriterTestRunner";

const runners: readonly (ReadTestRunner | WriteTestRunner)[] = [
  new CppStreamedReaderTestRunner(),
  new CppStreamedWriterTestRunner(),
  new GoStreamedReaderTestRunner(),
  new GoStreamedWriterTestRunner(),
  new PythonStreamedReaderTestRunner(),
  new PythonWriterTestRunner(),
  new TypescriptIndexedReaderTestRunner(),
  new TypescriptStreamedReaderTestRunner(),
  new TypescriptWriterTestRunner(),
  new SwiftWriterTestRunner(),
  new SwiftStreamedReaderTestRunner(),
];

export default runners;
