import CppIndexedReaderTestRunner from "./CppIndexedReaderTestRunner";
import CppStreamedReaderTestRunner from "./CppStreamedReaderTestRunner";
import CppStreamedWriterTestRunner from "./CppStreamedWriterTestRunner";
import GoIndexedReaderTestRunner from "./GoIndexedReaderTestRunner";
import GoStreamedReaderTestRunner from "./GoStreamedReaderTestRunner";
import GoStreamedWriterTestRunner from "./GoStreamedWriterTestRunner";
import KaitaiStructReaderTestRunner from "./KaitaiStructReaderTestRunner";
import PythonIndexedReaderTestRunner from "./PythonIndexedReaderTestRunner";
import PythonStreamedReaderTestRunner from "./PythonStreamedReaderTestRunner";
import PythonWriterTestRunner from "./PythonWriterTestRunner";
import RustReaderTestRunner from "./RustReaderTestRunner";
import RustWriterTestRunner from "./RustWriterTestRunner";
import SwiftIndexedReaderTestRunner from "./SwiftIndexedReaderTestRunner";
import SwiftStreamedReaderTestRunner from "./SwiftStreamedReaderTestRunner";
import SwiftWriterTestRunner from "./SwiftWriterTestRunner";
import { IndexedReadTestRunner, StreamedReadTestRunner, WriteTestRunner } from "./TestRunner";
import TypescriptIndexedReaderTestRunner from "./TypescriptIndexedReaderTestRunner";
import TypescriptStreamedReaderTestRunner from "./TypescriptStreamedReaderTestRunner";
import TypescriptWriterTestRunner from "./TypescriptWriterTestRunner";

const runners: readonly (IndexedReadTestRunner | StreamedReadTestRunner | WriteTestRunner)[] = [
  new CppIndexedReaderTestRunner(),
  new CppStreamedReaderTestRunner(),
  new CppStreamedWriterTestRunner(),
  new GoIndexedReaderTestRunner(),
  new GoStreamedReaderTestRunner(),
  new GoStreamedWriterTestRunner(),
  new PythonIndexedReaderTestRunner(),
  new PythonStreamedReaderTestRunner(),
  new PythonWriterTestRunner(),
  new TypescriptIndexedReaderTestRunner(),
  new TypescriptStreamedReaderTestRunner(),
  new TypescriptWriterTestRunner(),
  new RustReaderTestRunner(),
  new RustWriterTestRunner(),
  new SwiftWriterTestRunner(),
  new SwiftStreamedReaderTestRunner(),
  new SwiftIndexedReaderTestRunner(),
  new KaitaiStructReaderTestRunner(),
];

export default runners;
