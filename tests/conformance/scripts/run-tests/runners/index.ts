import CppIndexedReaderTestRunner from "./CppIndexedReaderTestRunner.ts";
import CppStreamedReaderTestRunner from "./CppStreamedReaderTestRunner.ts";
import CppStreamedWriterTestRunner from "./CppStreamedWriterTestRunner.ts";
import GoIndexedReaderTestRunner from "./GoIndexedReaderTestRunner.ts";
import GoStreamedReaderTestRunner from "./GoStreamedReaderTestRunner.ts";
import GoStreamedWriterTestRunner from "./GoStreamedWriterTestRunner.ts";
import KaitaiStructReaderTestRunner from "./KaitaiStructReaderTestRunner.ts";
import PythonIndexedReaderTestRunner from "./PythonIndexedReaderTestRunner.ts";
import PythonStreamedReaderTestRunner from "./PythonStreamedReaderTestRunner.ts";
import PythonWriterTestRunner from "./PythonWriterTestRunner.ts";
import RustAsyncReaderTestRunner from "./RustAsyncReaderTestRunner.ts";
import RustIndexedReaderTestRunner from "./RustIndexedReaderTestRunner.ts";
import RustReaderTestRunner from "./RustReaderTestRunner.ts";
import RustWriterTestRunner from "./RustWriterTestRunner.ts";
import SwiftIndexedReaderTestRunner from "./SwiftIndexedReaderTestRunner.ts";
import SwiftStreamedReaderTestRunner from "./SwiftStreamedReaderTestRunner.ts";
import SwiftWriterTestRunner from "./SwiftWriterTestRunner.ts";
import { IndexedReadTestRunner, StreamedReadTestRunner, WriteTestRunner } from "./TestRunner.ts";
import TypescriptIndexedReaderTestRunner from "./TypescriptIndexedReaderTestRunner.ts";
import TypescriptStreamedReaderTestRunner from "./TypescriptStreamedReaderTestRunner.ts";
import TypescriptWriterTestRunner from "./TypescriptWriterTestRunner.ts";

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
  new RustAsyncReaderTestRunner(),
  new RustReaderTestRunner(),
  new RustIndexedReaderTestRunner(),
  new RustWriterTestRunner(),
  new SwiftWriterTestRunner(),
  new SwiftStreamedReaderTestRunner(),
  new SwiftIndexedReaderTestRunner(),
  new KaitaiStructReaderTestRunner(),
];

export default runners;
