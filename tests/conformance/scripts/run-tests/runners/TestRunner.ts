import { TestVariant } from "../../../variants/types";
import {
  SerializableMcapRecord,
  TestCase,
  IndexedReadTestResult,
  StreamedReadTestResult,
} from "../types";

export abstract class StreamedReadTestRunner {
  abstract readonly name: string;
  abstract readonly sortsMessages: boolean;

  /**
   * @returns true if the test variant is supported; false if it is not. If this method returns
   * false, this test variant will be skipped.
   */
  abstract supportsVariant(variant: TestVariant): boolean;

  /**
   * Execute the reader test. This may involve calling out to separate process, e.g. with
   * `child_process.exec`.
   * @param filePath A path to a `.mcap` file that should be read.
   * @returns a StreamedReadTestResult object.
   */
  abstract runReadTest(filePath: string): Promise<StreamedReadTestResult>;

  expectedResult(testCase: TestCase): StreamedReadTestResult {
    if (this.sortsMessages) {
      return { records: sortMessageRecords(testCase.records) };
    }
    return { records: testCase.records };
  }
}

export function sortMessageRecords(records: SerializableMcapRecord[]): SerializableMcapRecord[] {
  let firstMessage: number | undefined;
  let lastMessage: number | undefined;
  for (let i = 0; i < records.length; i++) {
    const recordType = records[i]?.type;
    if (recordType === "Message" && firstMessage == undefined) {
      firstMessage = i;
    }
    if (firstMessage != undefined && recordType !== "Message") {
      lastMessage = i;
      break;
    }
  }
  if (firstMessage == undefined) {
    return records;
  }
  if (lastMessage == undefined) {
    return records;
  }
  const leader = records.slice(0, firstMessage);
  const messages = records.slice(firstMessage, lastMessage);
  const trailer = records.slice(lastMessage);
  messages.sort((a, b) => {
    const timeA = findLogTime(a);
    const timeB = findLogTime(b);
    if (timeA > timeB) {
      return 1;
    } else if (timeA < timeB) {
      return -1;
    }
    return 0;
  });
  return leader.concat(messages).concat(trailer);
}

function findLogTime(record: SerializableMcapRecord): bigint {
  for (const [fieldName, fieldValue] of record.fields) {
    if (fieldName === "log_time") {
      return BigInt(fieldValue as string);
    }
  }
  throw new Error(`could not find 'log_time' field on record: ${JSON.stringify(record)}`);
}

export abstract class IndexedReadTestRunner {
  abstract readonly name: string;

  /**
   * @returns true if the test variant is supported; false if it is not. If this method returns
   * false, this test variant will be skipped.
   */
  abstract supportsVariant(variant: TestVariant): boolean;

  /**
   * Execute the reader test. This may involve calling out to separate process, e.g. with
   * `child_process.exec`.
   * @param filePath A path to a `.mcap` file that should be read.
   * @returns an IndexedReadTestResult object.
   */
  abstract runReadTest(filePath: string): Promise<IndexedReadTestResult>;

  expectedResult(testCase: TestCase): IndexedReadTestResult {
    function findRecordId(record: SerializableMcapRecord): number {
      for (const [fieldName, fieldValue] of record.fields) {
        if (fieldName === "id") {
          return parseInt(fieldValue as string);
        }
      }
      throw new Error(`could not find 'id' field on record: ${JSON.stringify(record)}`);
    }

    const result: IndexedReadTestResult = {
      schemas: [],
      channels: [],
      messages: [],
      statistics: [],
    };
    const knownSchemaIds = new Set<number>();
    const knownChannelIds = new Set<number>();
    for (const record of testCase.records) {
      switch (record.type) {
        case "Schema":
          {
            const id = findRecordId(record);
            if (!knownSchemaIds.has(id)) {
              result.schemas.push(record);
              knownSchemaIds.add(id);
            }
          }
          break;
        case "Channel":
          {
            const id = findRecordId(record);
            if (!knownChannelIds.has(id)) {
              result.channels.push(record);
              knownChannelIds.add(id);
            }
          }
          break;
        case "Message":
          result.messages.push(record);
          break;
        case "Statistics":
          result.statistics.push(record);
          break;
        default:
          break;
      }
    }
    result.messages.sort((a, b) => {
      const timeA = findLogTime(a);
      const timeB = findLogTime(b);
      if (timeA > timeB) {
        return 1;
      } else if (timeA < timeB) {
        return -1;
      }
      return 0;
    });
    result.schemas.sort((a, b) => findRecordId(a) - findRecordId(b));
    result.channels.sort((a, b) => findRecordId(a) - findRecordId(b));
    return result;
  }
}

export abstract class WriteTestRunner {
  abstract readonly name: string;

  /**
   * @returns true if the test variant is supported; false if it is not. If this method returns
   * false, this test variant will be skipped.
   */
  abstract supportsVariant(variant: TestVariant): boolean;

  /**
   * Execute the writer test. This may involve calling out to separate process, e.g. with
   * `child_process.exec`.
   * @param filePath A path to a `.json` file that should be read.
   * @param variant Information about the
   * @returns A JSON-encoded object representing the input file.
   */
  abstract runWriteTest(filePath: string, variant: TestVariant): Promise<Uint8Array>;
}
