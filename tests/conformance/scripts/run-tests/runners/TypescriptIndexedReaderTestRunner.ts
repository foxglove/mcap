import { McapIndexedReader } from "@mcap/core";
import { FileHandleReadable } from "@mcap/nodejs";
import fs from "fs/promises";
import { TestFeatures, TestVariant } from "variants/types";

import { IndexedReadTestRunner } from "./TestRunner";
import { toSerializableMcapRecord } from "../toSerializableMcapRecord";
import { IndexedReadTestResult } from "../types";

export default class TypescriptIndexedReaderTestRunner extends IndexedReadTestRunner {
  readonly name = "ts-indexed-reader";

  async runReadTest(filePath: string): Promise<IndexedReadTestResult> {
    const handle = await fs.open(filePath, "r");
    try {
      return await this.#run(handle);
    } finally {
      await handle.close();
    }
  }

  supportsVariant({ records, features }: TestVariant): boolean {
    if (features.has(TestFeatures.AddExtraDataToRecords)) {
      // Due to a spec bug we currently support padding at the end of DataEnd records. This
      // interferes with the indexed writer's ability to find the location of the DataEnd record.
      return false;
    }
    if (!records.some((record) => record.type === "Message")) {
      return false;
    }
    if (!features.has(TestFeatures.UseChunks)) {
      return false;
    }
    if (!features.has(TestFeatures.UseChunkIndex)) {
      return false;
    }
    if (!features.has(TestFeatures.UseRepeatedChannelInfos)) {
      return false;
    }
    if (!features.has(TestFeatures.UseRepeatedSchemas)) {
      return false;
    }
    if (!features.has(TestFeatures.UseMessageIndex)) {
      return false;
    }
    return true;
  }

  async #run(fileHandle: fs.FileHandle): Promise<IndexedReadTestResult> {
    const readable = new FileHandleReadable(fileHandle);

    const reader = await McapIndexedReader.Initialize({ readable });
    if (reader.chunkIndexes.length === 0) {
      throw new Error("No chunk indexes");
    }

    const testResult: IndexedReadTestResult = {
      schemas: [],
      channels: [],
      messages: [],
      statistics: [],
    };

    for (const record of reader.schemasById.values()) {
      testResult.schemas.push(toSerializableMcapRecord(record));
    }
    for (const record of reader.channelsById.values()) {
      testResult.channels.push(toSerializableMcapRecord(record));
    }
    for await (const record of reader.readMessages()) {
      testResult.messages.push(toSerializableMcapRecord(record));
    }
    if (reader.statistics) {
      testResult.statistics.push(toSerializableMcapRecord(reader.statistics));
    }
    return testResult;
  }
}
