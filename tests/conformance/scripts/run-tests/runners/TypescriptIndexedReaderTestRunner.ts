import { Mcap0IndexedReader, Mcap0Types } from "@foxglove/mcap";
import fs from "fs/promises";
import { TestFeatures, TestVariant } from "variants/types";

import { ReadTestRunner } from "./TestRunner";
import { stringifyRecords } from "./stringifyRecords";

export default class TypescriptIndexedReaderTestRunner extends ReadTestRunner {
  readonly name = "ts-indexed-reader";
  readonly readsDataEnd = false;

  async runReadTest(filePath: string, variant: TestVariant): Promise<string> {
    const handle = await fs.open(filePath, "r");
    try {
      return await this._run(handle, variant);
    } finally {
      await handle.close();
    }
  }

  supportsVariant({ records, features }: TestVariant): boolean {
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

  private async _run(fileHandle: fs.FileHandle, variant: TestVariant): Promise<string> {
    const testResult: Mcap0Types.TypedMcapRecord[] = [];
    let buffer = new ArrayBuffer(4096);
    const readable = {
      size: async () => BigInt((await fileHandle.stat()).size),
      read: async (offset: bigint, length: bigint) => {
        if (offset > Number.MAX_SAFE_INTEGER || length > Number.MAX_SAFE_INTEGER) {
          throw new Error(`Read too large: offset ${offset}, length ${length}`);
        }
        if (length > buffer.byteLength) {
          buffer = new ArrayBuffer(Number(length * 2n));
        }
        const result = await fileHandle.read({
          buffer: new DataView(buffer, 0, Number(length)),
          position: Number(offset),
        });
        if (result.bytesRead !== Number(length)) {
          throw new Error(
            `Read only ${result.bytesRead} bytes from offset ${offset}, expected ${length}`,
          );
        }
        return new Uint8Array(result.buffer.buffer, result.buffer.byteOffset, result.bytesRead);
      },
    };

    const reader = await Mcap0IndexedReader.Initialize({ readable });
    if (reader.chunkIndexes.length === 0) {
      throw new Error("No chunk indexes");
    }

    testResult.push(reader.header);
    for (const record of reader.schemasById.values()) {
      testResult.push(record);
    }
    for (const record of reader.channelsById.values()) {
      testResult.push(record);
    }
    for await (const record of reader.readMessages()) {
      testResult.push(record);
    }

    testResult.push({ type: "DataEnd", dataSectionCrc: 0 });

    // repeat schemas & channel infos
    for (const record of reader.schemasById.values()) {
      testResult.push(record);
    }
    for (const record of reader.channelsById.values()) {
      testResult.push(record);
    }

    if (reader.statistics) {
      testResult.push(reader.statistics);
    }
    for (const record of reader.chunkIndexes) {
      testResult.push(record);
    }
    for (const record of reader.attachmentIndexes) {
      testResult.push(record);
    }
    for (const record of reader.metadataIndexes) {
      testResult.push(record);
    }
    for (const summaryOffset of reader.summaryOffsetsByOpcode.values()) {
      testResult.push(summaryOffset);
    }
    testResult.push(reader.footer);

    return stringifyRecords(testResult, variant);
  }
}
