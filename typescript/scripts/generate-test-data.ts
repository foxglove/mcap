import { program } from "commander";
import fs from "fs/promises";
import path from "path";

import { Mcap0StreamReader } from "../src";
import {
  AttachmentIndex,
  ChunkIndex,
  Mcap0RecordBuilder,
  MetadataIndex,
  TypedMcapRecords,
} from "../src/v0";
import { ChunkBuilder } from "../src/v0/ChunkBuilder";
import { Opcode } from "../src/v0/constants";

enum TestFeatures {
  UseChunks = "ch",
  UseMessageIndex = "mx",
  UseStatistics = "st",
  UseRepeatedChannelInfos = "rch",
  UseAttachmentIndex = "ax",
  UseMetadataIndex = "mdx",
  UseChunkIndex = "chx",
  UseSummaryOffset = "sum",
  AddExtraDataToRecords = "pad",
}

function* generateVariants(...variables: TestFeatures[]): Generator<Set<TestFeatures>, void, void> {
  if (variables.length === 0) {
    yield new Set();
    return;
  }
  for (const variant of generateVariants(...variables.slice(1))) {
    yield variant;
    yield new Set([variables[0]!, ...variant]);
  }
}

type TestDataRecord = TypedMcapRecords["Message" | "ChannelInfo" | "Attachment" | "Metadata"];

function generateFile(variant: Set<TestFeatures>, records: TestDataRecord[]) {
  const builder = new Mcap0RecordBuilder({
    padRecords: variant.has(TestFeatures.AddExtraDataToRecords),
  });
  builder.writeMagic();
  builder.writeHeader({ profile: "", library: "" });

  const chunk = variant.has(TestFeatures.UseChunks) ? new ChunkBuilder() : undefined;
  const chunkCount = chunk ? 1 : 0;

  const metadataIndexes: MetadataIndex[] = [];
  const attachmentIndexes: AttachmentIndex[] = [];
  const chunkIndexes: ChunkIndex[] = [];

  let messageCount = 0n;
  let channelCount = 0;
  let attachmentCount = 0;
  const channelMessageCounts = new Map<number, bigint>();

  for (const record of records) {
    switch (record.type) {
      case "ChannelInfo":
        channelCount++;
        if (chunk) {
          chunk.addChannelInfo(record);
        } else {
          builder.writeChannelInfo(record);
        }
        break;

      case "Message":
        messageCount++;
        channelMessageCounts.set(
          record.channelId,
          (channelMessageCounts.get(record.channelId) ?? 0n) + 1n,
        );
        if (chunk) {
          chunk.addMessage(record);
        } else {
          builder.writeMessage(record);
        }
        break;

      case "Attachment": {
        attachmentCount++;
        const offset = BigInt(builder.length);
        const length = builder.writeAttachment(record);
        attachmentIndexes.push({
          name: record.name,
          attachmentRecordLength: length,
          offset,
          attachmentSize: BigInt(record.data.byteLength),
          contentType: record.contentType,
          recordTime: record.recordTime,
        });
        break;
      }
      case "Metadata": {
        const offset = BigInt(builder.length);
        const length = builder.writeMetadata(record);
        metadataIndexes.push({ name: record.name, length, offset });
        break;
      }
    }
  }
  if (chunk) {
    const offset = BigInt(builder.length);
    const length = builder.writeChunk({
      compression: "",
      startTime: chunk.startTime,
      endTime: chunk.endTime,
      uncompressedCrc: 0,
      uncompressedSize: BigInt(chunk.buffer.byteLength),
      records: chunk.buffer,
    });
    chunkIndexes.push({
      compression: "",
      startTime: chunk.startTime,
      endTime: chunk.endTime,
      uncompressedSize: BigInt(chunk.buffer.byteLength),
      compressedSize: BigInt(chunk.buffer.byteLength),
      chunkStart: offset,
      chunkLength: length,
      messageIndexLength: 0n, //TODO
      messageIndexOffsets: new Map(), //TODO
    });
  }
  //TODO: data end
  const summaryStart = BigInt(builder.length);

  // TODO: UseRepeatedChannelInfos

  if (variant.has(TestFeatures.UseStatistics)) {
    builder.writeStatistics({
      attachmentCount,
      chunkCount,
      messageCount,
      channelCount,
      channelMessageCounts,
    });
  }

  // metadata indexes
  const metadataIndexStart = BigInt(builder.length);
  for (const record of metadataIndexes) {
    builder.writeMetadataIndex(record);
  }
  const metadataIndexLength = BigInt(builder.length) - metadataIndexStart;

  // attachment indexes
  const attachmentIndexStart = BigInt(builder.length);
  for (const record of attachmentIndexes) {
    builder.writeAttachmentIndex(record);
  }
  const attachmentIndexLength = BigInt(builder.length) - attachmentIndexStart;

  // chunk indexes
  const chunkIndexStart = BigInt(builder.length);
  for (const record of chunkIndexes) {
    builder.writeChunkIndex(record);
  }
  const chunkIndexLength = BigInt(builder.length) - chunkIndexStart;

  const hasSummary = BigInt(builder.length) === summaryStart;

  // summary offsets
  let summaryOffsetStart = 0n;
  if (variant.has(TestFeatures.UseSummaryOffset)) {
    summaryOffsetStart = BigInt(builder.length);
    builder.writeSummaryOffset({
      groupOpcode: Opcode.METADATA_INDEX,
      groupStart: metadataIndexStart,
      groupLength: metadataIndexLength,
    });
    builder.writeSummaryOffset({
      groupOpcode: Opcode.ATTACHMENT_INDEX,
      groupStart: attachmentIndexStart,
      groupLength: attachmentIndexLength,
    });
    builder.writeSummaryOffset({
      groupOpcode: Opcode.CHUNK_INDEX,
      groupStart: chunkIndexStart,
      groupLength: chunkIndexLength,
    });
  }

  //TODO: statistics

  builder.writeFooter({
    summaryOffsetStart,
    summaryStart: hasSummary ? summaryStart : 0n,
    crc: 0,
  });
  builder.writeMagic();
  return builder.buffer;
}

interface TestRunner {
  // supports(variant: Set<TestFeatures>): boolean;

  readonly name: string;

  readonly supportsDataOnly: boolean;
  readonly supportsDataAndSummary: boolean;
  readonly supportsDataAndSummaryWithOffsets: boolean;
  run(filePath: string): Promise<string[]>;
}

class TypescriptStreamedTestRunner implements TestRunner {
  name = "ts-stream";
  supportsDataOnly = true;
  supportsDataAndSummary = true;
  supportsDataAndSummaryWithOffsets = true;
  async run(filePath: string): Promise<string[]> {
    const result = [];
    const reader = new Mcap0StreamReader({ validateCrcs: true });
    reader.append(await fs.readFile(filePath));
    let record;
    while ((record = reader.nextRecord())) {
      result.push(
        JSON.stringify(record, (_key, value) =>
          // eslint-disable-next-line @typescript-eslint/no-unsafe-return
          typeof value === "bigint" ? `BigInt(${value})` : value,
        ),
      );
    }
    if (!reader.done()) {
      throw new Error("Reader not done");
    }
    return result;
  }
}
const inputs: { name: string; records: TestDataRecord[] }[] = [{ name: "NoData", records: [] }];

const runners: TestRunner[] = [new TypescriptStreamedTestRunner()];

async function main(options: { testDir: string; runner: string; update: boolean }) {
  const runner = runners.find((r) => r.name === options.runner);
  if (!runner) {
    throw new Error(`No runner named ${options.runner}`);
  }
  await fs.mkdir(options.testDir);

  for (const { name, records } of inputs) {
    for (const variant of generateVariants(...Object.values(TestFeatures))) {
      // validate that variant features make sense for the data
      if (
        variant.has(TestFeatures.UseAttachmentIndex) &&
        !records.some((record) => record.type === "Attachment")
      ) {
        continue;
      }
      if (
        variant.has(TestFeatures.UseMetadataIndex) &&
        !records.some((record) => record.type === "Metadata")
      ) {
        continue;
      }
      if (
        variant.has(TestFeatures.UseRepeatedChannelInfos) &&
        !records.some((record) => record.type === "ChannelInfo")
      ) {
        continue;
      }
      if (
        !records.some((record) => record.type === "Message" || record.type === "ChannelInfo") &&
        (variant.has(TestFeatures.UseChunks) ||
          variant.has(TestFeatures.UseChunkIndex) ||
          variant.has(TestFeatures.UseMessageIndex))
      ) {
        continue;
      }
      if (
        variant.has(TestFeatures.UseSummaryOffset) &&
        !(
          variant.has(TestFeatures.UseChunkIndex) ||
          variant.has(TestFeatures.UseRepeatedChannelInfos) ||
          variant.has(TestFeatures.UseMetadataIndex) ||
          variant.has(TestFeatures.UseAttachmentIndex) ||
          variant.has(TestFeatures.UseStatistics)
        )
      ) {
        continue;
      }

      const prefix = [name, ...Array.from(variant).sort()].join("-");

      const data = generateFile(variant, records);
      const filePath = path.join(options.testDir, `${prefix}.mcap`);
      const expectedOutputPath = path.join(options.testDir, `${prefix}.txt`);
      console.log("running", filePath);
      await fs.writeFile(filePath, data);
      const output = await runner.run(filePath);
      if (options.update) {
        await fs.writeFile(expectedOutputPath, output.join("\n"));
      } else {
        const expectedOutput = await fs.readFile(expectedOutputPath, { encoding: "utf-8" });
        if (output.join("\n") !== expectedOutput) {
          throw new Error("output did not match expected");
        }
      }
    }
  }
}

/*

channel info, then message (in same chunk)
channel info, then message (in different chunk)
channel info (duplicated in both chunks), then message (in 2nd chunk)
*/
// class TypescriptIndexedTestRunner implements TestRunner {
//   supportsDataOnly = false;
//   supportsDataAndSummary = true;
//   supportsDataAndSummaryWithOffsets = true;
//   async run(filePath: string) {}
// }
/*

# NoData-sum.yaml

expected-output:
  - "expected output for passed runners"
expect:
  ts-stream: pass
  ts-index: fail
  cpp: unsupported
  py: fail

*/

program
  .requiredOption("--test-dir <testDir>", "directory to output test data")
  .requiredOption("--runner <runnner>", "test runner to use")
  .option("--update", "update expected output files", false)
  .action(async (options) => {
    // eslint-disable-next-line @typescript-eslint/no-unsafe-argument
    await main(options);
  })
  .parse();
