import { Mcap0Types, Mcap0Constants, Mcap0RecordBuilder, Mcap0ChunkBuilder } from "@foxglove/mcap";
import { program } from "commander";
import fs from "fs/promises";
import path from "path";
import generateTestVariants from "variants/generateTestVariants";

import { collect } from "../util/collect";
import listDirRecursive from "../util/listDirRecursive";
import { TestDataRecord, TestFeatures } from "../variants/types";

type MetadataIndex = Mcap0Types.MetadataIndex;
type ChunkIndex = Mcap0Types.ChunkIndex;
type AttachmentIndex = Mcap0Types.AttachmentIndex;

function generateFile(features: Set<TestFeatures>, records: TestDataRecord[]) {
  const builder = new Mcap0RecordBuilder({
    padRecords: features.has(TestFeatures.AddExtraDataToRecords),
  });
  builder.writeMagic();
  builder.writeHeader({ profile: "", library: "" });

  const chunk = features.has(TestFeatures.UseChunks) ? new Mcap0ChunkBuilder() : undefined;
  const chunkCount = chunk ? 1 : 0;

  const metadataIndexes: MetadataIndex[] = [];
  const attachmentIndexes: AttachmentIndex[] = [];
  const chunkIndexes: ChunkIndex[] = [];

  let messageCount = 0n;
  let channelCount = 0;
  let schemaCount = 0;
  let attachmentCount = 0;
  let metadataCount = 0;
  const channelMessageCounts = new Map<number, bigint>();

  for (const record of records) {
    switch (record.type) {
      case "Schema":
        schemaCount++;
        if (chunk) {
          chunk.addSchema(record);
        } else {
          builder.writeSchema(record);
        }
        break;
      case "Channel":
        channelCount++;
        if (chunk) {
          chunk.addChannel(record);
        } else {
          builder.writeChannel(record);
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
        if (features.has(TestFeatures.UseAttachmentIndex)) {
          attachmentIndexes.push({
            name: record.name,
            length,
            offset,
            dataSize: BigInt(record.data.byteLength),
            contentType: record.contentType,
            logTime: record.logTime,
          });
        }
        break;
      }
      case "Metadata": {
        metadataCount++;
        const offset = BigInt(builder.length);
        const length = builder.writeMetadata(record);
        if (features.has(TestFeatures.UseMetadataIndex)) {
          metadataIndexes.push({ name: record.name, length, offset });
        }
        break;
      }
    }
  }
  if (chunk) {
    const chunkStartOffset = BigInt(builder.length);
    const chunkLength = builder.writeChunk({
      compression: "",
      startTime: chunk.startTime,
      endTime: chunk.endTime,
      uncompressedCrc: 0,
      uncompressedSize: BigInt(chunk.buffer.byteLength),
      records: chunk.buffer,
    });
    const messageIndexOffsets = new Map<number, bigint>();
    let messageIndexLength = 0n;
    if (features.has(TestFeatures.UseMessageIndex)) {
      for (const index of chunk.indices) {
        messageIndexOffsets.set(index.channelId, BigInt(builder.length));
        messageIndexLength += builder.writeMessageIndex(index);
      }
    }
    chunkIndexes.push({
      compression: "",
      startTime: chunk.startTime,
      endTime: chunk.endTime,
      uncompressedSize: BigInt(chunk.buffer.byteLength),
      compressedSize: BigInt(chunk.buffer.byteLength),
      chunkStartOffset,
      chunkLength,
      messageIndexLength,
      messageIndexOffsets,
    });
  }

  builder.writeDataEnd({ dataSectionCrc: 0 });

  const summaryStart = BigInt(builder.length);

  const repeatedSchemasStart = BigInt(builder.length);
  if (features.has(TestFeatures.UseRepeatedSchemas)) {
    for (const record of records) {
      if (record.type === "Schema") {
        builder.writeSchema(record);
      }
    }
  }
  const repeatedSchemasLength = BigInt(builder.length) - repeatedSchemasStart;

  const repeatedChannelInfosStart = BigInt(builder.length);
  if (features.has(TestFeatures.UseRepeatedChannelInfos)) {
    for (const record of records) {
      if (record.type === "Channel") {
        builder.writeChannel(record);
      }
    }
  }
  const repeatedChannelInfosLength = BigInt(builder.length) - repeatedChannelInfosStart;

  const statisticsStart = BigInt(builder.length);
  if (features.has(TestFeatures.UseStatistics)) {
    builder.writeStatistics({
      messageCount,
      channelCount,
      schemaCount,
      attachmentCount,
      metadataCount,
      chunkCount,
      channelMessageCounts,
    });
  }
  const statisticsLength = BigInt(builder.length) - statisticsStart;

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

  const hasSummary = BigInt(builder.length) !== summaryStart;

  // summary offsets
  let summaryOffsetStart = 0n;
  if (features.has(TestFeatures.UseSummaryOffset)) {
    summaryOffsetStart = BigInt(builder.length);
    if (repeatedSchemasLength !== 0n) {
      builder.writeSummaryOffset({
        groupOpcode: Mcap0Constants.Opcode.SCHEMA,
        groupStart: repeatedSchemasStart,
        groupLength: repeatedSchemasLength,
      });
    }
    if (repeatedChannelInfosLength !== 0n) {
      builder.writeSummaryOffset({
        groupOpcode: Mcap0Constants.Opcode.CHANNEL,
        groupStart: repeatedChannelInfosStart,
        groupLength: repeatedChannelInfosLength,
      });
    }
    if (metadataIndexLength !== 0n) {
      builder.writeSummaryOffset({
        groupOpcode: Mcap0Constants.Opcode.METADATA_INDEX,
        groupStart: metadataIndexStart,
        groupLength: metadataIndexLength,
      });
    }
    if (statisticsLength !== 0n) {
      builder.writeSummaryOffset({
        groupOpcode: Mcap0Constants.Opcode.STATISTICS,
        groupStart: statisticsStart,
        groupLength: statisticsLength,
      });
    }
    if (attachmentIndexLength !== 0n) {
      builder.writeSummaryOffset({
        groupOpcode: Mcap0Constants.Opcode.ATTACHMENT_INDEX,
        groupStart: attachmentIndexStart,
        groupLength: attachmentIndexLength,
      });
    }
    if (chunkIndexLength !== 0n) {
      builder.writeSummaryOffset({
        groupOpcode: Mcap0Constants.Opcode.CHUNK_INDEX,
        groupStart: chunkIndexStart,
        groupLength: chunkIndexLength,
      });
    }
  }

  builder.writeFooter({
    summaryOffsetStart,
    summaryStart: hasSummary ? summaryStart : 0n,
    summaryCrc: 0,
  });
  builder.writeMagic();
  return builder.buffer;
}

async function main(options: { dataDir: string; verify: boolean }) {
  let hadError = false;
  await fs.mkdir(options.dataDir, { recursive: true });
  const unexpectedFilePaths = new Set(
    (await collect(listDirRecursive(options.dataDir)))
      .filter((name) => name.endsWith(".mcap"))
      .map((name) => path.join(options.dataDir, name)),
  );

  for (const { baseName, name: testName, records, features } of generateTestVariants()) {
    const testDir = path.join(options.dataDir, baseName);
    const filePath = path.join(options.dataDir, baseName, `${testName}.mcap`);
    const data = generateFile(features, records);

    unexpectedFilePaths.delete(filePath);

    if (options.verify) {
      try {
        const existingData = await fs.readFile(filePath);
        if (existingData.equals(data)) {
          console.log(`  ok         ${filePath}`);
        } else {
          console.log(`* outdated   ${filePath}`);
          hadError = true;
        }
      } catch (error) {
        console.log(`- missing    ${filePath}`);
        hadError = true;
      }
    } else {
      console.log("generated", filePath);
      await fs.mkdir(testDir, { recursive: true });
      await fs.writeFile(filePath, data);
    }
  }

  if (options.verify) {
    for (const filePath of unexpectedFilePaths) {
      console.log(`+ unexpected ${filePath}`);
      hadError = true;
    }
  }

  if (hadError) {
    process.exit(1);
  }
}

program
  .requiredOption("--data-dir <dataDir>", "directory to output test data")
  .option("--verify", "verify generated tests are up to date")
  .action(main)
  .parse();
