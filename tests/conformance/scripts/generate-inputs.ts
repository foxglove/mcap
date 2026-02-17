import { crc32, crc32Init, crc32Update, crc32Final } from "@foxglove/crc";
import { McapRecordBuilder, McapChunkBuilder, Opcode } from "@mcap/core";
import type { AttachmentIndex, ChunkIndex, MetadataIndex } from "@mcap/core";
import { program } from "commander";
import fs from "node:fs/promises";
import path from "node:path";

import { collect } from "../util/collect.ts";
import listDirRecursive from "../util/listDirRecursive.ts";
import generateTestVariants from "../variants/generateTestVariants.ts";
import { TestFeatures } from "../variants/types.ts";
import type { TestDataRecord, TestFeature } from "../variants/types.ts";

function generateFile(features: Set<TestFeature>, records: TestDataRecord[]) {
  const builder = new McapRecordBuilder({
    padRecords: features.has(TestFeatures.AddExtraDataToRecords),
  });
  builder.writeMagic();
  builder.writeHeader({ profile: "", library: "" });

  const chunk = features.has(TestFeatures.UseChunks)
    ? new McapChunkBuilder({ useMessageIndex: features.has(TestFeatures.UseMessageIndex) })
    : undefined;
  const chunkCount = chunk ? 1 : 0;

  const metadataIndexes: MetadataIndex[] = [];
  const attachmentIndexes: AttachmentIndex[] = [];
  const chunkIndexes: ChunkIndex[] = [];

  let messageCount = 0n;
  let channelCount = 0;
  let schemaCount = 0;
  let attachmentCount = 0;
  let metadataCount = 0;
  let messageStartTime: bigint | undefined;
  let messageEndTime: bigint | undefined;
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
        if (messageStartTime == undefined || record.logTime < messageStartTime) {
          messageStartTime = record.logTime;
        }
        if (messageEndTime == undefined || record.logTime > messageEndTime) {
          messageEndTime = record.logTime;
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
            mediaType: record.mediaType,
            logTime: record.logTime,
            createTime: record.createTime,
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
      messageStartTime: chunk.messageStartTime,
      messageEndTime: chunk.messageEndTime,
      uncompressedCrc: crc32(chunk.buffer),
      uncompressedSize: BigInt(chunk.buffer.byteLength),
      records: chunk.buffer,
    });
    const messageIndexOffsets = new Map<number, bigint>();
    let messageIndexLength = 0n;
    for (const index of chunk.indices) {
      messageIndexOffsets.set(index.channelId, BigInt(builder.length));
      messageIndexLength += builder.writeMessageIndex(index);
    }
    if (features.has(TestFeatures.UseChunkIndex)) {
      chunkIndexes.push({
        compression: "",
        messageStartTime: chunk.messageStartTime,
        messageEndTime: chunk.messageEndTime,
        uncompressedSize: BigInt(chunk.buffer.byteLength),
        compressedSize: BigInt(chunk.buffer.byteLength),
        chunkStartOffset,
        chunkLength,
        messageIndexLength,
        messageIndexOffsets,
      });
    }
  }

  builder.writeDataEnd({ dataSectionCrc: crc32(builder.buffer) });

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
      messageStartTime: messageStartTime ?? 0n,
      messageEndTime: messageEndTime ?? 0n,
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
        groupOpcode: Opcode.SCHEMA,
        groupStart: repeatedSchemasStart,
        groupLength: repeatedSchemasLength,
      });
    }
    if (repeatedChannelInfosLength !== 0n) {
      builder.writeSummaryOffset({
        groupOpcode: Opcode.CHANNEL,
        groupStart: repeatedChannelInfosStart,
        groupLength: repeatedChannelInfosLength,
      });
    }
    if (statisticsLength !== 0n) {
      builder.writeSummaryOffset({
        groupOpcode: Opcode.STATISTICS,
        groupStart: statisticsStart,
        groupLength: statisticsLength,
      });
    }
    if (metadataIndexLength !== 0n) {
      builder.writeSummaryOffset({
        groupOpcode: Opcode.METADATA_INDEX,
        groupStart: metadataIndexStart,
        groupLength: metadataIndexLength,
      });
    }
    if (attachmentIndexLength !== 0n) {
      builder.writeSummaryOffset({
        groupOpcode: Opcode.ATTACHMENT_INDEX,
        groupStart: attachmentIndexStart,
        groupLength: attachmentIndexLength,
      });
    }
    if (chunkIndexLength !== 0n) {
      builder.writeSummaryOffset({
        groupOpcode: Opcode.CHUNK_INDEX,
        groupStart: chunkIndexStart,
        groupLength: chunkIndexLength,
      });
    }
  }

  let summaryCrc = crc32Init();
  const buffer = builder.buffer;
  const summaryData = new Uint8Array(
    buffer.buffer,
    buffer.byteOffset + Number(summaryStart),
    buffer.byteLength - Number(summaryStart),
  );
  summaryCrc = crc32Update(summaryCrc, summaryData);
  const tempBuffer = new DataView(new ArrayBuffer(1 + 8 + 8 + 8));
  tempBuffer.setUint8(0, Opcode.FOOTER);
  tempBuffer.setBigUint64(1, 8n + 8n + 4n, true);
  tempBuffer.setBigUint64(1 + 8, hasSummary ? summaryStart : 0n, true);
  tempBuffer.setBigUint64(1 + 8 + 8, summaryOffsetStart, true);
  summaryCrc = crc32Update(summaryCrc, tempBuffer);
  summaryCrc = crc32Final(summaryCrc);

  builder.writeFooter({
    summaryStart: hasSummary ? summaryStart : 0n,
    summaryOffsetStart,
    summaryCrc,
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
