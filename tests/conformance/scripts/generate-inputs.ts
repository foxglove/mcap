import { Mcap0Types, Mcap0Constants, Mcap0RecordBuilder, Mcap0ChunkBuilder } from "@foxglove/mcap";
import { program } from "commander";
import fs from "fs/promises";
import path from "path";

type MetadataIndex = Mcap0Types.MetadataIndex;
type ChunkIndex = Mcap0Types.ChunkIndex;
type AttachmentIndex = Mcap0Types.AttachmentIndex;

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

function* generateVariants(...features: TestFeatures[]): Generator<Set<TestFeatures>, void, void> {
  if (features.length === 0) {
    yield new Set();
    return;
  }
  for (const variant of generateVariants(...features.slice(1))) {
    yield variant;
    yield new Set([features[0]!, ...variant]);
  }
}

type TestDataRecord = Mcap0Types.TypedMcapRecords[
  | "Message"
  | "ChannelInfo"
  | "Attachment"
  | "Metadata"];

function generateFile(variant: Set<TestFeatures>, records: TestDataRecord[]) {
  const builder = new Mcap0RecordBuilder({
    padRecords: variant.has(TestFeatures.AddExtraDataToRecords),
  });
  builder.writeMagic();
  builder.writeHeader({ profile: "", library: "" });

  const chunk = variant.has(TestFeatures.UseChunks) ? new Mcap0ChunkBuilder() : undefined;
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
    const messageIndexOffsets = new Map<number, bigint>();
    let messageIndexLength = 0n;
    for (const index of chunk.indices) {
      messageIndexOffsets.set(index.channelId, BigInt(builder.length));
      messageIndexLength += builder.writeMessageIndex(index);
    }
    chunkIndexes.push({
      compression: "",
      startTime: chunk.startTime,
      endTime: chunk.endTime,
      uncompressedSize: BigInt(chunk.buffer.byteLength),
      compressedSize: BigInt(chunk.buffer.byteLength),
      chunkStart: offset,
      chunkLength: length,
      messageIndexLength,
      messageIndexOffsets,
    });
  }

  builder.writeDataEnd({ dataSectionCrc: 0 });

  const summaryStart = BigInt(builder.length);

  const repeatedChannelInfosStart = BigInt(builder.length);
  if (variant.has(TestFeatures.UseRepeatedChannelInfos)) {
    for (const record of records) {
      if (record.type === "ChannelInfo") {
        builder.writeChannelInfo(record);
      }
    }
  }
  const repeatedChannelInfosLength = BigInt(builder.length) - repeatedChannelInfosStart;

  const statisticsStart = BigInt(builder.length);
  if (variant.has(TestFeatures.UseStatistics)) {
    builder.writeStatistics({
      attachmentCount,
      chunkCount,
      messageCount,
      channelCount,
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
  if (variant.has(TestFeatures.UseSummaryOffset)) {
    summaryOffsetStart = BigInt(builder.length);
    if (repeatedChannelInfosLength !== 0n) {
      builder.writeSummaryOffset({
        groupOpcode: Mcap0Constants.Opcode.METADATA_INDEX,
        groupStart: repeatedChannelInfosStart,
        groupLength: repeatedChannelInfosLength,
      });
    }
    builder.writeSummaryOffset({
      groupOpcode: Mcap0Constants.Opcode.METADATA_INDEX,
      groupStart: metadataIndexStart,
      groupLength: metadataIndexLength,
    });
    if (statisticsLength !== 0n) {
      builder.writeSummaryOffset({
        groupOpcode: Mcap0Constants.Opcode.STATISTICS,
        groupStart: statisticsStart,
        groupLength: statisticsLength,
      });
    }
    builder.writeSummaryOffset({
      groupOpcode: Mcap0Constants.Opcode.ATTACHMENT_INDEX,
      groupStart: attachmentIndexStart,
      groupLength: attachmentIndexLength,
    });
    builder.writeSummaryOffset({
      groupOpcode: Mcap0Constants.Opcode.CHUNK_INDEX,
      groupStart: chunkIndexStart,
      groupLength: chunkIndexLength,
    });
  }

  builder.writeFooter({
    summaryOffsetStart,
    summaryStart: hasSummary ? summaryStart : 0n,
    crc: 0,
  });
  builder.writeMagic();
  return builder.buffer;
}

const inputs: { name: string; records: TestDataRecord[] }[] = [
  { name: "NoData", records: [] },
  {
    name: "OneMessage",
    records: [
      {
        type: "ChannelInfo",
        channelId: 1,
        topicName: "example",
        schemaName: "Example",
        messageEncoding: "a",
        schema: "b",
        schemaEncoding: "c",
        userData: [["foo", "bar"]],
      },
      {
        type: "Message",
        channelId: 1,
        publishTime: 1n,
        recordTime: 2n,
        messageData: new Uint8Array([1, 2, 3]),
        sequence: 10,
      },
    ],
  },
];

async function main(options: { dataDir: string; verify: boolean }) {
  let hadError = false;
  await fs.mkdir(options.dataDir, { recursive: true });
  const unexpectedFilePaths = new Set(
    (await fs.readdir(options.dataDir))
      .filter((name) => name.endsWith(".mcap"))
      .map((name) => path.join(options.dataDir, name)),
  );

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
      const filePath = path.join(options.dataDir, `${prefix}.mcap`);
      const data = generateFile(variant, records);

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
        await fs.writeFile(filePath, data);
      }
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
