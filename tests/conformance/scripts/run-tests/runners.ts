import { Mcap0StreamReader, Mcap0Types } from "@foxglove/mcap";
import fs from "fs/promises";

export interface TestRunner {
  readonly name: string;

  readonly supportsDataOnly: boolean;
  readonly supportsDataAndSummary: boolean;
  readonly supportsDataAndSummaryWithOffsets: boolean;
  run(filePath: string): Promise<string[]>;
}

function stringifyRecord(record: Mcap0Types.TypedMcapRecord): string {
  function stringifyFields(fields: [string | bigint | number, string | bigint | number][]): string {
    return fields.map(([name, value]) => `${name}=${value === "" ? `""` : value}`).join(" ");
  }
  function stringifyData(data: Uint8Array): string {
    let result = "";
    for (const value of data) {
      result += value.toString(16).padStart(2, "0");
    }
    return result;
  }
  switch (record.type) {
    case "Header":
      return (
        "Header " +
        stringifyFields([
          ["profile", record.profile],
          ["library", record.library],
        ])
      );
    case "Footer":
      return (
        "Footer " +
        stringifyFields([
          ["summaryStart", record.summaryStart],
          ["summaryOffsetStart", record.summaryOffsetStart],
        ])
      );
    case "ChannelInfo":
      return (
        "ChannelInfo " +
        stringifyFields([
          ["channelId", record.channelId],
          ["topicName", record.topicName],
          ["messageEncoding", record.messageEncoding],
          ["schemaEncoding", record.schemaEncoding],
          ["schema", record.schema],
          ["schemaName", record.schemaName],
          ["userData", "{" + stringifyFields(record.userData) + "}"],
        ])
      );
    case "Message":
      return (
        "Message " +
        stringifyFields([
          ["channelId", record.channelId],
          ["sequence", record.sequence],
          ["publishTime", record.publishTime],
          ["recordTime", record.recordTime],
          ["messageData", stringifyData(record.messageData)],
        ])
      );
    case "Chunk":
      throw new Error("TODO: how to handle chunks?");
    case "MessageIndex":
      return (
        "MessageIndex " +
        stringifyFields([
          ["channelId", record.channelId],
          ["count", record.count],
          ["records", "{" + stringifyFields(record.records) + "}"],
        ])
      );
    case "ChunkIndex":
      return (
        "ChunkIndex " +
        stringifyFields([
          ["startTime", record.startTime],
          ["endTime", record.endTime],
          ["chunkStart", record.chunkStart],
          ["chunkLength", record.chunkLength],
          [
            "messageIndexOffsets",
            "{" +
              stringifyFields(Array.from(record.messageIndexOffsets).sort((a, b) => a[0] - b[0])) +
              "}",
          ],
          ["messageIndexLength", record.messageIndexLength],
          ["compression", record.compression],
          ["compressedSize", record.compressedSize],
          ["uncompressedSize", record.uncompressedSize],
        ])
      );
    case "Attachment":
      return (
        "Attachment " +
        stringifyFields([
          ["name", record.name],
          ["recordTime", record.recordTime],
          ["contentType", record.contentType],
          ["data", stringifyData(record.data)],
        ])
      );
    case "AttachmentIndex":
      return (
        "AttachmentIndex " +
        stringifyFields([
          ["recordTime", record.recordTime],
          ["attachmentSize", record.attachmentSize],
          ["name", record.name],
          ["contentType", record.contentType],
          ["offset", record.offset],
          ["attachmentRecordLength", record.attachmentRecordLength],
        ])
      );
    case "Statistics":
      return (
        "Statistics " +
        stringifyFields([
          ["messageCount", record.messageCount],
          ["channelCount", record.channelCount],
          ["attachmentCount", record.attachmentCount],
          ["chunkCount", record.chunkCount],
          [
            "channelMessageCounts",
            "{" +
              stringifyFields(Array.from(record.channelMessageCounts).sort((a, b) => a[0] - b[0])) +
              "}",
          ],
        ])
      );
    case "Metadata":
      return (
        "Metadata " +
        stringifyFields([
          ["name", record.name],
          ["metadata", "{" + stringifyFields(record.metadata) + "}"],
        ])
      );
    case "MetadataIndex":
      return (
        "MetadataIndex " +
        stringifyFields([
          ["offset", record.offset],
          ["length", record.length],
          ["name", record.name],
        ])
      );
    case "SummaryOffset":
      return (
        "SummaryOffset " +
        stringifyFields([
          ["groupOpcode", record.groupOpcode],
          ["groupStart", record.groupStart],
          ["groupLength", record.groupLength],
        ])
      );
    case "Unknown":
      return "Unknown " + stringifyFields([["opcode", record.opcode]]);
  }
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
      result.push(stringifyRecord(record));
    }
    if (!reader.done()) {
      throw new Error("Reader not done");
    }
    return result;
  }
}

export default [new TypescriptStreamedTestRunner()];
