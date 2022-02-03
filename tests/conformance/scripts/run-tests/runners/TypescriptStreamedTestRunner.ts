import { Mcap0StreamReader, Mcap0Types } from "@foxglove/mcap";
import fs from "fs/promises";

import ITestRunner from "./ITestRunner";

function stringifyRecord(record: Mcap0Types.TypedMcapRecord): string {
  function stringifyFields(fields: [string | bigint | number, string | bigint | number][]): string {
    return fields.map(([name, value]) => `${name}=${value === "" ? `""` : value}`).join(" ");
  }
  function stringifyData(data: Uint8Array): string {
    let result = "";
    for (const value of data) {
      result += value.toString(16).padStart(2, "0");
    }
    return `<${result}>`;
  }
  switch (record.type) {
    case "Chunk":
    case "DataEnd":
      throw new Error(`${record.type} record not expected in conformance test output`);

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
          ["summary_start", record.summaryStart],
          ["summary_offset_start", record.summaryOffsetStart],
        ])
      );
    case "ChannelInfo":
      return (
        "ChannelInfo " +
        stringifyFields([
          ["id", record.channelId],
          ["topic", record.topicName],
          ["message_encoding", record.messageEncoding],
          ["schema_encoding", record.schemaEncoding],
          ["schema", record.schema],
          ["schema_name", record.schemaName],
          ["metadata", "{" + stringifyFields(record.userData) + "}"],
        ])
      );
    case "Message":
      return (
        "Message " +
        stringifyFields([
          ["channel_id", record.channelId],
          ["sequence", record.sequence],
          ["publish_time", record.publishTime],
          ["log_time", record.recordTime],
          ["message_data", stringifyData(record.messageData)],
        ])
      );
    case "MessageIndex":
      return (
        "MessageIndex " +
        stringifyFields([
          ["channel_id", record.channelId],
          ["records", "{" + stringifyFields(record.records) + "}"],
        ])
      );
    case "ChunkIndex":
      return (
        "ChunkIndex " +
        stringifyFields([
          ["start_time", record.startTime],
          ["end_time", record.endTime],
          ["chunk_start_offset", record.chunkStart],
          ["chunk_length", record.chunkLength],
          [
            "message_index_offsets",
            "{" +
              stringifyFields(Array.from(record.messageIndexOffsets).sort((a, b) => a[0] - b[0])) +
              "}",
          ],
          ["message_index_length", record.messageIndexLength],
          ["compression", record.compression],
          ["compressed_size", record.compressedSize],
          ["uncompressed_size", record.uncompressedSize],
        ])
      );
    case "Attachment":
      return (
        "Attachment " +
        stringifyFields([
          ["name", record.name],
          ["created_at", record.createdAt],
          ["log_time", record.recordTime],
          ["content_type", record.contentType],
          ["data", stringifyData(record.data)],
        ])
      );
    case "AttachmentIndex":
      return (
        "AttachmentIndex " +
        stringifyFields([
          ["offset", record.offset],
          ["length", record.attachmentRecordLength],
          ["log_time", record.recordTime],
          ["data_size", record.attachmentSize],
          ["name", record.name],
          ["content_type", record.contentType],
        ])
      );
    case "Statistics":
      return (
        "Statistics " +
        stringifyFields([
          ["message_count", record.messageCount],
          ["channel_count", record.channelCount],
          ["attachment_count", record.attachmentCount],
          ["chunk_count", record.chunkCount],
          [
            "channel_message_counts",
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
          ["group_opcode", record.groupOpcode],
          ["group_start", record.groupStart],
          ["group_length", record.groupLength],
        ])
      );
    case "Unknown":
      return "Unknown " + stringifyFields([["op", record.opcode]]);
  }
}

export default class TypescriptStreamedTestRunner implements ITestRunner {
  name = "ts-stream";
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
