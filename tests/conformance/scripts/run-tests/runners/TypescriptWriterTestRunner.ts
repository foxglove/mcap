import { Mcap0Writer, Mcap0Types } from "@foxglove/mcap";
import fs from "fs/promises";
import { camelCase } from "lodash";
import { TestFeatures, TestVariant } from "variants/types";

import ITestRunner from "./ITestRunner";

type JsonValue<T> = T extends number | bigint | string
  ? string
  : T extends Uint8Array
  ? number[]
  : T extends Map<infer K, infer V>
  ? K extends number | bigint | string
    ? Record<JsonValue<K>, JsonValue<V>>
    : never
  : never;

type JsonRecord<R extends keyof Mcap0Types.McapRecords> = {
  type: R;
  fields: {
    [K in keyof Mcap0Types.McapRecords[R]]: JsonValue<Mcap0Types.McapRecords[R][K]>;
  };
};

type JsonRecords = {
  [R in keyof Mcap0Types.McapRecords]: JsonRecord<R>;
};

function parseJsonRecord(record: {
  fields: Array<[string, unknown]>;
}): Mcap0Types.TypedMcapRecord | undefined {
  const jsonRecord = {
    ...record,
    fields: Object.fromEntries(record.fields.map(([k, v]) => [camelCase(k), v])),
  } as JsonRecords[keyof JsonRecords];

  switch (jsonRecord.type) {
    case "Header":
      return {
        type: jsonRecord.type,
        library: jsonRecord.fields.library,
        profile: jsonRecord.fields.profile,
      };
    case "Footer":
      return {
        type: jsonRecord.type,
        summaryStart: BigInt(jsonRecord.fields.summaryStart),
        summaryOffsetStart: BigInt(jsonRecord.fields.summaryOffsetStart),
        summaryCrc: Number(jsonRecord.fields.summaryCrc),
      };
    case "Schema":
      return {
        type: jsonRecord.type,
        id: Number(jsonRecord.fields.id),
        name: jsonRecord.fields.name,
        encoding: jsonRecord.fields.encoding,
        data: Uint8Array.from(jsonRecord.fields.data),
      };
    case "Channel":
      return {
        type: jsonRecord.type,
        id: Number(jsonRecord.fields.id),
        schemaId: Number(jsonRecord.fields.schemaId),
        topic: jsonRecord.fields.topic,
        messageEncoding: jsonRecord.fields.messageEncoding,
        metadata: new Map(Object.entries(jsonRecord.fields.metadata)),
      };
    case "Message":
      return {
        type: jsonRecord.type,
        channelId: Number(jsonRecord.fields.channelId),
        sequence: Number(jsonRecord.fields.sequence),
        logTime: BigInt(jsonRecord.fields.logTime),
        publishTime: BigInt(jsonRecord.fields.publishTime),
        data: Uint8Array.from(jsonRecord.fields.data),
      };
    case "DataEnd":
      return {
        type: jsonRecord.type,
        dataSectionCrc: Number(jsonRecord.fields.dataSectionCrc),
      };
    case "Chunk":
    case "Unknown":
      throw new Error(`${jsonRecord.type} not expected in writer test input`);

    case "Attachment":
      return {
        type: jsonRecord.type,
        name: jsonRecord.fields.name,
        createdAt: BigInt(jsonRecord.fields.createdAt),
        logTime: BigInt(jsonRecord.fields.logTime),
        contentType: jsonRecord.fields.contentType,
        data: Uint8Array.from(jsonRecord.fields.data),
      };
    case "MessageIndex":
    case "ChunkIndex":
    case "AttachmentIndex":
    case "Statistics":
    case "MetadataIndex":
    case "SummaryOffset":
      break;
    case "Metadata":
      return {
        type: jsonRecord.type,
        name: jsonRecord.fields.name,
        metadata: new Map(Object.entries(jsonRecord.fields.metadata)),
      };
  }
  return undefined;
}

export default class TypescriptWriterTestRunner implements ITestRunner {
  readonly name = "ts-writer";
  readonly mode = "write";

  supportsVariant(variant: TestVariant): boolean {
    if (variant.features.has(TestFeatures.AddExtraDataToRecords)) {
      return false;
    }
    return true;
  }

  async run(filePath: string, variant: TestVariant): Promise<string> {
    const json = await fs.readFile(filePath, { encoding: "utf-8" });
    const jsonRecords = (
      JSON.parse(json) as { records: Array<{ fields: Array<[string, unknown]> }> }
    ).records;

    const buffer = new Uint8Array(4096);
    let usedBytes = 0;
    const writable = {
      position: () => BigInt(usedBytes),
      async write(input: Uint8Array): Promise<void> {
        if (usedBytes + input.byteLength > buffer.byteLength) {
          const newBuffer = new Uint8Array(usedBytes + input.byteLength);
          newBuffer.set(buffer);
        }
        buffer.set(input, usedBytes);
        usedBytes += input.byteLength;
      },
    };
    const writer = new Mcap0Writer({
      writable,
      startChannelId: 1,
      useStatistics: variant.features.has(TestFeatures.UseStatistics),
      useSummaryOffsets: variant.features.has(TestFeatures.UseSummaryOffset),
      useChunks: variant.features.has(TestFeatures.UseChunks),
      repeatSchemas: variant.features.has(TestFeatures.UseRepeatedSchemas),
      repeatChannels: variant.features.has(TestFeatures.UseRepeatedChannelInfos),
      useAttachmentIndex: variant.features.has(TestFeatures.UseAttachmentIndex),
      useMetadataIndex: variant.features.has(TestFeatures.UseMetadataIndex),
      useMessageIndex: variant.features.has(TestFeatures.UseMessageIndex),
      useChunkIndex: variant.features.has(TestFeatures.UseChunkIndex),
    });

    const schemaIdMap = new Map<number, number>();
    const channelIdMap = new Map<number, number>();
    for (const jsonRecord of jsonRecords) {
      const record = parseJsonRecord(jsonRecord);
      if (!record) {
        continue;
      }
      switch (record.type) {
        case "Header":
          await writer.start(record);
          break;
        case "DataEnd":
          await writer.end();
          break;
        case "Schema": {
          const newSchemaId = await writer.registerSchema(record);
          schemaIdMap.set(record.id, newSchemaId);
          break;
        }
        case "Channel": {
          const schemaId = record.schemaId === 0 ? 0 : schemaIdMap.get(record.schemaId);
          if (schemaId == undefined) {
            throw new Error(`Never saw schema with id ${record.schemaId}`);
          }
          const newChannelId = await writer.registerChannel({ ...record, schemaId });
          channelIdMap.set(record.id, newChannelId);
          break;
        }
        case "Message": {
          const channelId = channelIdMap.get(record.channelId);
          if (channelId == undefined) {
            throw new Error(`Never saw channel with id ${record.channelId}`);
          }
          await writer.addMessage({ ...record, channelId });
          break;
        }
        case "Attachment":
          await writer.addAttachment(record);
          break;
        case "Metadata":
          await writer.addMetadata(record);
          break;
        case "Chunk":
        case "MessageIndex":
        case "ChunkIndex":
        case "AttachmentIndex":
        case "Statistics":
        case "MetadataIndex":
        case "SummaryOffset":
        case "Footer":
          break;
        case "Unknown":
          throw new Error("unknown records not supported");
      }
    }

    return Array.from(new Uint8Array(buffer.buffer, 0, usedBytes))
      .map((b) => b.toString(16).padStart(2, "0"))
      .join("");
  }
}
