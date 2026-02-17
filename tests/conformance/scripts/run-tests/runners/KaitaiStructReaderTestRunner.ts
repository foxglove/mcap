import { Opcode } from "@mcap/core";
import type { TypedMcapRecord } from "@mcap/core";
import YAML from "js-yaml";
import { KaitaiStream } from "kaitai-struct";
import KaitaiStructCompiler from "kaitai-struct-compiler";
import fs from "node:fs/promises";
import path from "node:path";

import { StreamedReadTestRunner } from "./TestRunner.ts";
import type { TestVariant } from "../../../variants/types.ts";
import { toSerializableMcapRecord } from "../toSerializableMcapRecord.ts";
import type { StreamedReadTestResult } from "../types.ts";

type ParsedRecord =
  | {
      op: Opcode.HEADER;
      body: { profile: { str: string }; library: { str: string } };
    }
  | {
      op: Opcode.FOOTER;
      body: { ofsSummarySection: bigint; ofsSummaryOffsetSection: bigint; summaryCrc32: number };
    }
  | {
      op: Opcode.SCHEMA;
      body: {
        id: number;
        name: { str: string };
        encoding: { str: string };
        lenData: number;
        data: Uint8Array;
      };
    }
  | {
      op: Opcode.CHANNEL;
      body: {
        id: number;
        schemaId: number;
        topic: { str: string };
        messageEncoding: { str: string };
        metadata: { entries: { entries: Array<{ key: { str: string }; value: { str: string } }> } };
      };
    }
  | {
      op: Opcode.MESSAGE;
      body: {
        channelId: number;
        sequence: number;
        logTime: bigint;
        publishTime: bigint;
        data: Uint8Array;
      };
    }
  | {
      op: Opcode.CHUNK;
      body: { compression: { str: string }; records?: { records: ParsedRecord[] } };
    }
  | {
      op: Opcode.MESSAGE_INDEX;
      body: { channelId: number; records: { entries: Array<{ id: number; offset: number }> } };
    }
  | {
      op: Opcode.CHUNK_INDEX;
      body: {
        messageStartTime: bigint;
        messageEndTime: bigint;
        ofsChunk: bigint;
        lenChunk: bigint;
        messageIndexOffsets: { entries: Array<{ channelId: number; offset: bigint }> };
        messageIndexLength: bigint;
        compression: { str: string };
        compressedSize: bigint;
        uncompressedSize: bigint;
      };
    }
  | {
      op: Opcode.ATTACHMENT;
      body: {
        logTime: bigint;
        createTime: bigint;
        name: { str: string };
        mediaType: { str: string };
        data: Uint8Array;
        crc32: number;
      };
    }
  | {
      op: Opcode.ATTACHMENT_INDEX;
      body: {
        ofsAttachment: bigint;
        lenAttachment: bigint;
        logTime: bigint;
        createTime: bigint;
        dataSize: bigint;
        name: { str: string };
        mediaType: { str: string };
      };
    }
  | {
      op: Opcode.STATISTICS;
      body: {
        messageCount: bigint;
        schemaCount: number;
        channelCount: number;
        attachmentCount: number;
        metadataCount: number;
        chunkCount: number;
        messageStartTime: bigint;
        messageEndTime: bigint;
        channelMessageCounts: { entries: Array<{ channelId: number; messageCount: bigint }> };
      };
    }
  | {
      op: Opcode.METADATA;
      body: {
        name: { str: string };
        metadata: { entries: { entries: Array<{ key: { str: string }; value: { str: string } }> } };
      };
    }
  | {
      op: Opcode.METADATA_INDEX;
      body: { ofsMetadata: bigint; lenMetadata: bigint; name: { str: string } };
    }
  | {
      op: Opcode.SUMMARY_OFFSET;
      body: { groupOpcode: number; ofsGroup: bigint; lenGroup: bigint };
    }
  | { op: Opcode.DATA_END; body: { dataSectionCrc32: number } };

type Mcap = {
  new (_: KaitaiStream): Mcap;
  headerMagic: Uint8Array;
  footerMagic: Uint8Array;
  records: ParsedRecord[];
};

let mcapClass: Mcap | undefined;
async function compileMcapClass(): Promise<Mcap> {
  if (mcapClass) {
    return mcapClass;
  }
  // KaitaiStream implementation does not natively support bigint
  KaitaiStream.prototype.readU8le = function () {
    const lo = this.readU4le();
    const hi = this.readU4le();
    return (BigInt(hi) << 32n) | BigInt(lo);
  };
  const originalReadBytes = KaitaiStream.prototype.readBytes; // eslint-disable-line @typescript-eslint/unbound-method
  KaitaiStream.prototype.readBytes = function (len: number | bigint) {
    if (len > Number.MAX_SAFE_INTEGER) {
      throw new Error(`Cannot read ${len} bytes with Number precision`);
    }
    return originalReadBytes.call(this, Number(len));
  };
  const ksy = await fs.readFile(path.join(__dirname, "../../../../../website/docs/spec/mcap.ksy"), {
    encoding: "utf-8",
  });
  const compiler = new KaitaiStructCompiler();
  const files = await compiler.compile("javascript", YAML.load(ksy));
  const root = { KaitaiStream };
  new Function(files["Mcap.js"]!).call(root); // eslint-disable-line @typescript-eslint/no-implied-eval, no-new-func
  mcapClass = (root as Record<string, unknown>)["Mcap"] as Mcap;
  return mcapClass;
}

export default class KaitaiStructReaderTestRunner extends StreamedReadTestRunner {
  readonly name = "ksy-reader";

  supportsVariant(_variant: TestVariant): boolean {
    return true;
  }

  async runReadTest(filePath: string): Promise<StreamedReadTestResult> {
    const Mcap = await compileMcapClass();
    const fileData = new Uint8Array(await fs.readFile(filePath));
    const mcap = new Mcap(new KaitaiStream(fileData.buffer));

    const result: TypedMcapRecord[] = [];

    function addRecord(record: ParsedRecord) {
      switch (record.op) {
        case Opcode.MESSAGE_INDEX:
        default:
          break;

        case Opcode.HEADER:
          result.push({
            type: "Header",
            profile: record.body.profile.str,
            library: record.body.library.str,
          });
          break;
        case Opcode.FOOTER:
          result.push({
            type: "Footer",
            summaryStart: record.body.ofsSummarySection,
            summaryOffsetStart: record.body.ofsSummaryOffsetSection,
            summaryCrc: record.body.summaryCrc32,
          });
          break;
        case Opcode.SCHEMA:
          result.push({
            type: "Schema",
            id: record.body.id,
            name: record.body.name.str,
            encoding: record.body.encoding.str,
            data: record.body.data,
          });
          break;
        case Opcode.CHANNEL:
          result.push({
            type: "Channel",
            id: record.body.id,
            topic: record.body.topic.str,
            schemaId: record.body.schemaId,
            messageEncoding: record.body.messageEncoding.str,
            metadata: new Map(
              record.body.metadata.entries.entries.map(({ key, value }) => [key.str, value.str]),
            ),
          });
          break;
        case Opcode.MESSAGE:
          result.push({
            type: "Message",
            channelId: record.body.channelId,
            logTime: record.body.logTime,
            publishTime: record.body.publishTime,
            sequence: record.body.sequence,
            data: record.body.data,
          });
          break;
        case Opcode.CHUNK:
          if (record.body.records) {
            for (const rec of record.body.records.records) {
              addRecord(rec);
            }
          } else {
            throw new Error(`Unsupported compression: ${record.body.compression.str}`);
          }
          break;
        case Opcode.CHUNK_INDEX:
          result.push({
            type: "ChunkIndex",
            chunkStartOffset: record.body.ofsChunk,
            chunkLength: record.body.lenChunk,
            compressedSize: record.body.compressedSize,
            uncompressedSize: record.body.uncompressedSize,
            compression: record.body.compression.str,
            messageEndTime: record.body.messageEndTime,
            messageIndexLength: record.body.messageIndexLength,
            messageIndexOffsets: new Map(
              record.body.messageIndexOffsets.entries.map(({ channelId, offset }) => [
                channelId,
                offset,
              ]),
            ),
            messageStartTime: record.body.messageStartTime,
          });
          break;
        case Opcode.ATTACHMENT:
          result.push({
            type: "Attachment",
            name: record.body.name.str,
            mediaType: record.body.mediaType.str,
            logTime: record.body.logTime,
            createTime: record.body.createTime,
            data: record.body.data,
          });
          break;
        case Opcode.ATTACHMENT_INDEX:
          result.push({
            type: "AttachmentIndex",
            offset: record.body.ofsAttachment,
            length: record.body.lenAttachment,
            name: record.body.name.str,
            mediaType: record.body.mediaType.str,
            logTime: record.body.logTime,
            createTime: record.body.createTime,
            dataSize: record.body.dataSize,
          });
          break;
        case Opcode.STATISTICS:
          result.push({
            type: "Statistics",
            attachmentCount: record.body.attachmentCount,
            channelCount: record.body.channelCount,
            channelMessageCounts: new Map(
              record.body.channelMessageCounts.entries.map(({ channelId, messageCount }) => [
                channelId,
                messageCount,
              ]),
            ),
            chunkCount: record.body.chunkCount,
            messageCount: record.body.messageCount,
            messageEndTime: record.body.messageEndTime,
            messageStartTime: record.body.messageStartTime,
            metadataCount: record.body.metadataCount,
            schemaCount: record.body.schemaCount,
          });
          break;
        case Opcode.METADATA:
          result.push({
            type: "Metadata",
            name: record.body.name.str,
            metadata: new Map(
              record.body.metadata.entries.entries.map(({ key, value }) => [key.str, value.str]),
            ),
          });
          break;
        case Opcode.METADATA_INDEX:
          result.push({
            type: "MetadataIndex",
            offset: record.body.ofsMetadata,
            length: record.body.lenMetadata,
            name: record.body.name.str,
          });
          break;
        case Opcode.SUMMARY_OFFSET:
          result.push({
            type: "SummaryOffset",
            groupOpcode: record.body.groupOpcode,
            groupStart: record.body.ofsGroup,
            groupLength: record.body.lenGroup,
          });
          break;
        case Opcode.DATA_END:
          result.push({
            type: "DataEnd",
            dataSectionCrc: record.body.dataSectionCrc32,
          });
          break;
      }
    }
    for (const record of mcap.records) {
      addRecord(record);
    }
    return { records: result.map(toSerializableMcapRecord) };
  }
}
