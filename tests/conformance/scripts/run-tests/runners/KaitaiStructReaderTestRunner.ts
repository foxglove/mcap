import { Mcap0Constants, Mcap0Types } from "@mcap/core";
import fs from "fs/promises";
import YAML from "js-yaml";
import { KaitaiStream } from "kaitai-struct";
import KaitaiStructCompiler from "kaitai-struct-compiler";
import path from "path";

import { TestVariant } from "../../../variants/types";
import { ReadTestRunner } from "./TestRunner";
import { stringifyRecords } from "./stringifyRecords";

// https://github.com/microsoft/TypeScript/issues/23182#issuecomment-379091887
type IsNever<T> = [T] extends [never] ? true : false;

/**
 * Transform a value to the representation of this value in the Kaitai Struct definition.
 *
 * @param MapKeyValue Indicates the key and value property names used in encoding the map entries.
 * For instance, `channelMessageCounts: Map<number, bigint>` is encoded as `{channelId: u2,
 * messageCount: u8}[]`.
 */
type ParsedValue<T, MapKeyValue extends [string, string] = never> = T extends number | bigint
  ? T
  : T extends string
  ? { str: string }
  : T extends Map<infer K, infer V>
  ? IsNever<MapKeyValue> extends true
    ? { entry: { entry: Array<{ key: ParsedValue<K>; value: ParsedValue<V> }> } }
    : {
        entry: Array<
          { [_ in MapKeyValue[0]]: ParsedValue<K> } & { [_ in MapKeyValue[1]]: ParsedValue<V> }
        >;
      }
  : T;

/** Transform a record type to the representation of this type in the Kaitai Struct definition. */
type ParsedBody<
  R extends keyof Mcap0Types.McapRecords,
  MapKeyValue extends [string, string] = never,
> = {
  [K in keyof Mcap0Types.McapRecords[R]]: ParsedValue<Mcap0Types.McapRecords[R][K], MapKeyValue>;
};

type ParsedRecord =
  | { op: Mcap0Constants.Opcode.HEADER; body: ParsedBody<"Header"> }
  | { op: Mcap0Constants.Opcode.FOOTER; body: ParsedBody<"Footer"> }
  | { op: Mcap0Constants.Opcode.SCHEMA; body: ParsedBody<"Schema"> }
  | { op: Mcap0Constants.Opcode.CHANNEL; body: ParsedBody<"Channel"> }
  | { op: Mcap0Constants.Opcode.MESSAGE; body: ParsedBody<"Message"> }
  | {
      op: Mcap0Constants.Opcode.CHUNK;
      body: { compression: ParsedValue<string>; records?: { records: ParsedRecord[] } };
    }
  | { op: Mcap0Constants.Opcode.MESSAGE_INDEX; body: ParsedBody<"MessageIndex"> }
  | {
      op: Mcap0Constants.Opcode.CHUNK_INDEX;
      body: ParsedBody<"ChunkIndex", ["channelId", "offset"]>;
    }
  | { op: Mcap0Constants.Opcode.ATTACHMENT; body: ParsedBody<"Attachment"> }
  | { op: Mcap0Constants.Opcode.ATTACHMENT_INDEX; body: ParsedBody<"AttachmentIndex"> }
  | {
      op: Mcap0Constants.Opcode.STATISTICS;
      body: ParsedBody<"Statistics", ["channelId", "messageCount"]>;
    }
  | { op: Mcap0Constants.Opcode.METADATA; body: ParsedBody<"Metadata"> }
  | { op: Mcap0Constants.Opcode.METADATA_INDEX; body: ParsedBody<"MetadataIndex"> }
  | { op: Mcap0Constants.Opcode.SUMMARY_OFFSET; body: ParsedBody<"SummaryOffset"> }
  | { op: Mcap0Constants.Opcode.DATA_END; body: ParsedBody<"DataEnd"> };

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
  const ksy = await fs.readFile(
    path.join(__dirname, "../../../../../docs/specification/mcap.ksy"),
    { encoding: "utf-8" },
  );
  const compiler = new KaitaiStructCompiler();
  const files = await compiler.compile("javascript", YAML.load(ksy));
  const root = { KaitaiStream };
  new Function(files["Mcap.js"]!).call(root); // eslint-disable-line @typescript-eslint/no-implied-eval, no-new-func
  mcapClass = (root as Record<string, unknown>)["Mcap"] as Mcap;
  return mcapClass;
}

export default class KaitaiStructReaderTestRunner extends ReadTestRunner {
  readonly name = "ksy-reader";
  readonly readsDataEnd = true;

  supportsVariant(_variant: TestVariant): boolean {
    return true;
  }

  async runReadTest(filePath: string, variant: TestVariant): Promise<string> {
    const Mcap = await compileMcapClass();
    const mcap = new Mcap(new KaitaiStream((await fs.readFile(filePath)).buffer));

    const result: Mcap0Types.TypedMcapRecord[] = [];

    function addRecord(record: ParsedRecord) {
      switch (record.op) {
        case Mcap0Constants.Opcode.MESSAGE_INDEX:
        default:
          break;

        case Mcap0Constants.Opcode.HEADER:
          result.push({
            type: "Header",
            profile: record.body.profile.str,
            library: record.body.library.str,
          });
          break;
        case Mcap0Constants.Opcode.FOOTER:
          result.push({
            type: "Footer",
            summaryStart: record.body.summaryStart,
            summaryOffsetStart: record.body.summaryOffsetStart,
            summaryCrc: record.body.summaryCrc,
          });
          break;
        case Mcap0Constants.Opcode.SCHEMA:
          result.push({
            type: "Schema",
            id: record.body.id,
            name: record.body.name.str,
            encoding: record.body.encoding.str,
            data: record.body.data,
          });
          break;
        case Mcap0Constants.Opcode.CHANNEL:
          result.push({
            type: "Channel",
            id: record.body.id,
            topic: record.body.topic.str,
            schemaId: record.body.schemaId,
            messageEncoding: record.body.messageEncoding.str,
            metadata: new Map(
              record.body.metadata.entry.entry.map(({ key, value }) => [key.str, value.str]),
            ),
          });
          break;
        case Mcap0Constants.Opcode.MESSAGE:
          result.push({
            type: "Message",
            channelId: record.body.channelId,
            logTime: record.body.logTime,
            publishTime: record.body.publishTime,
            sequence: record.body.sequence,
            data: record.body.data,
          });
          break;
        case Mcap0Constants.Opcode.CHUNK:
          if (record.body.records) {
            for (const rec of record.body.records.records) {
              addRecord(rec);
            }
          } else {
            throw new Error(`Unsupported compression: ${record.body.compression.str}`);
          }
          break;
        case Mcap0Constants.Opcode.CHUNK_INDEX:
          result.push({
            type: "ChunkIndex",
            chunkStartOffset: record.body.chunkStartOffset,
            chunkLength: record.body.chunkLength,
            compressedSize: record.body.compressedSize,
            uncompressedSize: record.body.uncompressedSize,
            compression: record.body.compression.str,
            messageEndTime: record.body.messageEndTime,
            messageIndexLength: record.body.messageIndexLength,
            messageIndexOffsets: new Map(
              record.body.messageIndexOffsets.entry.map(({ channelId, offset }) => [
                channelId,
                offset,
              ]),
            ),
            messageStartTime: record.body.messageStartTime,
          });
          break;
        case Mcap0Constants.Opcode.ATTACHMENT:
          result.push({
            type: "Attachment",
            name: record.body.name.str,
            contentType: record.body.contentType.str,
            logTime: record.body.logTime,
            createTime: record.body.createTime,
            data: record.body.data,
          });
          break;
        case Mcap0Constants.Opcode.ATTACHMENT_INDEX:
          result.push({
            type: "AttachmentIndex",
            offset: record.body.offset,
            length: record.body.length,
            name: record.body.name.str,
            contentType: record.body.contentType.str,
            logTime: record.body.logTime,
            createTime: record.body.createTime,
            dataSize: record.body.dataSize,
          });
          break;
        case Mcap0Constants.Opcode.STATISTICS:
          result.push({
            type: "Statistics",
            attachmentCount: record.body.attachmentCount,
            channelCount: record.body.channelCount,
            channelMessageCounts: new Map(
              record.body.channelMessageCounts.entry.map(({ channelId, messageCount }) => [
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
        case Mcap0Constants.Opcode.METADATA:
          result.push({
            type: "Metadata",
            name: record.body.name.str,
            metadata: new Map(
              record.body.metadata.entry.entry.map(({ key, value }) => [key.str, value.str]),
            ),
          });
          break;
        case Mcap0Constants.Opcode.METADATA_INDEX:
          result.push({
            type: "MetadataIndex",
            offset: record.body.offset,
            length: record.body.length,
            name: record.body.name.str,
          });
          break;
        case Mcap0Constants.Opcode.SUMMARY_OFFSET:
          result.push({
            type: "SummaryOffset",
            groupOpcode: record.body.groupOpcode,
            groupStart: record.body.groupStart,
            groupLength: record.body.groupLength,
          });
          break;
        case Mcap0Constants.Opcode.DATA_END:
          result.push({
            type: "DataEnd",
            dataSectionCrc: record.body.dataSectionCrc,
          });
          break;
      }
    }
    for (const record of mcap.records) {
      addRecord(record);
    }
    return stringifyRecords(result, variant);
  }
}
