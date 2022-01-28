import { McapStreamReader, TypedMcapRecord } from "../v0/types";
import McapPre0Reader from "./McapPre0Reader";
import { McapRecord as McapPre0Record } from "./types";

function translateRecord(record: McapPre0Record): TypedMcapRecord {
  switch (record.type) {
    case "ChannelInfo":
      return {
        type: "ChannelInfo",
        channelId: record.id,
        topicName: record.topic,
        messageEncoding: record.encoding,
        schemaEncoding: "",
        schemaName: record.schemaName,
        schema: record.schema,
        userData: [],
      };
    case "Message":
      return {
        type: "Message",
        channelId: record.channelInfo.id,
        sequence: 0,
        publishTime: record.timestamp,
        recordTime: record.timestamp,
        messageData: new Uint8Array(record.data),
      };
    case "Chunk":
      return {
        type: "Chunk",
        startTime: 0n,
        endTime: 0n,
        uncompressedSize: record.decompressedSize,
        uncompressedCrc: record.decompressedCrc,
        compression: record.compression,
        records: new Uint8Array(record.data),
      };
    case "Footer":
      return {
        type: "Footer",
        summaryStart: 0n,
        summaryOffsetStart: 0n,
        crc: 0,
      };
  }
}

/**
 * Stream reader which translates pre0 records to the v0 record format.
 */
export default class McapPre0To0StreamReader implements McapStreamReader {
  private reader: McapPre0Reader;

  constructor(...params: ConstructorParameters<typeof McapPre0Reader>) {
    this.reader = new McapPre0Reader(...params);
  }

  done(): boolean {
    return this.reader.done();
  }

  bytesRemaining(): number {
    return this.reader.bytesRemaining();
  }

  append(data: Uint8Array): void {
    this.reader.append(data);
  }

  nextRecord(): TypedMcapRecord | undefined {
    const record = this.reader.nextRecord();
    if (!record) {
      return undefined;
    }
    return translateRecord(record);
  }
}
