import McapPre0Reader from "../pre0/McapPre0Reader";
import { McapRecord as McapPre0Record } from "../pre0/types";
import { ChannelInfo, McapLatestStreamReader, McapRecord } from "./types";

function translateRecord(record: McapPre0Record): McapRecord {
  switch (record.type) {
    case "ChannelInfo":
      return {
        type: "ChannelInfo",
        channelId: record.id,
        topicName: record.topic,
        encoding: record.encoding,
        schemaName: record.schemaName,
        schema: record.schema,
        userData: [],
      };
    case "Message":
      return {
        type: "Message",
        channelInfo: translateRecord(record.channelInfo) as ChannelInfo,
        sequence: 0, //FIXME?
        publishTime: record.timestamp,
        recordTime: record.timestamp,
        messageData: record.data,
      };
    case "Chunk":
      return {
        type: "Chunk",
        uncompressedSize: record.decompressedSize,
        uncompressedCrc: record.decompressedCrc,
        compression: record.compression,
        records: record.data,
      };
    case "Footer":
      return {
        type: "Footer",
        indexOffset: 0n,
        indexCrc: 0,
      };
  }
}

/**
 * Stream reader which translates pre0 records to the latest record format.
 */
export default class McapPre0LatestStreamReader implements McapLatestStreamReader {
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

  nextRecord(): McapRecord | undefined {
    const record = this.reader.nextRecord();
    if (!record) {
      return undefined;
    }
    return translateRecord(record);
  }
}
