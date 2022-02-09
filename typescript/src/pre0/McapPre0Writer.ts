import { open, FileHandle } from "fs/promises";

import { BufferBuilder } from "./BufferBuilder";
import { MCAP_MAGIC, RecordType } from "./constants";
import { Channel, McapRecord, Message } from "./types";

export default class McapPre0Writer {
  private writeStream?: FileHandle;

  async open(pathname: string): Promise<void> {
    this.writeStream = await open(pathname, "w");

    // write the magic
    // 0x89, M, C, A, P, \r, \n, \n
    await this.writeStream.write(new Uint8Array(MCAP_MAGIC));

    // write the format version
    await this.writeStream.write(new Uint8Array([1]));
  }

  async write(record: McapRecord): Promise<void> {
    switch (record.type) {
      case "Channel":
        await this.writeChannelRecord(record);
        break;
      case "Message":
        await this.writeMessageRecord(record);
        break;
      default:
        throw new Error(`Unsupported record type: ${record.type}`);
    }
  }

  async end(): Promise<void> {
    if (!this.writeStream) {
      return;
    }
    // write the footer
    const serializer = new BufferBuilder();
    serializer.uint8(RecordType.FOOTER);
    serializer.uint64(0n);
    serializer.uint32(0);

    await this.writeStream.write(serializer.buffer);

    await this.writeStream?.close();
  }

  private async writeChannelRecord(info: Channel): Promise<void> {
    if (!this.writeStream) {
      return;
    }
    const serializer = new BufferBuilder();
    serializer.uint32(info.id);
    serializer.string(info.topic);
    serializer.string(info.encoding);
    serializer.string(info.schemaName);
    serializer.string(info.schema);

    const preamble = new BufferBuilder();
    preamble.uint8(RecordType.CHANNEL);
    preamble.uint32(serializer.length);

    await this.writeStream.write(preamble.buffer);
    await this.writeStream.write(serializer.buffer);
  }

  private async writeMessageRecord(message: Message): Promise<void> {
    if (!this.writeStream) {
      return;
    }
    const serializer = new BufferBuilder();
    serializer.uint32(message.channel.id);
    serializer.uint64(message.timestamp);

    const preamble = new BufferBuilder();
    preamble.uint8(RecordType.MESSAGE);
    preamble.uint32(serializer.length + message.data.byteLength);

    await this.writeStream.write(preamble.buffer);
    await this.writeStream.write(serializer.buffer);
    await this.writeStream?.write(new Uint8Array(message.data));
  }
}
