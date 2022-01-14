import Writer from "../common/Writer";
import { IWritable } from "./IWritable";
import { MCAP0_MAGIC, Opcode } from "./constants";
import { ChannelInfo, Header, Footer, Message, Attachment } from "./types";

/**
 * Mcap0RecordWriter provides methods to serialize mcap records to an IWritable.
 *
 * It makes no effort to ensure spec compatability on the order of records, this is the responsibility
 * of the caller.
 *
 * Unless you are building your own higher level writer interface, you'll likely want to use one of
 * the higher level writer interfaces.
 */
export class Mcap0RecordWriter {
  private writable: IWritable;

  constructor(writable: IWritable) {
    this.writable = writable;
  }

  async writeMagic(): Promise<void> {
    await this.writable.write(new Uint8Array(MCAP0_MAGIC));
  }

  async writeHeader(header: Header): Promise<void> {
    const serializer = new Writer();
    serializer.uint8(Opcode.HEADER);
    serializer.string(header.profile);
    serializer.string(header.library);
    //fixme - header metadata
    await this.writable.write(serializer.toUint8());
  }

  async writeFooter(footer: Footer): Promise<void> {
    const serializer = new Writer();
    serializer.uint8(Opcode.FOOTER);
    serializer.uint64(footer.indexOffset);
    serializer.uint32(footer.indexCrc);
    await this.writable.write(serializer.toUint8());
  }

  async writeChannelInfo(info: ChannelInfo): Promise<void> {
    const serializer = new Writer();
    serializer.uint32(info.channelId);
    serializer.string(info.topicName);
    serializer.string(info.encoding);
    serializer.string(info.schemaName);
    serializer.string(info.schema);

    const preamble = new Writer();
    preamble.uint8(Opcode.CHANNEL_INFO);
    preamble.uint32(serializer.size());

    await this.writable.write(preamble.toUint8());
    await this.writable.write(serializer.toUint8());
  }

  async writeMessage(message: Message): Promise<void> {
    const serializer = new Writer();
    serializer.uint16(message.channelId);
    serializer.uint32(message.sequence);
    serializer.uint64(message.publishTime);
    serializer.uint64(message.recordTime);

    const preamble = new Writer();
    preamble.uint8(Opcode.MESSAGE);
    preamble.uint32(serializer.size() + message.messageData.byteLength);

    await this.writable.write(preamble.toUint8());
    await this.writable.write(serializer.toUint8());
    await this.writable.write(message.messageData);
  }

  async writeAttachment(attachment: Attachment): Promise<void> {
    const serializer = new Writer();

    serializer.string(attachment.name);
    serializer.uint64(attachment.recordTime);
    serializer.string(attachment.contentType);

    const preamble = new Writer();
    preamble.uint8(Opcode.CHANNEL_INFO);
    preamble.uint32(serializer.size() + attachment.data.byteLength);

    await this.writable.write(preamble.toUint8());
    await this.writable.write(serializer.toUint8());
    await this.writable.write(attachment.data);
  }
}
