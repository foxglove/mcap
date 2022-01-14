import { BufferedWriter } from "../common/BufferedWriter";
import { IWritable } from "../common/IWritable";
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
  private recordPrefixWriter: BufferedWriter;
  private bufferedWriter: BufferedWriter;
  private writable: IWritable;

  constructor(writable: IWritable) {
    this.recordPrefixWriter = new BufferedWriter();
    this.bufferedWriter = new BufferedWriter();
    this.writable = writable;
  }

  async writeMagic(): Promise<void> {
    await this.writable.write(new Uint8Array(MCAP0_MAGIC));
  }

  async writeHeader(header: Header): Promise<void> {
    this.bufferedWriter.uint8(Opcode.HEADER);
    this.bufferedWriter.string(header.profile);
    this.bufferedWriter.string(header.library);

    await this.bufferedWriter.flush(this.writable);
    //fixme - header metadata
  }

  async writeFooter(footer: Footer): Promise<void> {
    this.bufferedWriter.uint8(Opcode.FOOTER);
    this.bufferedWriter.uint64(footer.indexOffset);
    this.bufferedWriter.uint32(footer.indexCrc);

    await this.bufferedWriter.flush(this.writable);
  }

  async writeChannelInfo(info: ChannelInfo): Promise<void> {
    this.bufferedWriter.uint32(info.channelId);
    this.bufferedWriter.string(info.topicName);
    this.bufferedWriter.string(info.encoding);
    this.bufferedWriter.string(info.schemaName);
    this.bufferedWriter.string(info.schema);

    this.recordPrefixWriter.uint8(Opcode.CHANNEL_INFO);
    this.recordPrefixWriter.uint32(this.bufferedWriter.size());

    await this.recordPrefixWriter.flush(this.writable);
    await this.bufferedWriter.flush(this.writable);
  }

  async writeMessage(message: Message): Promise<void> {
    this.bufferedWriter.uint16(message.channelId);
    this.bufferedWriter.uint32(message.sequence);
    this.bufferedWriter.uint64(message.publishTime);
    this.bufferedWriter.uint64(message.recordTime);

    this.recordPrefixWriter.uint8(Opcode.MESSAGE);
    this.recordPrefixWriter.uint32(this.bufferedWriter.size() + message.messageData.byteLength);

    await this.recordPrefixWriter.flush(this.writable);
    await this.bufferedWriter.flush(this.writable);
    await this.writable.write(message.messageData);
  }

  async writeAttachment(attachment: Attachment): Promise<void> {
    this.bufferedWriter.string(attachment.name);
    this.bufferedWriter.uint64(attachment.recordTime);
    this.bufferedWriter.string(attachment.contentType);

    this.recordPrefixWriter.uint8(Opcode.CHANNEL_INFO);
    this.recordPrefixWriter.uint32(this.bufferedWriter.size() + attachment.data.byteLength);

    await this.recordPrefixWriter.flush(this.writable);
    await this.bufferedWriter.flush(this.writable);
    await this.writable.write(attachment.data);
  }
}
