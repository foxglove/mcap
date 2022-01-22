import { IWritable } from "../common/IWritable";
import { Mcap0BufferRecordBuilder } from "./Mcap0BufferedRecordBuilder";
import { ChannelInfo, Message, Header, Attachment } from "./types";

/**
 * Mcap0UnindexedWriter provides an interface for writing messages
 * to unindexed mcap files.
 *
 * NOTE: callers must wait on any method call to complete before calling another
 * method. Calling a method before another has completed will result in a corrupt
 * mcap file.
 */
export class Mcap0UnindexedWriter {
  private bufferRecordBuilder: Mcap0BufferRecordBuilder;
  private writable: IWritable;

  private nextChannelId = 1;

  constructor(writable: IWritable) {
    this.writable = writable;
    this.bufferRecordBuilder = new Mcap0BufferRecordBuilder();
  }

  async start(header: Header): Promise<void> {
    this.bufferRecordBuilder.writeMagic();
    this.bufferRecordBuilder.writeHeader(header);

    await this.writable.write(this.bufferRecordBuilder.buffer);
    this.bufferRecordBuilder.reset();
  }

  async end(): Promise<void> {
    this.bufferRecordBuilder.writeFooter({
      indexOffset: 0n,
      indexCrc: 0,
    });
    await this.writable.write(this.bufferRecordBuilder.buffer);
    this.bufferRecordBuilder.reset();
  }

  /**
   * Add channel info and return a generated channel id. The channel id is used when adding messages.
   */
  async registerChannel(info: Omit<ChannelInfo, "channelId">): Promise<number> {
    const channelId = this.nextChannelId;
    this.bufferRecordBuilder.writeChannelInfo({
      ...info,
      channelId,
    });

    await this.writable.write(this.bufferRecordBuilder.buffer);
    this.bufferRecordBuilder.reset();

    this.nextChannelId += 1;
    return channelId;
  }

  async addMessage(message: Message): Promise<void> {
    this.bufferRecordBuilder.writeMessage(message);
    await this.writable.write(this.bufferRecordBuilder.buffer);
    this.bufferRecordBuilder.reset();
  }

  async addAttachment(attachment: Attachment): Promise<void> {
    this.bufferRecordBuilder.writeAttachment(attachment);
    await this.writable.write(this.bufferRecordBuilder.buffer);
    this.bufferRecordBuilder.reset();
  }
}
