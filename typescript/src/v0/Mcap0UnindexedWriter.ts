import { IWritable } from "../common/IWritable";
import { Mcap0RecordWriter } from "./Mcap0RecordWriter";
import { ChannelInfo, Message, Header, Attachment } from "./types";

export class Mcap0UnindexedWriter {
  private recordWriter: Mcap0RecordWriter;

  private channelInfos = new Map<number, ChannelInfo>();
  private writtenChannelIds = new Set<number>();

  constructor(writable: IWritable) {
    this.recordWriter = new Mcap0RecordWriter(writable);
  }

  async start(header: Header): Promise<void> {
    await this.recordWriter.writeMagic();
    await this.recordWriter.writeHeader(header);
  }

  async end(): Promise<void> {
    await this.recordWriter.writeFooter({
      indexOffset: 0n,
      indexCrc: 0,
    });
  }

  /**
   * Add channel info and return a generated channel id. The channel id is used when adding messages.
   */
  async registerChannel(info: Omit<ChannelInfo, "channelId">): Promise<number> {
    const channelId = this.channelInfos.size + 1;
    this.channelInfos.set(channelId, {
      ...info,
      channelId,
    });

    return channelId;
  }

  async addMessage(message: Message): Promise<void> {
    // write out channel id if we have not yet done so
    if (!this.writtenChannelIds.has(message.channelId)) {
      const channelInfo = this.channelInfos.get(message.channelId);
      if (!channelInfo) {
        throw new Error(
          `Mcap0UnindexedWriter#addMessage failed: missing channel info for id ${message.channelId}`,
        );
      }

      await this.recordWriter.writeChannelInfo(channelInfo);
      this.writtenChannelIds.add(message.channelId);
    }

    await this.recordWriter.writeMessage(message);
  }

  async addAttachment(attachment: Attachment): Promise<void> {
    await this.recordWriter.writeAttachment(attachment);
  }
}
