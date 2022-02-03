import { Mcap0RecordBuilder } from "./Mcap0RecordBuilder";
import { ChannelInfo, Message, MessageIndex } from "./types";

class ChunkBuilder {
  private recordWriter = new Mcap0RecordBuilder();
  private messageIndices = new Map<number, MessageIndex>();
  private totalMessageCount = 0;

  startTime = 0n;
  endTime = 0n;

  get numMessages(): number {
    return this.totalMessageCount;
  }

  get buffer(): Uint8Array {
    return this.recordWriter.buffer;
  }

  get indices(): IterableIterator<MessageIndex> {
    return this.messageIndices.values();
  }

  addChannelInfo(info: ChannelInfo): void {
    if (!this.messageIndices.has(info.channelId)) {
      this.messageIndices.set(info.channelId, {
        channelId: info.channelId,
        records: [],
      });
    }
    this.recordWriter.writeChannelInfo(info);
  }

  addMessage(message: Message): void {
    if (this.startTime === 0n) {
      this.startTime = message.recordTime;
    }
    this.endTime = message.recordTime;

    let messageIndex = this.messageIndices.get(message.channelId);
    if (!messageIndex) {
      messageIndex = {
        channelId: message.channelId,
        records: [],
      };
      this.messageIndices.set(message.channelId, messageIndex);
    }

    messageIndex.records.push([message.recordTime, BigInt(this.recordWriter.length)]);

    this.totalMessageCount += 1;
    this.recordWriter.writeMessage(message);
  }

  reset(): void {
    this.startTime = 0n;
    this.endTime = 0n;
    this.totalMessageCount = 0;
    this.messageIndices.clear();
  }
}

export { ChunkBuilder };
