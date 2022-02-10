import { Mcap0RecordBuilder } from "./Mcap0RecordBuilder";
import { Channel, Message, MessageIndex, Schema } from "./types";

class ChunkBuilder {
  private recordWriter = new Mcap0RecordBuilder();
  private messageIndices = new Map<number, MessageIndex>();
  private totalMessageCount = 0;

  messageStartTime = 0n;
  messageEndTime = 0n;

  get numMessages(): number {
    return this.totalMessageCount;
  }

  get buffer(): Uint8Array {
    return this.recordWriter.buffer;
  }

  get indices(): IterableIterator<MessageIndex> {
    return this.messageIndices.values();
  }

  addSchema(schema: Schema): void {
    this.recordWriter.writeSchema(schema);
  }

  addChannel(info: Channel): void {
    if (!this.messageIndices.has(info.id)) {
      this.messageIndices.set(info.id, {
        channelId: info.id,
        records: [],
      });
    }
    this.recordWriter.writeChannel(info);
  }

  addMessage(message: Message): void {
    if (this.messageStartTime === 0n) {
      this.messageStartTime = message.logTime;
    }
    this.messageEndTime = message.logTime;

    let messageIndex = this.messageIndices.get(message.channelId);
    if (!messageIndex) {
      messageIndex = {
        channelId: message.channelId,
        records: [],
      };
      this.messageIndices.set(message.channelId, messageIndex);
    }

    messageIndex.records.push([message.logTime, BigInt(this.recordWriter.length)]);

    this.totalMessageCount += 1;
    this.recordWriter.writeMessage(message);
  }

  reset(): void {
    this.messageStartTime = 0n;
    this.messageEndTime = 0n;
    this.totalMessageCount = 0;
    this.messageIndices.clear();
  }
}

export { ChunkBuilder };
