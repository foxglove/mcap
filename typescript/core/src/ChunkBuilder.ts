import { McapRecordBuilder } from "./McapRecordBuilder";
import { Channel, Message, MessageIndex, Schema } from "./types";

type ChunkBuilderOptions = {
  useMessageIndex?: boolean;
};
class ChunkBuilder {
  #recordWriter = new McapRecordBuilder();
  #messageIndices: Map<number, MessageIndex> | undefined;
  #totalMessageCount = 0;

  messageStartTime = 0;
  messageEndTime = 0;

  constructor({ useMessageIndex = true }: ChunkBuilderOptions) {
    if (useMessageIndex) {
      this.#messageIndices = new Map();
    }
  }

  get numMessages(): number {
    return this.#totalMessageCount;
  }

  get buffer(): Uint8Array {
    return this.#recordWriter.buffer;
  }

  get byteLength(): number {
    return this.#recordWriter.length;
  }

  get indices(): Iterable<MessageIndex> {
    if (this.#messageIndices) {
      return this.#messageIndices.values();
    }
    return [];
  }

  addSchema(schema: Schema): void {
    this.#recordWriter.writeSchema(schema);
  }

  addChannel(info: Channel): void {
    if (this.#messageIndices && !this.#messageIndices.has(info.id)) {
      this.#messageIndices.set(info.id, {
        channelId: info.id,
        records: [],
      });
    }
    this.#recordWriter.writeChannel(info);
  }

  addMessage(message: Message): void {
    if (this.#totalMessageCount === 0 || message.logTime < this.messageStartTime) {
      this.messageStartTime = message.logTime;
    }
    if (this.#totalMessageCount === 0 || message.logTime > this.messageEndTime) {
      this.messageEndTime = message.logTime;
    }

    if (this.#messageIndices) {
      let messageIndex = this.#messageIndices.get(message.channelId);
      if (!messageIndex) {
        messageIndex = {
          channelId: message.channelId,
          records: [],
        };
        this.#messageIndices.set(message.channelId, messageIndex);
      }
      messageIndex.records.push([message.logTime, this.#recordWriter.length]);
    }

    this.#totalMessageCount += 1;
    this.#recordWriter.writeMessage(message);
  }

  reset(): void {
    this.messageStartTime = 0;
    this.messageEndTime = 0;
    this.#totalMessageCount = 0;
    this.#messageIndices?.clear();
    this.#recordWriter.reset();
  }
}

export { ChunkBuilder };
