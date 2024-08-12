import { ChunkBuilder } from "./ChunkBuilder";

describe("ChunkBuilder", () => {
  it("generates correct messageStartTime/messageEndTime for messages with logTime 0", () => {
    const builder = new ChunkBuilder({ useMessageIndex: true });

    builder.addMessage({
      channelId: 0,
      data: new Uint8Array(),
      sequence: 0,
      logTime: 0,
      publishTime: 0,
    });
    builder.addMessage({
      channelId: 0,
      data: new Uint8Array(),
      sequence: 1,
      logTime: 1,
      publishTime: 1,
    });
    expect(builder.messageStartTime).toBe(0);
    expect(builder.messageEndTime).toBe(1);
  });
});
