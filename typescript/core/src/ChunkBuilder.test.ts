import { ChunkBuilder } from "./ChunkBuilder.ts";

describe("ChunkBuilder", () => {
  it("generates correct messageStartTime/messageEndTime for messages with logTime 0", () => {
    const builder = new ChunkBuilder({ useMessageIndex: true });

    builder.addMessage({
      channelId: 0,
      data: new Uint8Array(),
      sequence: 0,
      logTime: 0n,
      publishTime: 0n,
    });
    builder.addMessage({
      channelId: 0,
      data: new Uint8Array(),
      sequence: 1,
      logTime: 1n,
      publishTime: 1n,
    });
    expect(builder.messageStartTime).toBe(0n);
    expect(builder.messageEndTime).toBe(1n);
  });
});
