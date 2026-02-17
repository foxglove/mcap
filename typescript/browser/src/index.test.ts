import { McapWriter, McapIndexedReader, TempBuffer } from "@mcap/core";
import { Blob } from "node:buffer";

import { BlobReadable } from "./BlobReadable.ts";

async function collect<T>(iterable: AsyncIterable<T>): Promise<T[]> {
  const result: T[] = [];
  for await (const item of iterable) {
    result.push(item);
  }
  return result;
}

describe("BlobReadable", () => {
  it("reads blob", async () => {
    const tempBuffer = new TempBuffer();

    const header = { library: "lib", profile: "prof" };
    const writer = new McapWriter({ writable: tempBuffer });
    await writer.start(header);
    const channel = {
      topic: "foo",
      schemaId: 0,
      messageEncoding: "enc",
      metadata: new Map(),
    };
    const channelId = await writer.registerChannel(channel);
    const message = {
      channelId,
      sequence: 1,
      logTime: 1n,
      publishTime: 2n,
      data: new Uint8Array([1, 2, 3]),
    };
    await writer.addMessage(message);
    await writer.end();

    const blob = new Blob([tempBuffer.get()]);

    const reader = await McapIndexedReader.Initialize({
      readable: new BlobReadable(blob as unknown as ConstructorParameters<typeof BlobReadable>[0]),
    });
    expect(reader.header).toEqual({ ...header, type: "Header" });
    expect(await collect(reader.readMessages())).toEqual([{ ...message, type: "Message" }]);
  });
});
