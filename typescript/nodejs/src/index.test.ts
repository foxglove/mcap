import { McapWriter, McapIndexedReader } from "@mcap/core";
import { mkdtemp, open, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import * as path from "node:path";

import { FileHandleReadable } from "./FileHandleReadable.ts";
import { FileHandleWritable } from "./FileHandleWritable.ts";

async function collect<T>(iterable: AsyncIterable<T>): Promise<T[]> {
  const result: T[] = [];
  for await (const item of iterable) {
    result.push(item);
  }
  return result;
}

describe("FileHandleReadable & FileHandleWritable", () => {
  it("roundtrips", async () => {
    const dir = await mkdtemp(path.join(tmpdir(), "mcap-test"));
    try {
      const handle = await open(path.join(dir, "test.mcap"), "wx+");

      const header = { library: "lib", profile: "prof" };
      const writer = new McapWriter({ writable: new FileHandleWritable(handle) });
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

      const reader = await McapIndexedReader.Initialize({
        readable: new FileHandleReadable(handle),
      });
      expect(reader.header).toEqual({ ...header, type: "Header" });
      expect(await collect(reader.readMessages())).toEqual([{ ...message, type: "Message" }]);
    } finally {
      await rm(dir, { recursive: true });
    }
  });
});
