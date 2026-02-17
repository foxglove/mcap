import { McapWriter } from "@mcap/core";
import { FileHandleWritable } from "@mcap/nodejs";
import { open } from "node:fs/promises";

async function main() {
  const mcapFilePath = "output.mcap";
  const fileHandle = await open(mcapFilePath, "w");
  const fileHandleWritable = new FileHandleWritable(fileHandle);

  const mcapFile = new McapWriter({
    writable: fileHandleWritable,
    useStatistics: true,
    useChunks: true,
    useChunkIndex: true,
  });

  await mcapFile.start({
    profile: "",
    library: "mcap example",
  });

  const schema = {
    title: "HelloWorld",
    type: "object",
    properties: {
      value: {
        type: "string",
      },
    },
  };

  const schemaId = await mcapFile.registerSchema({
    name: schema.title,
    encoding: "jsonschema",
    data: new Uint8Array(Buffer.from(JSON.stringify(schema))),
  });

  const channelId = await mcapFile.registerChannel({
    schemaId,
    topic: "some_topic",
    messageEncoding: "json",
    metadata: new Map(),
  });

  const msgData = Buffer.from(
    JSON.stringify({
      value: "hello world!",
    }),
  );

  await mcapFile.addMessage({
    channelId,
    sequence: 0,
    publishTime: 0n,
    logTime: BigInt(Date.now()) * 1_000_000n,
    data: new Uint8Array(msgData),
  });

  await mcapFile.end();
}

void main();
