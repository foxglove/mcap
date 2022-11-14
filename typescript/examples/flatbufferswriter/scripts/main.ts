import { LinePrimitive } from "@foxglove/schemas/jsonschema";
import { McapWriter, IWritable } from "@mcap/core";
import { open, FileHandle } from "fs/promises";

// Mcap IWritable interface for nodejs FileHandle
class FileHandleWritable implements IWritable {
  private handle: FileHandle;
  private totalBytesWritten = 0;

  constructor(handle: FileHandle) {
    this.handle = handle;
  }

  async write(buffer: Uint8Array): Promise<void> {
    const written = await this.handle.write(buffer);
    this.totalBytesWritten += written.bytesWritten;
  }

  position(): bigint {
    return BigInt(this.totalBytesWritten);
  }
}

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

  const schema = LinePrimitive;

  const schemaId = await mcapFile.registerSchema({
    name: schema.title,
    encoding: "flatbuffer",
    data: Buffer.from(JSON.stringify(schema)),
  });

  const channelId = await mcapFile.registerChannel({
    schemaId,
    topic: "some_topic",
    messageEncoding: "flatbuffer",
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
    data: msgData,
  });

  await mcapFile.end();
}

void main();
