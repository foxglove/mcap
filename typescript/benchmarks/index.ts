import { McapIndexedReader, McapStreamReader, McapWriter, TempBuffer } from "@mcap/core";
import { program } from "commander";
import assert from "node:assert";

import { runBenchmark } from "./bench.ts";

/**
 * An IWritable that copies data to memory, but overwrites previous data. This allows benchmarking
 * the copies without actually allocating the full initial capacity.
 */
class FakeMemoryWritable {
  #lastWrittenData: Uint8Array;
  #size = 0;

  constructor(capacity: number) {
    this.#lastWrittenData = new Uint8Array(capacity);
  }

  reset() {
    this.#size = 0;
  }
  position() {
    return BigInt(this.#size);
  }
  async write(data: Uint8Array) {
    if (data.byteLength > this.#lastWrittenData.byteLength) {
      throw new Error(
        `Write out of bounds, capacity would need to be at least ${this.#size + data.byteLength}`,
      );
    }
    this.#lastWrittenData.set(data, 0);
    this.#size += data.byteLength;
  }
}

async function benchmarkReaders() {
  const messageSize = 10;
  const chunkSize = 1024 * 1024 * 4;
  const numMessages = 1_000_000;
  const messageData = new Uint8Array(messageSize).fill(42);
  const buf = new TempBuffer();
  const writer = new McapWriter({ writable: buf, chunkSize });
  await writer.start({ library: "", profile: "" });
  const channelId = await writer.registerChannel({
    schemaId: 0,
    topic: "",
    messageEncoding: "",
    metadata: new Map([]),
  });
  for (let i = 0; i < numMessages; i++) {
    await writer.addMessage({
      channelId,
      sequence: i,
      logTime: BigInt(i),
      publishTime: BigInt(i),
      data: messageData,
    });
  }
  await writer.end();
  await runBenchmark(McapStreamReader.name, async () => {
    const reader = new McapStreamReader();
    reader.append(buf.get());
    let messageCount = 0;
    for (;;) {
      const rec = reader.nextRecord();
      if (rec != undefined) {
        if (rec.type === "Message") {
          messageCount++;
        }
      } else {
        break;
      }
    }
    assert(messageCount === numMessages, `expected ${numMessages} messages, got ${messageCount}`);
  });
  await runBenchmark(McapIndexedReader.name, async () => {
    const reader = await McapIndexedReader.Initialize({ readable: buf });
    let messageCount = 0;
    for await (const _ of reader.readMessages()) {
      messageCount++;
    }
    assert(messageCount === numMessages, `expected ${numMessages} messages, got ${messageCount}`);
  });
  await runBenchmark(McapIndexedReader.name + "_reverse", async () => {
    const reader = await McapIndexedReader.Initialize({ readable: buf });
    let messageCount = 0;
    for await (const _ of reader.readMessages({ reverse: true })) {
      messageCount++;
    }
    assert(messageCount === numMessages, `expected ${numMessages} messages, got ${messageCount}`);
  });
}

export async function benchmarkWriter(): Promise<void> {
  await runWriteBenchmark({ numMessages: 1_000_000, messageSize: 1, chunkSize: 1024 * 1024 });
  await runWriteBenchmark({ numMessages: 100_000, messageSize: 1000, chunkSize: 1024 * 1024 });
  await runWriteBenchmark({ numMessages: 100, messageSize: 1_000_000, chunkSize: 1024 * 1024 });
  await runWriteBenchmark({ numMessages: 1_000_000, messageSize: 1, chunkSize: 10 * 1024 * 1024 });
  await runWriteBenchmark({ numMessages: 100_000, messageSize: 1000, chunkSize: 10 * 1024 * 1024 });
  await runWriteBenchmark({
    numMessages: 100,
    messageSize: 1_000_000,
    chunkSize: 10 * 1024 * 1024,
  });
}

async function runWriteBenchmark({
  numMessages,
  messageSize,
  chunkSize,
}: {
  numMessages: number;
  messageSize: number;
  chunkSize: number;
}) {
  const messageData = new Uint8Array(messageSize).fill(42);
  const writable = new FakeMemoryWritable(2 * chunkSize);
  await runBenchmark(
    `count=${numMessages.toLocaleString()} size=${messageSize.toLocaleString()} chunkSize=${chunkSize.toLocaleString()} (1 op ≈ ${(
      numMessages * messageSize
    ).toLocaleString()} bytes)`,
    async () => {
      writable.reset();
      const writer = new McapWriter({ writable, chunkSize });
      await writer.start({ library: "", profile: "" });
      const channelId = await writer.registerChannel({
        schemaId: 0,
        topic: "",
        messageEncoding: "",
        metadata: new Map([]),
      });
      for (let i = 0; i < numMessages; i++) {
        await writer.addMessage({
          channelId,
          sequence: i,
          logTime: BigInt(i),
          publishTime: BigInt(i),
          data: messageData,
        });
      }
      await writer.end();
    },
  );
}

async function main(args: { suite?: string }) {
  const { suite } = args;
  if (suite == undefined || suite === "writer") {
    console.log("Running 'writer' suite");
    await benchmarkWriter();
  }
  if (suite == undefined || suite === "reader") {
    console.log("Running 'reader' suite");
    await benchmarkReaders();
  }
}

program
  .addOption(program.createOption("--suite <suite>", "Name of suite to run"))
  .action(main)
  .parse();
