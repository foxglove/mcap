import { McapIndexedReader, McapStreamReader, McapWriter, McapTypes, IWritable } from "@mcap/core";
import assert from "assert";
import { program } from "commander";
import fs from "fs/promises";
import os from "os";
import path from "path";

import { runBenchmark } from "./bench";

class ReadableFile implements McapTypes.IReadable {
  #fd: fs.FileHandle;
  constructor(fd: fs.FileHandle) {
    this.#fd = fd;
  }
  async read(offset: bigint, size: bigint): Promise<Uint8Array> {
    const res = new Uint8Array(Number(size));
    await this.#fd.read(res, 0, Number(size), Number(offset));
    return res;
  }
  async size(): Promise<bigint> {
    const stat = await this.#fd.stat();
    return BigInt(stat.size);
  }
}

class WritableFile implements IWritable {
  #fd: fs.FileHandle;
  #pos: bigint;
  constructor(fd: fs.FileHandle) {
    this.#fd = fd;
    this.#pos = BigInt(0);
  }
  async write(buffer: Uint8Array): Promise<void> {
    await this.#fd.write(buffer);
    this.#pos = this.#pos + BigInt(buffer.length);
  }
  position(): bigint {
    return this.#pos;
  }
}

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
  const filepath = path.join(os.tmpdir(), "sample.mcap");
  {
    const fd = await fs.open(filepath, "w");

    const writer = new McapWriter({ writable: new WritableFile(fd), chunkSize });
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
    await fd.close();
  }
  await runBenchmark(McapStreamReader.name, async () => {
    const fd = await fs.open(filepath);
    const stream = fd.createReadStream();
    const reader = new McapStreamReader();
    let messageCount = 0;
    stream.on("data", (chunk) => {
      reader.append(Buffer.from(chunk));
      for (let record; (record = reader.nextRecord()); ) {
        if (record.type === "Message") {
          messageCount++;
        }
      }
    });
    await new Promise((resolve) => stream.on("end", resolve));
    stream.close();
    assert(messageCount === numMessages, `expected ${numMessages} messages, got ${messageCount}`);
    await fd.close();
  });
  await runBenchmark("readMessages_async", async () => {
    const fd = await fs.open(filepath);
    const reader = await McapIndexedReader.Initialize({ readable: new ReadableFile(fd) });
    let messageCount = 0;
    for await (const _ of reader.readMessages()) {
      messageCount++;
    }
    assert(messageCount === numMessages, `expected ${numMessages} messages, got ${messageCount}`);
    await fd.close();
  });
  await runBenchmark("readMessages_async_reverse", async () => {
    const fd = await fs.open(filepath);
    const reader = await McapIndexedReader.Initialize({ readable: new ReadableFile(fd) });
    let messageCount = 0;
    for await (const _ of reader.readMessages({ reverse: true })) {
      messageCount++;
    }
    assert(messageCount === numMessages, `expected ${numMessages} messages, got ${messageCount}`);
    await fd.close();
  });
  await runBenchmark("readMessages_sync", async () => {
    const fd = await fs.open(filepath);
    const reader = await McapIndexedReader.Initialize({ readable: new ReadableFile(fd) });
    let messageCount = 0;
    for (const { promise } of reader.readMessagesSync()) {
      if (promise != undefined) {
        await promise;
      } else {
        messageCount++;
      }
    }
    assert(messageCount === numMessages, `expected ${numMessages} messages, got ${messageCount}`);
    await fd.close();
  });
  await runBenchmark("readMessages_sync_reverse", async () => {
    const fd = await fs.open(filepath);
    const reader = await McapIndexedReader.Initialize({ readable: new ReadableFile(fd) });
    let messageCount = 0;
    for (const { promise } of reader.readMessagesSync({ reverse: true })) {
      if (promise != undefined) {
        await promise;
      } else {
        messageCount++;
      }
    }
    assert(messageCount === numMessages, `expected ${numMessages} messages, got ${messageCount}`);
    await fd.close();
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
