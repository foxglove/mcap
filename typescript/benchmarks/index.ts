import { McapWriter } from "@mcap/core";
import { add, complete, cycle, suite } from "benny";

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

function addWriteBenchmark({
  numMessages,
  messageSize,
  chunkSize,
}: {
  numMessages: number;
  messageSize: number;
  chunkSize: number;
}) {
  return add(
    `count=${numMessages.toLocaleString()} size=${messageSize.toLocaleString()} chunkSize=${chunkSize.toLocaleString()} (1 op ≈ ${(
      numMessages * messageSize
    ).toLocaleString()} bytes)`,
    async () => {
      const messageData = new Uint8Array(messageSize).fill(42);
      const writable = new FakeMemoryWritable(2 * chunkSize);
      return async () => {
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
      };
    },
  );
}

async function benchmarkWriter() {
  await suite(
    McapWriter.name,
    addWriteBenchmark({ numMessages: 1_000_000, messageSize: 1, chunkSize: 1024 * 1024 }),
    addWriteBenchmark({ numMessages: 100_000, messageSize: 1000, chunkSize: 1024 * 1024 }),
    addWriteBenchmark({ numMessages: 100, messageSize: 1_000_000, chunkSize: 1024 * 1024 }),
    addWriteBenchmark({ numMessages: 1_000_000, messageSize: 1, chunkSize: 10 * 1024 * 1024 }),
    addWriteBenchmark({ numMessages: 100_000, messageSize: 1000, chunkSize: 10 * 1024 * 1024 }),
    addWriteBenchmark({ numMessages: 100, messageSize: 1_000_000, chunkSize: 10 * 1024 * 1024 }),
    cycle(),
    complete(),
  );
}

async function main() {
  await benchmarkWriter();
}

void main();
