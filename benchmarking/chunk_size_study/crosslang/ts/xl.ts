// Cross-language correlation check (TypeScript, @mcap/core).
// Run with: npx tsx xl.ts <write|read> <file> <num> <size> <chunk> <none>
// Note: @mcap/core ships no zstd *compressor*, so this runs uncompressed.
import * as fs from "node:fs";

import { McapWriter, McapIndexedReader, type IWritable } from "../../../../typescript/core/src/index.ts";
import type { IReadable } from "../../../../typescript/core/src/types.ts";

class FileWritable implements IWritable {
  #fd: number;
  #pos = 0n;
  constructor(path: string) {
    this.#fd = fs.openSync(path, "w");
  }
  async write(buffer: Uint8Array): Promise<void> {
    fs.writeSync(this.#fd, buffer);
    this.#pos += BigInt(buffer.byteLength);
  }
  position(): bigint {
    return this.#pos;
  }
  close(): void {
    fs.closeSync(this.#fd);
  }
}

class FileReadable implements IReadable {
  #fd: number;
  #size: bigint;
  constructor(path: string) {
    this.#fd = fs.openSync(path, "r");
    this.#size = BigInt(fs.fstatSync(this.#fd).size);
  }
  async size(): Promise<bigint> {
    return this.#size;
  }
  async read(offset: bigint, size: bigint): Promise<Uint8Array> {
    const buf = Buffer.alloc(Number(size));
    fs.readSync(this.#fd, buf, 0, Number(size), Number(offset));
    return new Uint8Array(buf.buffer, buf.byteOffset, buf.byteLength);
  }
}

function fill(size: number, seq: number): Uint8Array {
  const b = new Uint8Array(size);
  for (let i = 0; i < size; i++) b[i] = (i + seq) & 0xff;
  return b;
}

async function main(): Promise<void> {
  const [op, file, numS, sizeS, chunkS, comp] = process.argv.slice(2);
  const num = Number(numS);
  const size = Number(sizeS);
  const chunk = Number(chunkS);

  if (op === "write") {
    const writable = new FileWritable(file);
    const writer = new McapWriter({ writable, chunkSize: chunk });
    await writer.start({ profile: "xl", library: "typescript" });
    const schemaId = await writer.registerSchema({
      name: "Bench",
      encoding: "jsonschema",
      data: new TextEncoder().encode("{}"),
    });
    const channelId = await writer.registerChannel({
      topic: "/bench",
      messageEncoding: "json",
      schemaId,
      metadata: new Map(),
    });
    const payload = fill(size, 0); // one reusable payload, generated outside timing
    const t = process.hrtime.bigint();
    for (let i = 0; i < num; i++) {
      await writer.addMessage({
        channelId,
        sequence: i,
        logTime: BigInt(i) * 1000n,
        publishTime: BigInt(i) * 1000n,
        data: payload,
      });
    }
    await writer.end();
    const wall = Number(process.hrtime.bigint() - t) / 1e9;
    writable.close();
    const fsize = fs.statSync(file).size;
    process.stdout.write(`typescript\twrite\t${comp}\t${num}\t${num * size}\t${fsize}\t${wall.toFixed(6)}\n`);
  } else {
    const readable = new FileReadable(file);
    const reader = await McapIndexedReader.Initialize({ readable });
    const t = process.hrtime.bigint();
    let count = 0;
    let nbytes = 0;
    for await (const msg of reader.readMessages()) {
      count++;
      nbytes += msg.data.byteLength;
    }
    const wall = Number(process.hrtime.bigint() - t) / 1e9;
    process.stdout.write(`typescript\tread\t${comp}\t${count}\t${nbytes}\t0\t${wall.toFixed(6)}\n`);
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
