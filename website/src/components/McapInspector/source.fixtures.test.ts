import * as zstd from "@foxglove/wasm-zstd";
import {
  McapWriter,
  McapRecordBuilder,
  type DecompressHandlers,
} from "@mcap/core";
import { loadDecompressHandlers } from "@mcap/support";
import assert from "node:assert/strict";
import { test } from "node:test";

import { createDemo } from "./demo.ts";
import { fileReadable, InspectorSource } from "./source.ts";

async function readRecording(file: Blob, handlers: DecompressHandlers) {
  const source = await InspectorSource.open(
    fileReadable(file),
    handlers,
    "fixture.mcap",
    new AbortController().signal,
  );
  return (await source.readWindow(0, source.recording.duration))!;
}

async function fixture({
  useChunks,
  compression = "",
}: {
  useChunks: boolean;
  compression?: string;
}) {
  const parts: Uint8Array<ArrayBuffer>[] = [];
  let pos = 0n;
  await zstd.isLoaded;
  const writer = new McapWriter({
    useChunks,
    chunkSize: 50,
    useStatistics: false,
    useSummaryOffsets: false,
    compressChunk: compression
      ? (data) => ({ compression, compressedData: zstd.compress(data) })
      : undefined,
    writable: {
      position: () => pos,
      write: async (data) => {
        parts.push(new Uint8Array(data));
        pos += BigInt(data.length);
      },
    },
  });
  await writer.start({ profile: "", library: "test" });
  const id = await writer.registerChannel({
    schemaId: 0,
    topic: "/same",
    messageEncoding: "json",
    metadata: new Map(),
  });
  const second = await writer.registerChannel({
    schemaId: 0,
    topic: "/same",
    messageEncoding: "json",
    metadata: new Map(),
  });
  const origin = 1_720_000_000_000_000_000n;
  for (const [i, dt] of [20n, 0n, 10n, 30n].entries()) {
    await writer.addMessage({
      channelId: i % 2 !== 0 ? second : id,
      sequence: i,
      logTime: origin + dt,
      publishTime: origin + dt - 1n,
      data: new Uint8Array([1, 2, 3]),
    });
  }
  await writer.end();
  return new File(parts, "fixture.mcap");
}
for (const mode of ["loose", "chunked", "zstd"]) {
  void test(`reads ${mode} and preserves nanoseconds and separate channel IDs`, async () => {
    const result = await readRecording(
      await fixture({
        useChunks: mode !== "loose",
        compression: mode === "zstd" ? mode : "",
      }),
      await loadDecompressHandlers(),
    );
    assert.equal(result.messageCount, 4);
    assert.equal(result.channels.length, 2);
    assert.equal(result.duration, 30e-9);
    assert.equal(result.startTime, 1_720_000_000_000_000_000n);
    assert.equal(result.looseCount, mode === "loose" ? 4 : 0);
    if (mode !== "loose") {
      assert.ok(result.chunks.length > 1);
      for (const c of result.channels) {
        for (const m of c.messages) {
          assert.notEqual(m.chunkId, null);
          assert.ok(result.chunks[m.chunkId!]!.ranges.has(c.id));
        }
      }
    }
  });
}
void test("mixed loose/chunked physical records are never assigned to the preceding chunk", async () => {
  const file = await fixture({ useChunks: true });
  const buffer = new Uint8Array(await file.arrayBuffer());
  let offset = 8;
  let insert = 0;
  while (offset < buffer.length - 8) {
    const length =
      Number(new DataView(buffer.buffer).getBigUint64(offset + 1, true)) + 9;
    if (buffer[offset] === 6) {
      insert = offset + length;
      break;
    }
    offset += length;
  }
  const builder = new McapRecordBuilder();
  builder.writeMessage({
    channelId: 0,
    sequence: 99,
    logTime: 1_720_000_000_000_000_005n,
    publishTime: 0n,
    data: new Uint8Array([4]),
  });
  const mixed = new File(
    [
      buffer.slice(0, insert),
      new Uint8Array(builder.buffer),
      buffer.slice(insert),
    ],
    "mixed.mcap",
  );
  // Summary offsets are intentionally stale: the sequential scanner must not rely on them.
  const result = await readRecording(mixed, {});
  assert.equal(result.looseCount, 1);
  assert.equal(
    result.channels[0]!.messages.find((m) => m.sequence === 99)?.chunkId,
    null,
  );
});
void test("rejects invalid magic and truncated recordings", async () => {
  await assert.rejects(
    readRecording(new Blob([new Uint8Array(64)]), {}),
    /magic/i,
  );
  const file = await fixture({ useChunks: true });
  await assert.rejects(
    readRecording(file.slice(0, file.size - 4), {}),
    /truncated|incomplete/i,
  );
});
void test("demo is a real MCAP with all twelve populated channels", async () => {
  const result = await readRecording(await createDemo(), {});
  assert.equal(result.channels.length, 12);
  assert.ok(result.chunks.length > 20);
  assert.ok(result.channels.every((c) => c.messages.length > 0));
});
