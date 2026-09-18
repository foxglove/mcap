import * as zstd from "@foxglove/wasm-zstd";
import { McapRecordBuilder, MCAP_MAGIC, type IReadable } from "@mcap/core";
import { loadDecompressHandlers } from "@mcap/support";
import assert from "node:assert/strict";
import { test } from "node:test";

import { createDemo } from "./demo.ts";
import { fileReadable, InspectorSource } from "./source.ts";

void test("catalog skips payloads; five-second windows read only intersecting physical chunks and reuse metadata", async () => {
  const file = await createDemo();
  const base = fileReadable(file);
  const reads: { offset: number; size: number }[] = [];
  const readable: IReadable = {
    size: async () => await base.size(),
    read: async (offset, size) => {
      reads.push({ offset: Number(offset), size: Number(size) });
      return await base.read(offset, size);
    },
  };
  const source = await InspectorSource.open(
    readable,
    {},
    file.name,
    new AbortController().signal,
  );
  assert.equal(source.recording.messageCount, 0);
  assert.ok(reads.reduce((sum, r) => sum + r.size, 0) < file.size / 4);
  for (const chunk of source.recording.chunks) {
    assert.ok(
      !reads.some(
        (r) => r.offset === chunk.offset && r.size === chunk.byteLength,
      ),
    );
  }
  reads.length = 0;
  const initial = await source.readWindow(0, 5);
  assert.ok(initial!.messageCount > 0);
  assert.ok(
    initial!.channels.every((channel) =>
      channel.messages.every(
        (message) => message.time >= 0 && message.time <= 5,
      ),
    ),
  );
  const future = source.recording.chunks.find(
    (chunk) => Number(chunk.startTime - source.recording.startTime) / 1e9 > 10,
  );
  assert.ok(future);
  assert.ok(!reads.some((r) => r.offset === future.offset));
  reads.length = 0;
  await source.readWindow(0, 5);
  assert.equal(reads.length, 0, "repeated viewport uses metadata cache");
  const later = await source.readWindow(10, 15);
  assert.ok(
    later!.channels.every((channel) =>
      channel.messages.every(
        (message) => message.time >= 10 && message.time <= 15,
      ),
    ),
  );
  assert.ok(reads.some((r) => r.offset === future.offset));
  assert.equal(
    await source.readWindow(15, 20, () => false),
    undefined,
    "superseded window does not read or publish",
  );
});

function overlappingFile(
  options: {
    compression?: string;
    channelInChunk?: boolean;
    looseTime?: bigint;
  } = {},
): File {
  const writer = new McapRecordBuilder();
  writer.writeHeader({ profile: "", library: "test" });
  const channel = {
    id: 7,
    schemaId: 0,
    topic: "/test",
    messageEncoding: "json",
    metadata: new Map<string, string>(),
  };
  if (options.channelInChunk !== true) {
    writer.writeChannel(channel);
  }
  for (const times of [
    [0n, 6_000_000_000n],
    [4_000_000_000n, 9_000_000_000n],
  ]) {
    const records = new McapRecordBuilder();
    if (options.channelInChunk === true && times[0] === 0n) {
      records.writeChannel(channel);
    }
    for (const logTime of times) {
      records.writeMessage({
        channelId: 7,
        sequence: 1,
        logTime,
        publishTime: logTime,
        data: new Uint8Array(100),
      });
    }
    writer.writeChunk({
      messageStartTime: times[0]!,
      messageEndTime: times[1]!,
      uncompressedSize: BigInt(records.buffer.length),
      uncompressedCrc: 0,
      compression: options.compression ?? "",
      records: options.compression
        ? zstd.compress(records.buffer)
        : records.buffer,
    });
  }
  writer.writeMessage({
    channelId: 7,
    sequence: 2,
    logTime: options.looseTime ?? 5_000_000_000n,
    publishTime: 0n,
    data: new Uint8Array(4000),
  });
  writer.writeDataEnd({ dataSectionCrc: 0 });
  writer.writeFooter({
    summaryStart: 0n,
    summaryOffsetStart: 0n,
    summaryCrc: 0,
  });
  return new File(
    [
      new Uint8Array(MCAP_MAGIC),
      new Uint8Array(writer.buffer),
      new Uint8Array(MCAP_MAGIC),
    ],
    "overlap.mcap",
  );
}

void test("on-demand unindexed mixed records preserve overlap membership and skip loose payloads", async () => {
  const source = await InspectorSource.open(
    fileReadable(overlappingFile()),
    {},
    "overlap",
    new AbortController().signal,
  );
  const result = await source.readWindow(4, 6);
  assert.deepEqual(
    result!.channels[0]!.messages.map((m) => [m.time, m.chunkId]),
    [
      [4, 1],
      [5, null],
      [6, 0],
    ],
  );
  assert.equal(result!.looseCount, 1);
  assert.deepEqual(
    result!.chunks[0]!.ranges.get(7),
    { start: 0, end: 6, count: 2 },
    "chunk outline preserves full extent beyond loaded viewport",
  );
});

void test("readable loading rejects truncation and honors cancellation", async () => {
  const file = overlappingFile();
  await assert.rejects(
    InspectorSource.open(
      fileReadable(file.slice(0, file.size - 1)),
      {},
      "broken",
      new AbortController().signal,
    ),
    /footer|magic/,
  );
  const controller = new AbortController();
  controller.abort();
  await assert.rejects(
    InspectorSource.open(
      fileReadable(file),
      {},
      "cancelled",
      controller.signal,
    ),
    /abort/i,
  );
});

void test("on-demand reads decompress intersecting zstd chunks", async () => {
  await zstd.isLoaded;
  const source = await InspectorSource.open(
    fileReadable(overlappingFile({ compression: "zstd" })),
    await loadDecompressHandlers(),
    "compressed",
    new AbortController().signal,
  );
  const result = await source.readWindow(4, 6);
  assert.deepEqual(
    result!.channels[0]!.messages.map((m) => [m.time, m.chunkId]),
    [
      [4, 1],
      [5, null],
      [6, 0],
    ],
  );
});

void test("unindexed seeks discover channel definitions in earlier chunks, including loose-only windows", async () => {
  const file = overlappingFile({
    channelInChunk: true,
    looseTime: 15_000_000_000n,
  });
  for (const [start, end, expectedChunk] of [
    [8, 9, 1],
    [15, 15, null],
  ] as const) {
    const source = await InspectorSource.open(
      fileReadable(file),
      {},
      "unindexed",
      new AbortController().signal,
    );
    assert.equal(source.recording.channels.length, 0);
    const result = await source.readWindow(start, end);
    assert.equal(result!.channels[0]!.topic, "/test");
    assert.equal(result!.channels[0]!.messages[0]!.chunkId, expectedChunk);
  }
});
