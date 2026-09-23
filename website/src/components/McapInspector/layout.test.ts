import assert from "node:assert/strict";
import { test } from "node:test";

import {
  groupChunks,
  chunkRows,
  groupTimeRange,
  type ChunkGroup,
} from "./layout.ts";
import { overlappingRecording } from "./testFixture.ts";

void test("packed lanes preserve physical membership despite coincident timestamps", () => {
  const recording = overlappingRecording();
  const rows = chunkRows(groupChunks(recording), "");
  assert.equal(rows.length, 2);
  const seen = new Set();
  for (const row of rows) {
    for (const group of row.groups) {
      for (const child of group.children) {
        for (const message of child.messages) {
          assert.equal(message.chunkId, group.chunk?.id);
          assert.ok(child.channel.messages.includes(message));
          assert.ok(!seen.has(message));
          seen.add(message);
        }
      }
    }
  }
  assert.equal(seen.size, recording.messageCount);
  assert.deepEqual(
    rows.map((row) => groupTimeRange(row.groups[0]!, recording.startTime)),
    [
      { start: 0, end: 2 },
      { start: 1, end: 3 },
    ],
  );
});

void test("filtering preserves chunk extents and only includes matching channels", () => {
  const rows = chunkRows(groupChunks(overlappingRecording()), "/imu");
  assert.equal(rows.length, 2);
  for (const row of rows) {
    assert.equal(row.messages.length, 3);
    assert.equal(row.children.length, 1);
    assert.equal(row.children[0]!.channel.topic, "/imu");
  }
  assert.deepEqual(
    chunkRows(groupChunks(overlappingRecording()), "/missing"),
    [],
  );
});

void test("loose messages stay separate from packed chunks", () => {
  const recording = overlappingRecording();
  const loose = { ...recording.channels[0]!.messages[0]!, chunkId: null };
  recording.channels[0]!.messages.unshift(loose);
  const rows = chunkRows(groupChunks(recording), "");
  assert.equal(rows.length, 3);
  assert.equal(rows[2]!.key, "loose");
  assert.deepEqual(rows[2]!.messages, [loose]);
});

void test("sequential chunks share one lane; overlapping, nested, and touching chunks use the minimum lanes", () => {
  const template = overlappingRecording().chunks[0]!;
  const groups = (ranges: [number, number][]): ChunkGroup[] =>
    ranges.map(([start, end], id) => ({
      key: id,
      chunk: {
        ...template,
        id,
        offset: id * 100,
        startTime: BigInt(start),
        endTime: BigInt(end),
      },
      children: [],
    }));
  assert.equal(
    chunkRows(
      groups([
        [4, 5],
        [0, 1],
        [2, 3],
      ]),
      "",
    ).length,
    1,
  );
  const input = groups([
    [4, 5],
    [0, 3],
    [1, 2],
    [2, 4],
    [0, 0],
  ]);
  const rows = chunkRows(input, "");
  assert.equal(rows.length, 3);
  assert.equal(
    new Set(rows.flatMap((row) => row.groups.map((g) => g.key))).size,
    input.length,
  );
  for (const row of rows) {
    for (let i = 1; i < row.groups.length; i++) {
      assert.ok(
        row.groups[i - 1]!.chunk!.endTime < row.groups[i]!.chunk!.startTime,
      );
    }
  }
  assert.deepEqual(
    chunkRows([...input].reverse(), "").map((row) =>
      row.groups.map((g) => g.key),
    ),
    rows.map((row) => row.groups.map((g) => g.key)),
    "lane assignment is deterministic",
  );
});
