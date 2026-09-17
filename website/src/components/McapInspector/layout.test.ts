import assert from "node:assert/strict";
import { test } from "node:test";

import { groupChunks, chunkRows, groupTimeRange } from "./layout.ts";
import { overlappingRecording } from "./testFixture.ts";

void test("physical chunk groups remain distinct despite coincident timestamps", () => {
  const recording = overlappingRecording();
  const groups = groupChunks(recording);
  const rows = chunkRows(groups, "", new Set([0, 1]));
  const seen = new Set();
  for (const row of rows) {
    if (row.kind === "group") {
      continue;
    }
    for (const message of row.messages) {
      assert.equal(message.chunkId, row.chunk?.id);
      assert.ok(row.channel.messages.includes(message));
      assert.ok(!seen.has(message));
      seen.add(message);
    }
  }
  assert.equal(seen.size, recording.messageCount);
  const collapsed = chunkRows(groups, "", new Set());
  assert.equal(collapsed.length, 2);
  assert.deepEqual(
    collapsed.map((row) =>
      row.kind === "group"
        ? groupTimeRange(row, recording.startTime)
        : undefined,
    ),
    [
      { start: 0, end: 2 },
      { start: 1, end: 3 },
    ],
  );
});

void test("filtering preserves physical extents but counts only matching channels", () => {
  const rows = chunkRows(
    groupChunks(overlappingRecording()),
    "/imu",
    new Set([0, 1]),
  );
  assert.equal(rows.length, 4);
  for (const row of rows) {
    if (row.kind === "group") {
      assert.equal(row.shownMessageCount, 3);
      assert.equal(row.children.length, 1);
    } else {
      assert.equal(row.channel.topic, "/imu");
    }
  }
  assert.deepEqual(
    chunkRows(groupChunks(overlappingRecording()), "/missing", new Set()),
    [],
  );
});

void test("loose messages do not become members of overlapping chunks", () => {
  const recording = overlappingRecording();
  const loose = { ...recording.channels[0]!.messages[0]!, chunkId: null };
  recording.channels[0]!.messages.unshift(loose);
  const groups = groupChunks(recording);
  const looseGroup = groups.find((group) => group.key === "loose");
  assert.ok(looseGroup);
  assert.equal(looseGroup.chunk, undefined);
  assert.deepEqual(looseGroup.children[0]!.messages, [loose]);
});
