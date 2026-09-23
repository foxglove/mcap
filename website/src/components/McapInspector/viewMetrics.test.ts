import assert from "node:assert/strict";
import { test } from "node:test";

import { frequencyLabel, frequencyWindow } from "./model.ts";
import { overlappingRecording } from "./testFixture.ts";
import { compressionRatio, inspectorHeight } from "./viewMetrics.ts";

void test("viewport height keeps details usable and obeys host minimum and maximum", () => {
  assert.equal(inspectorHeight(1, 600), 320);
  assert.equal(inspectorHeight(0, 600), 320);
  assert.equal(inspectorHeight(8, 600), 428);
  assert.equal(inspectorHeight(1, 600, 400), 400);
  assert.equal(inspectorHeight(1, 200, 400), 200);
  assert.equal(inspectorHeight(1, 600, Number.NaN), 320);
  assert.equal(inspectorHeight(4, 600), 320);
  assert.equal(inspectorHeight(100, 600), 600);
  assert.equal(inspectorHeight(1, 60), 60);
  assert.equal(inspectorHeight(undefined, 600), 320);
});

void test("recording compression ratio weights chunk bytes instead of averaging ratios", () => {
  const { chunks } = overlappingRecording();
  chunks[0]!.uncompressedSize = 1000;
  chunks[0]!.compressedSize = 100;
  chunks[1]!.uncompressedSize = 1000;
  chunks[1]!.compressedSize = 1000;
  assert.equal(compressionRatio(chunks), 2000 / 1100);
  assert.equal(compressionRatio([]), undefined);
  assert.equal(
    compressionRatio([
      { ...chunks[0]!, uncompressedSize: 0, compressedSize: 0 },
    ]),
    undefined,
  );
});

void test("manual height overrides row fitting while respecting host constraints", () => {
  assert.equal(inspectorHeight(100, 1200, 320, 450), 450);
  assert.equal(inspectorHeight(1, 1200, 320, 900), 900);
  assert.equal(inspectorHeight(1, 600, 320, 900), 600);
  assert.equal(inspectorHeight(1, 600, 320, 100), 320);
});

void test("frequency averages over the recording duration, including silence", () => {
  // Five log messages in a 1 ms burst must use all 39.273 s, not that burst.
  assert.equal(frequencyLabel(5, 39.273), "0.1 Hz");
  assert.equal(frequencyLabel(4887, 39.273), "124.4 Hz");
  assert.equal(frequencyLabel(10, 1), "10 Hz");
  assert.equal(frequencyLabel(2, 60), "<0.1 Hz");
  assert.equal(frequencyLabel(2, 40), "0.1 Hz");
  assert.equal(frequencyLabel(12346, 10), `${(1234.6).toLocaleString()} Hz`);
  assert.equal(frequencyLabel(10, 0), "— Hz");
  assert.equal(frequencyLabel(10, Number.NaN), "— Hz");
  assert.equal(frequencyLabel(10), "— Hz");
  assert.equal(frequencyLabel(1, 39.273), "— Hz");
});

void test("frequency interval matches loaded counts and intersects chunk drill-down", () => {
  const recording = overlappingRecording();
  assert.deepEqual(frequencyWindow(recording), {
    start: 0,
    end: 3,
    duration: 3,
  });
  assert.equal(frequencyLabel(6, frequencyWindow(recording)?.duration), "2 Hz");
  recording.partial = true;
  assert.equal(
    frequencyWindow(recording),
    undefined,
    "catalog has no known loaded interval",
  );
  recording.loadedRange = { start: 0.5, end: 1.5 };
  assert.deepEqual(frequencyWindow(recording), {
    start: 0.5,
    end: 1.5,
    duration: 1,
  });
  assert.deepEqual(frequencyWindow(recording, recording.chunks[1]), {
    start: 1,
    end: 1.5,
    duration: 0.5,
  });
  recording.loadedRange = { start: 0, end: 0.5 };
  assert.equal(frequencyWindow(recording, recording.chunks[1]), undefined);
  recording.loadedRange = { start: -1, end: 10 };
  assert.deepEqual(frequencyWindow(recording), {
    start: 0,
    end: 3,
    duration: 3,
  });
  assert.deepEqual(frequencyWindow(recording, recording.chunks[1]), {
    start: 1,
    end: 3,
    duration: 2,
  });
});
