import assert from "node:assert/strict";
import { test } from "node:test";

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
