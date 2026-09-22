import assert from "node:assert/strict";
import { test } from "node:test";

import { frequencyLabel, messageFrequency } from "./model.ts";
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

void test("frequency uses observed nanosecond intervals, including sparse and simultaneous messages", () => {
  const epoch = 1_750_000_000_000_000_000n;
  assert.equal(frequencyLabel(11, epoch, epoch + 1_000_000_000n), "10 Hz");
  assert.equal(frequencyLabel(2, epoch, epoch + 10_000_000_000n), "0.1 Hz");
  assert.equal(
    frequencyLabel(2, epoch, epoch + 100_000n),
    `${(10000).toLocaleString()} Hz`,
  );
  assert.equal(frequencyLabel(2, epoch, epoch + 3_000_000_000n), "0.3 Hz");
  assert.equal(
    frequencyLabel(12347, epoch, epoch + 10_000_000_000n),
    `${(1234.6).toLocaleString()} Hz`,
  );
  assert.equal(frequencyLabel(0), "— Hz");
  assert.equal(frequencyLabel(1, epoch, epoch), "— Hz");
  assert.equal(frequencyLabel(10, epoch, epoch), "— Hz");
  const messages = overlappingRecording().channels[0]!.messages;
  assert.equal(
    messageFrequency(messages),
    frequencyLabel(
      messages.length,
      messages[0]!.logTime,
      messages[messages.length - 1]!.logTime,
    ),
  );
});
