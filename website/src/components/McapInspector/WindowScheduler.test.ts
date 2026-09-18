import assert from "node:assert/strict";
import { test } from "node:test";
import { setImmediate } from "node:timers/promises";

import { WindowScheduler } from "./WindowScheduler.ts";
import type { LoaderMessage, Recording } from "./model.ts";
import { overlappingRecording } from "./testFixture.ts";

const snapshot = (start: number, end: number): Recording => ({
  ...overlappingRecording(),
  duration: 20,
  loadedRange: { start, end },
});

void test("idle preloading expands to the full recording without foreground progress", async (context) => {
  context.mock.timers.enable({ apis: ["setTimeout"] });
  const messages: LoaderMessage[] = [];
  const reads: number[][] = [];
  const scheduler = new WindowScheduler(
    {
      readWindow: async (start, end) => {
        reads.push([start, end]);
        return snapshot(start, end);
      },
    },
    (message) => {
      messages.push(message);
    },
  );
  scheduler.request(1, 0, 5);
  await setImmediate();
  assert.deepEqual(reads, [[0, 5]]);
  context.mock.timers.tick(500);
  await setImmediate();
  context.mock.timers.tick(500);
  await setImmediate();
  assert.deepEqual(reads, [
    [0, 5],
    [0, 10],
    [0, 20],
  ]);
  assert.equal(messages.filter((m) => m.type === "window").length, 1);
  assert.equal(messages.filter((m) => m.type === "prefetched").length, 2);
  assert.ok(
    messages.some((m) => m.type === "preload-status" && m.state === "complete"),
  );
  scheduler.destroy();
});

void test("foreground navigation preempts idle reads without concurrent access or stale snapshots", async (context) => {
  context.mock.timers.enable({ apis: ["setTimeout"] });
  const messages: LoaderMessage[] = [];
  const reads: number[][] = [];
  let finish: (() => void) | undefined;
  const scheduler = new WindowScheduler(
    {
      readWindow: async (start, end, current) => {
        reads.push([start, end]);
        if (reads.length === 2) {
          return await new Promise<Recording | undefined>((resolve) => {
            finish = () => {
              resolve(current?.() === false ? undefined : snapshot(start, end));
            };
          });
        }
        return snapshot(start, end);
      },
    },
    (message) => {
      messages.push(message);
    },
  );
  scheduler.request(1, 0, 5);
  await setImmediate();
  context.mock.timers.tick(500);
  await setImmediate();
  scheduler.request(2, 12, 17);
  assert.equal(reads.length, 2);
  finish?.();
  await setImmediate();
  assert.deepEqual(reads[2], [12, 17]);
  assert.ok(!messages.some((m) => m.type === "prefetched"));
  assert.ok(messages.some((m) => m.type === "window" && m.id === 2));
  scheduler.destroy();
});

void test("preload limits pause background work without replacing the current view or surfacing a foreground error", async (context) => {
  context.mock.timers.enable({ apis: ["setTimeout"] });
  let calls = 0;
  const messages: LoaderMessage[] = [];
  const scheduler = new WindowScheduler(
    {
      readWindow: async (start, end) => {
        if (++calls > 1) {
          throw new Error("metadata limit");
        }
        return snapshot(start, end);
      },
    },
    (message) => {
      messages.push(message);
    },
  );
  scheduler.request(1, 0, 5);
  await setImmediate();
  context.mock.timers.tick(500);
  await setImmediate();
  context.mock.timers.tick(5000);
  await setImmediate();
  assert.equal(calls, 2);
  assert.ok(
    messages.some((m) => m.type === "preload-status" && m.state === "paused"),
  );
  assert.ok(
    !messages.some((m) => m.type === "error" || m.type === "prefetched"),
  );
  scheduler.destroy();
});
