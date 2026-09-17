import assert from "node:assert/strict";
import { test } from "node:test";

import type { ChunkInfo } from "./model.ts";
import { overlappingRecording } from "./testFixture.ts";
import { Timeline } from "./timeline.ts";

void test("double-click drills into a chunk, back restores the viewport, and disposal removes listeners", () => {
  const previous = new Map<string, PropertyDescriptor | undefined>();
  const install = (name: string, value: unknown) => {
    previous.set(name, Object.getOwnPropertyDescriptor(globalThis, name));
    Object.defineProperty(globalThis, name, {
      configurable: true,
      writable: true,
      value,
    });
  };
  install("window", { devicePixelRatio: 1 });
  install("requestAnimationFrame", () => 1);
  install("cancelAnimationFrame", () => {
    /* No rendering is needed in this state test. */
  });
  install(
    "ResizeObserver",
    class {
      #callback: () => void;
      constructor(callback: () => void) {
        this.#callback = callback;
      }
      observe() {
        this.#callback();
      }
      disconnect() {
        /* No native observer in this test. */
      }
    },
  );
  const canvas = new EventTarget() as EventTarget & Record<string, unknown>;
  canvas.getContext = () => ({
    setTransform() {
      /* Canvas rendering is mocked. */
    },
  });
  canvas.getBoundingClientRect = () => ({ width: 1000, height: 500 });
  const tooltip = { hidden: true } as HTMLDivElement;
  let scope: ChunkInfo | undefined;
  let changes = 0;
  let grouping = "";
  let view = [0, 0];
  let timeline: Timeline | undefined;
  try {
    timeline = new Timeline(
      canvas as unknown as HTMLCanvasElement,
      () => {
        /* No rendering is needed in this state test. */
      },
      (start, span) => {
        view = [start, span];
      },
      tooltip,
      (chunk, mode) => {
        scope = chunk;
        grouping = mode;
        changes++;
      },
    );
    timeline.setRecording(overlappingRecording());
    timeline.setGrouping("chunk");
    timeline.zoom(2);
    timeline.pan(0.4);
    const before = [...view];
    const event = new Event("dblclick");
    Object.defineProperties(event, {
      offsetX: { value: 400 },
      offsetY: { value: 110 },
    });
    canvas.dispatchEvent(event); // The second collapsed chunk row.
    assert.equal(scope?.id, 1);
    assert.equal(grouping, "channel");
    assert.deepEqual(view, [1, 2]);
    timeline.exitChunk();
    assert.equal(scope, undefined);
    assert.equal(grouping, "chunk");
    assert.deepEqual(view, before);
    timeline.destroy();
    const atDisposal = changes;
    canvas.dispatchEvent(event);
    assert.equal(changes, atDisposal);
  } finally {
    timeline?.destroy();
    for (const [name, descriptor] of previous) {
      if (descriptor) {
        Object.defineProperty(globalThis, name, descriptor);
      } else {
        Reflect.deleteProperty(globalThis, name);
      }
    }
  }
});
