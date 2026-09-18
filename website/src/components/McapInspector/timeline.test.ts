import assert from "node:assert/strict";
import { test } from "node:test";

import type { ChunkInfo } from "./model.ts";
import { overlappingRecording } from "./testFixture.ts";
import { Timeline, type Selection } from "./timeline.ts";

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
  let frame: (() => void) | undefined;
  install("requestAnimationFrame", (callback: () => void) => {
    frame = callback;
    return 1;
  });
  const drawing: { method: string; args: unknown[] }[] = [];
  const context = new Proxy<Record<string, unknown>>(
    {},
    {
      get(target, key: string) {
        if (key === "measureText") {
          return (value: string) => ({ width: value.length * 7 });
        }
        return (
          target[key] ??
          ((...args: unknown[]) => drawing.push({ method: key, args }))
        );
      },
    },
  );
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
  canvas.getContext = () => context;
  canvas.style = {};
  canvas.focus = () => {
    /* Mock focus. */
  };
  canvas.setPointerCapture = () => {
    /* Mock capture. */
  };
  canvas.hasPointerCapture = () => false;
  canvas.getBoundingClientRect = () => ({ width: 1000, height: 500 });
  const tooltip = { hidden: true } as HTMLDivElement;
  let scope: ChunkInfo | undefined;
  let selection: Selection | undefined;
  let changes = 0;
  let grouping = "";
  let view = [0, 0];
  let timeline: Timeline | undefined;
  try {
    timeline = new Timeline(
      canvas as unknown as HTMLCanvasElement,
      (picked) => {
        selection = picked;
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
    timeline.focusChunk(1);
    timeline.setGrouping("chunk");
    assert.equal(scope, undefined, "grouping exits drill-down directly");
    assert.equal(grouping, "chunk");
    timeline.setGrouping("channel");
    timeline.selectChannel(2);
    assert.equal(selection?.channel?.id, 2);
    timeline.setShowChunks({ visible: false });
    const beforeUpdate = [...view];
    timeline.updateRecording(overlappingRecording());
    assert.deepEqual(view, beforeUpdate, "window results preserve viewport");
    assert.equal(selection.channel.id, 2, "window results preserve selection");
    timeline.clearSelection();
    assert.equal(selection, undefined);
    timeline.fit();
    // A message at t=1 on the second row: its marker must identify both time and channel.
    for (const type of ["pointerdown", "pointerup"]) {
      const click = new Event(type);
      Object.defineProperties(click, {
        offsetX: { value: 246 + 736 / 3 },
        offsetY: { value: 114 },
        button: { value: 0 },
        pointerId: { value: 1 },
      });
      canvas.dispatchEvent(click);
    }
    assert.equal((selection as Selection | undefined)?.message?.time, 1);
    assert.equal((selection as Selection | undefined)?.channel?.id, 2);
    frame?.();
    assert.ok(
      drawing.some(
        (call) =>
          call.method === "strokeRect" &&
          call.args[1] === 101 &&
          call.args[2] === 12 &&
          call.args[3] === 26,
      ),
      "selected tick gets a visible box on the selected row",
    );
    assert.ok(
      !drawing.some((call) => call.method === "fill"),
      "chunk polygons are hidden in ticks-only mode",
    );
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
