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
  let resize: (() => void) | undefined;
  install(
    "ResizeObserver",
    class {
      #callback: () => void;
      constructor(callback: () => void) {
        this.#callback = callback;
        resize = callback;
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
  const rect = { width: 1000, height: 500 };
  canvas.getBoundingClientRect = () => rect;
  const tooltip = { hidden: true } as HTMLDivElement;
  let scope: ChunkInfo | undefined;
  let selection: Selection | undefined;
  let changes = 0;
  let viewChanges = 0;
  let rows = 0;
  let grouping = "";
  let columnWidth = 0;
  let view = [0, 0];
  let timeline: Timeline | undefined;
  try {
    timeline = new Timeline(
      canvas as unknown as HTMLCanvasElement,
      (picked) => {
        selection = picked;
      },
      (start, span) => {
        viewChanges++;
        view = [start, span];
      },
      tooltip,
      (chunk, mode) => {
        scope = chunk;
        grouping = mode;
        changes++;
      },
      (count) => {
        rows = count;
      },
      (width) => {
        columnWidth = width;
      },
    );
    timeline.setRecording(overlappingRecording());
    assert.equal(rows, 2);
    timeline.setFilter("camera");
    assert.equal(rows, 1, "height tracks filtered rows");
    timeline.setFilter("");
    const notifications = viewChanges;
    const wheel = new Event("wheel", { cancelable: true });
    Object.defineProperties(wheel, {
      deltaX: { value: 0 },
      deltaY: { value: 40 },
      deltaMode: { value: 0 },
    });
    canvas.dispatchEvent(wheel);
    assert.equal(
      viewChanges,
      notifications,
      "vertical scrolling does not notify the loader",
    );
    timeline.setGrouping("chunk");
    timeline.zoom(2);
    timeline.pan(0.4);
    const before = [...view];
    const event = new Event("dblclick");
    Object.defineProperties(event, {
      offsetX: { value: 650 },
      offsetY: { value: 110 },
    });
    canvas.dispatchEvent(event); // The second collapsed chunk row.
    assert.equal(scope?.id, 1);
    assert.equal(grouping, "channel");
    assert.deepEqual(view, [1, 2]);
    const escape = new Event("keydown", { cancelable: true });
    Object.defineProperty(escape, "key", { value: "Escape" });
    canvas.dispatchEvent(escape);
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
    const sequential = overlappingRecording();
    sequential.duration = 6;
    sequential.chunks[1]!.startTime += 3_000_000_000n;
    sequential.chunks[1]!.endTime += 3_000_000_000n;
    for (const channel of sequential.channels) {
      for (const message of channel.messages) {
        if (message.chunkId === 1) {
          message.time += 3;
          message.logTime += 3_000_000_000n;
        }
      }
      channel.messages.sort((a, b) => a.time - b.time);
    }
    timeline.setRecording(sequential);
    timeline.setShowChunks({ visible: true });
    timeline.setGrouping("chunk");
    timeline.fit();
    assert.equal(rows, 1, "sequential chunks share one row");
    frame?.();
    assert.ok(
      drawing.some(
        (call) => call.method === "fillText" && call.args[0] === "#1",
      ),
      "chunk ID is rendered inside its lane outline",
    );
    const packedClick = new Event("dblclick");
    Object.defineProperties(packedClick, {
      offsetX: { value: 246 + (736 * 5) / 6 },
      offsetY: { value: 66 },
    });
    canvas.dispatchEvent(packedClick);
    assert.equal(
      (scope as ChunkInfo | undefined)?.id,
      1,
      "hit-testing resolves the clicked chunk within a shared row",
    );
    canvas.dispatchEvent(escape);
    assert.equal(scope, undefined);
    const gapClick = new Event("dblclick");
    Object.defineProperties(gapClick, {
      offsetX: { value: 614 },
      offsetY: { value: 66 },
    });
    canvas.dispatchEvent(gapClick);
    assert.equal(
      scope,
      undefined,
      "empty space between chunks does not select a chunk",
    );
    const beforeResize = [...view];
    const requestsBeforeResize = viewChanges;
    timeline.setLabelWidth(340);
    assert.equal(columnWidth, 340);
    rect.width = 350;
    resize?.();
    assert.equal(columnWidth, 232, "column leaves room for the time plot");
    rect.width = 1000;
    resize?.();
    assert.equal(
      columnWidth,
      340,
      "requested width survives a temporary narrow layout",
    );
    assert.deepEqual(view, beforeResize);
    assert.equal(
      viewChanges,
      requestsBeforeResize,
      "resizing does not request more data",
    );
    const resizedClick = new Event("dblclick");
    Object.defineProperties(resizedClick, {
      offsetX: { value: 340 + (642 * 5) / 6 },
      offsetY: { value: 66 },
    });
    canvas.dispatchEvent(resizedClick);
    assert.equal(
      (scope as ChunkInfo | undefined)?.id,
      1,
      "hit-testing uses the resized column",
    );
    canvas.dispatchEvent(escape);
    timeline.setLabelWidth(undefined);
    assert.equal(columnWidth, 246, "reset restores automatic column sizing");
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
