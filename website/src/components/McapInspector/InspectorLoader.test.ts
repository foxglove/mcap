import assert from "node:assert/strict";
import { test } from "node:test";

import { InspectorLoader } from "./InspectorLoader.ts";
import type { LoaderMessage, LoaderRequest } from "./model.ts";
import { overlappingRecording } from "./testFixture.ts";

void test("loader ignores stale windows and sources and never transfers borrowed readable storage", async () => {
  const workers: {
    onmessage?: (event: MessageEvent<LoaderMessage>) => void;
    onerror?: () => void;
    postMessage: (data: LoaderRequest, transfer?: Transferable[]) => void;
    terminate: () => void;
  }[] = [];
  const posts: LoaderRequest[] = [];
  let windows = 0;
  let terminated = 0;
  const loader = new InspectorLoader(
    () => {
      const worker = {
        postMessage: (data: LoaderRequest, transfer?: Transferable[]) => {
          posts.push(data);
          if (transfer) {
            structuredClone(data, { transfer });
          }
        },
        terminate: () => {
          terminated++;
        },
      };
      workers.push(worker);
      return worker as unknown as Worker;
    },
    {
      onCatalog: () => {
        /* Not observed in this test. */
      },
      onWindow: () => {
        windows++;
      },
      onProgress: () => {
        /* Not observed in this test. */
      },
      onBusy: () => {
        /* Not observed in this test. */
      },
      onError: (error) => {
        throw error;
      },
    },
  );
  const borrowed = new Uint8Array([1, 2, 3]);
  await loader.openReadable({
    size: async () => 3n,
    read: async () => borrowed,
  });
  const receive = (index: number, data: LoaderMessage) =>
    workers[index]!.onmessage?.({ data } as MessageEvent<LoaderMessage>);
  receive(0, { type: "read", id: 1, offset: 0n, size: 3n });
  await Promise.resolve();
  await Promise.resolve();
  assert.equal(borrowed.byteLength, 3, "caller retains its buffer");
  assert.ok(posts.some((p) => p.type === "read-result"));
  loader.requestWindow(0, 5);
  loader.requestWindow(10, 5);
  receive(0, { type: "window", id: -1, recording: overlappingRecording() });
  assert.equal(windows, 0);
  loader.openFile(new File([], "next.mcap"));
  receive(0, { type: "opened", recording: overlappingRecording() });
  receive(0, { type: "window", id: 3, recording: overlappingRecording() });
  assert.equal(windows, 0);
  loader.cancel();
  assert.equal(terminated, 2);
});

void test("loaded intervals satisfy zoom-in and panning without loads; duplicate requests and stale progress are ignored", (context) => {
  context.mock.timers.enable({ apis: ["setTimeout"] });
  const posts: LoaderRequest[] = [];
  const phases: ("catalog" | "window" | undefined)[] = [];
  const progress: number[] = [];
  let windows = 0;
  const worker = {
    onmessage: undefined as
      | ((event: MessageEvent<LoaderMessage>) => void)
      | undefined,
    postMessage: (message: LoaderRequest) => {
      posts.push(message);
    },
    terminate: () => {
      /* Fake worker has no resources. */
    },
  };
  const loader = new InspectorLoader(() => worker as unknown as Worker, {
    onCatalog: () => {
      /* Catalog contents not needed. */
    },
    onWindow: () => {
      windows++;
    },
    onProgress: (fraction) => {
      progress.push(fraction);
    },
    onBusy: (phase) => {
      phases.push(phase);
    },
    onError: (error) => {
      throw error;
    },
  });
  const receive = (data: LoaderMessage) =>
    worker.onmessage?.({ data } as MessageEvent<LoaderMessage>);
  loader.openFile(new File([], "test.mcap"));
  receive({ type: "opened", recording: overlappingRecording() });
  phases.length = 0;
  loader.requestWindow(0, 100);
  loader.requestWindow(0, 100); // A vertical pan reports the same time interval.
  loader.requestWindow(5, 5); // Share a pending broader load rather than restarting it.
  context.mock.timers.tick(120);
  const request = posts.find((post) => post.type === "window")!;
  assert.equal(request.type, "window");
  assert.deepEqual(phases, ["window"]);
  receive({ type: "progress", id: request.id, fraction: 0.5 });
  assert.equal(progress.at(-1), 0.5);
  receive({
    type: "window",
    id: request.id,
    recording: {
      ...overlappingRecording(),
      loadedRange: { start: 0, end: 100 },
    },
  });
  phases.length = 0;
  loader.requestWindow(5, 5);
  loader.requestWindow(30, 10);
  context.mock.timers.tick(120);
  assert.equal(
    posts.filter((post) => post.type === "window").length,
    1,
    "zooming inside the loaded full recording does not read again",
  );
  assert.equal(phases.length, 0, "cache hits do not flash loading UI");
  loader.requestWindow(150, 5);
  context.mock.timers.tick(120);
  const outside = posts.at(-1)!;
  assert.equal(outside.type, "window");
  loader.requestWindow(40, 5); // Return to retained metadata while a different request runs.
  assert.equal(posts.at(-1)!.type, "cancel-window");
  const before = progress.length;
  receive({ type: "progress", id: outside.id, fraction: 0.9 });
  receive({
    type: "window",
    id: outside.id,
    recording: overlappingRecording(),
  });
  assert.equal(progress.length, before);
  assert.equal(windows, 1);
  assert.equal(phases.at(-1), undefined);
  loader.cancel();
});
