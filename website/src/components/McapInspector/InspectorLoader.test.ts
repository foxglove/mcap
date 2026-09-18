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
