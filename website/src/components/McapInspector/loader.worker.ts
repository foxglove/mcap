import type { IReadable } from "@mcap/core";
import { loadDecompressHandlers } from "@mcap/support";

import type { LoaderRequest } from "./model.ts";
import { fileReadable, InspectorSource } from "./source.ts";

let source: InspectorSource | undefined;
let generation = 0;
let pending: { id: number; start: number; end: number } | undefined;
let reading = false;
let nextReadId = 0;
const reads = new Map<
  number,
  { resolve: (bytes: Uint8Array) => void; reject: (error: Error) => void }
>();

async function drain() {
  if (reading || !source) {
    return;
  }
  reading = true;
  try {
    while (pending) {
      const request = pending;
      pending = undefined;
      try {
        let lastProgress = 0;
        const recording = await source.readWindow(
          request.start,
          request.end,
          () => request.id === generation,
          (fraction) => {
            if (
              request.id === generation &&
              (fraction === 0 ||
                fraction === 1 ||
                Date.now() - lastProgress > 80)
            ) {
              lastProgress = Date.now();
              self.postMessage({ type: "progress", id: request.id, fraction });
            }
          },
        );
        if (recording && request.id === generation) {
          self.postMessage({ type: "window", id: request.id, recording });
        }
      } catch (error) {
        if (request.id === generation) {
          self.postMessage({
            type: "error",
            id: request.id,
            message: error instanceof Error ? error.message : String(error),
          });
        }
      }
    }
  } finally {
    reading = false;
  }
}

self.onmessage = async ({ data }: MessageEvent<LoaderRequest>) => {
  if (data.type === "read-result") {
    const waiting = reads.get(data.id);
    reads.delete(data.id);
    if (data.error) {
      waiting?.reject(new Error(data.error));
    } else if (data.bytes) {
      waiting?.resolve(data.bytes);
    }
    return;
  }
  if (data.type === "cancel-window") {
    generation = data.id;
    pending = undefined;
    return;
  }
  if (data.type === "window") {
    generation = data.id;
    pending = data;
    await drain();
    return;
  }
  try {
    const readable: IReadable = data.file
      ? fileReadable(data.file)
      : {
          size: async () => data.size!,
          read: async (offset, size) =>
            await new Promise<Uint8Array>((resolve, reject) => {
              const id = nextReadId++;
              reads.set(id, { resolve, reject });
              self.postMessage({ type: "read", id, offset, size });
            }),
        };
    source = await InspectorSource.open(
      readable,
      await loadDecompressHandlers(),
      data.name,
      new AbortController().signal,
      (fraction) => {
        self.postMessage({ type: "progress", fraction });
      },
    );
    self.postMessage({ type: "opened", recording: source.recording });
  } catch (error) {
    self.postMessage({
      type: "error",
      message: error instanceof Error ? error.message : String(error),
    });
  }
};
