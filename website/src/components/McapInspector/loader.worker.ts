import type { IReadable } from "@mcap/core";
import { loadDecompressHandlers } from "@mcap/support";

import { WindowScheduler } from "./WindowScheduler.ts";
import type { LoaderRequest } from "./model.ts";
import { fileReadable, InspectorSource } from "./source.ts";

let scheduler: WindowScheduler | undefined;
let nextReadId = 0;
const reads = new Map<
  number,
  { resolve: (bytes: Uint8Array) => void; reject: (error: Error) => void }
>();

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
    scheduler?.cancel(data.id);
    return;
  }
  if (data.type === "window") {
    scheduler?.request(data.id, data.start, data.end);
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
    const source = await InspectorSource.open(
      readable,
      await loadDecompressHandlers(),
      data.name,
      new AbortController().signal,
      (fraction) => {
        self.postMessage({ type: "progress", fraction });
      },
    );
    scheduler?.destroy();
    scheduler = new WindowScheduler(source, (message) => {
      self.postMessage(message);
    });
    self.postMessage({ type: "opened", recording: source.recording });
  } catch (error) {
    self.postMessage({
      type: "error",
      message: error instanceof Error ? error.message : String(error),
    });
  }
};
