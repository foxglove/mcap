import { loadDecompressHandlers } from "@mcap/support";

import { parseMcap } from "./parse.ts";

self.onmessage = async ({ data }: MessageEvent<File>) => {
  try {
    const handlers = await loadDecompressHandlers();
    const recording = await parseMcap(data, handlers, (fraction) => {
      self.postMessage({ type: "progress", fraction });
    });
    self.postMessage({ type: "loaded", recording });
  } catch (error) {
    self.postMessage({
      type: "error",
      message: error instanceof Error ? error.message : String(error),
    });
  }
};
