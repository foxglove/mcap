import type { IReadable } from "@mcap/core";

import type { Recording } from "./model.ts";

export interface InspectorOptions {
  /** Maximum canvas viewport height in CSS pixels. Defaults to 520. */
  maxHeight?: number;
  /** Compatibility alias for maxHeight. Prefer maxHeight in new integrations. */
  height?: number;
  createWorker?: () => Worker;
  onLoad?: (recording: Recording) => void;
  onError?: (error: Error) => void;
}
export interface InspectorHandle {
  loadFile: (file: File) => void;
  loadReadable: (readable: IReadable, name?: string) => void;
  setRecording: (recording: Recording) => void;
  focusChunk: (id: number) => void;
  exitChunk: () => void;
  destroy: () => void;
}
export type InspectorControls = Omit<InspectorHandle, "destroy">;
