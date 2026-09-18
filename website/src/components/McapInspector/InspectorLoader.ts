import type { IReadable } from "@mcap/core";

import type { LoaderMessage, LoaderRequest, Recording } from "./model.ts";

export interface LoaderCallbacks {
  onCatalog: (recording: Recording) => void;
  onWindow: (recording: Recording) => void;
  onProgress: (fraction: number) => void;
  onBusy: (phase: "catalog" | "window" | undefined) => void;
  onError: (error: Error) => void;
  onPreload?: (state: "loading" | "complete" | "paused" | undefined) => void;
}

/** Owns one worker and discards results from superseded sources/time windows. */
export class InspectorLoader {
  #createWorker: () => Worker;
  #callbacks: LoaderCallbacks;
  #worker?: Worker;
  #request = 0;
  #loadedRange?: { start: number; end: number };
  #requestedRange?: { start: number; end: number };
  #opening = 0;
  #timer?: ReturnType<typeof setTimeout>;
  constructor(createWorker: () => Worker, callbacks: LoaderCallbacks) {
    this.#createWorker = createWorker;
    this.#callbacks = callbacks;
  }
  openFile(file: File): void {
    this.cancel();
    this.#open({ type: "open", file, name: file.name });
  }
  async openReadable(readable: IReadable, name = "MCAP source"): Promise<void> {
    this.cancel();
    const opening = this.#opening;
    this.#callbacks.onBusy("catalog");
    try {
      const size = await readable.size();
      if (opening === this.#opening) {
        this.#open({ type: "open", size, name }, readable);
      }
    } catch (error) {
      if (opening === this.#opening) {
        this.#fail(error);
      }
    }
  }
  #open(request: LoaderRequest, readable?: IReadable) {
    this.#callbacks.onBusy("catalog");
    this.#callbacks.onProgress(0);
    try {
      const worker = this.#createWorker();
      this.#worker = worker;
      worker.onmessage = ({ data }: MessageEvent<LoaderMessage>) => {
        if (this.#worker !== worker) {
          return;
        }
        switch (data.type) {
          case "read":
            void this.#read(worker, readable, data);
            break;
          case "progress":
            if (data.id != undefined && data.id !== this.#request) {
              return;
            }
            this.#callbacks.onProgress(data.fraction);
            break;
          case "opened":
            this.#callbacks.onBusy(undefined);
            this.#callbacks.onCatalog(data.recording);
            break;
          case "preload-status":
            if (data.id === this.#request) {
              this.#callbacks.onPreload?.(data.state);
            }
            break;
          case "prefetched":
            if (data.id !== this.#request || this.#requestedRange) {
              return;
            }
            if (
              !data.recording.loadedRange ||
              (this.#loadedRange &&
                (data.recording.loadedRange.start > this.#loadedRange.start ||
                  data.recording.loadedRange.end < this.#loadedRange.end))
            ) {
              return;
            }
            this.#loadedRange = data.recording.loadedRange;
            this.#callbacks.onWindow(data.recording);
            break;
          case "window":
            if (data.id !== this.#request) {
              return;
            }
            this.#loadedRange = data.recording.loadedRange;
            this.#requestedRange = undefined;
            this.#callbacks.onBusy(undefined);
            this.#callbacks.onWindow(data.recording);
            break;
          case "error":
            if (data.id != undefined && data.id !== this.#request) {
              return;
            }
            this.#fail(new Error(data.message));
            break;
        }
      };
      worker.onerror = (event) => {
        if (worker === this.#worker) {
          this.#fail(
            new Error(event.message || "The file loader stopped unexpectedly."),
          );
        }
      };
      worker.postMessage(request);
    } catch (error) {
      this.#fail(error);
    }
  }
  async #read(
    worker: Worker,
    readable: IReadable | undefined,
    request: Extract<LoaderMessage, { type: "read" }>,
  ) {
    try {
      if (!readable) {
        throw new Error("No readable supplied.");
      }
      const result = await readable.read(request.offset, request.size);
      if (worker !== this.#worker) {
        return;
      }
      // IReadable may return borrowed storage: never detach the caller's buffer.
      const bytes = new Uint8Array(result);
      worker.postMessage(
        { type: "read-result", id: request.id, bytes } satisfies LoaderRequest,
        [bytes.buffer],
      );
    } catch (error) {
      if (worker === this.#worker) {
        worker.postMessage({
          type: "read-result",
          id: request.id,
          error: error instanceof Error ? error.message : String(error),
        } satisfies LoaderRequest);
      }
    }
  }
  requestWindow(start: number, span: number): void {
    if (!this.#worker) {
      return;
    }
    const end = start + span;
    // The canvas already owns all metadata in this interval. Keep that snapshot
    // when narrowing the view, even if its chunks were evicted from the worker LRU.
    if (
      this.#loadedRange &&
      start >= this.#loadedRange.start &&
      end <= this.#loadedRange.end
    ) {
      if (this.#requestedRange) {
        clearTimeout(this.#timer);
        this.#worker.postMessage({
          type: "cancel-window",
          id: ++this.#request,
        } satisfies LoaderRequest);
        this.#requestedRange = undefined;
        this.#callbacks.onBusy(undefined);
      }
      return;
    }
    // Repeated notifications and narrower views can share the outstanding load.
    if (
      this.#requestedRange &&
      start >= this.#requestedRange.start &&
      end <= this.#requestedRange.end
    ) {
      return;
    }
    this.#callbacks.onPreload?.(undefined);
    this.#requestedRange = { start, end };
    const id = ++this.#request;
    clearTimeout(this.#timer);
    this.#callbacks.onProgress(0);
    this.#callbacks.onBusy("window");
    this.#timer = setTimeout(() => {
      this.#worker?.postMessage({
        type: "window",
        id,
        start,
        end: start + span,
      } satisfies LoaderRequest);
    }, 120);
  }
  #fail(error: unknown) {
    this.#requestedRange = undefined;
    this.#callbacks.onBusy(undefined);
    this.#callbacks.onError(
      error instanceof Error ? error : new Error(String(error)),
    );
  }
  cancel(): void {
    this.#opening++;
    this.#request++;
    this.#loadedRange = undefined;
    this.#requestedRange = undefined;
    clearTimeout(this.#timer);
    this.#worker?.terminate();
    this.#worker = undefined;
    this.#callbacks.onPreload?.(undefined);
    this.#callbacks.onBusy(undefined);
  }
}
