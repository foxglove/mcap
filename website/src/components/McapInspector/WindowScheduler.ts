import type { LoaderMessage, Recording } from "./model.ts";
import type { InspectorSource } from "./source.ts";

/** Serializes reads; visible requests preempt idle expansion at chunk boundaries. */
export class WindowScheduler {
  #source: Pick<InspectorSource, "readWindow">;
  #post: (message: LoaderMessage) => void;
  #generation = 0;
  #pending?: { id: number; start: number; end: number };
  #reading = false;
  #recording?: Recording;
  #timer?: ReturnType<typeof setTimeout>;
  #paused = false;
  constructor(
    source: Pick<InspectorSource, "readWindow">,
    post: (message: LoaderMessage) => void,
  ) {
    this.#source = source;
    this.#post = post;
  }
  request(id: number, start: number, end: number): void {
    clearTimeout(this.#timer);
    this.#generation = id;
    this.#pending = { id, start, end };
    this.#paused = false;
    void this.#drain();
  }
  cancel(id: number): void {
    this.#generation = id;
    this.#pending = undefined;
    clearTimeout(this.#timer);
    if (!this.#reading) {
      this.#schedule();
    }
  }
  destroy(): void {
    this.#generation++;
    this.#pending = undefined;
    this.#paused = true;
    clearTimeout(this.#timer);
  }
  async #drain() {
    if (this.#reading) {
      return;
    }
    this.#reading = true;
    try {
      while (this.#pending) {
        const request = this.#pending;
        this.#pending = undefined;
        try {
          let lastProgress = 0;
          const recording = await this.#source.readWindow(
            request.start,
            request.end,
            () => request.id === this.#generation,
            (fraction) => {
              if (
                request.id === this.#generation &&
                (fraction === 0 ||
                  fraction === 1 ||
                  Date.now() - lastProgress > 80)
              ) {
                lastProgress = Date.now();
                this.#post({ type: "progress", id: request.id, fraction });
              }
            },
          );
          if (recording && request.id === this.#generation) {
            this.#recording = recording;
            this.#post({ type: "window", id: request.id, recording });
          }
        } catch (error) {
          if (request.id === this.#generation) {
            this.#paused = true;
            this.#post({
              type: "error",
              id: request.id,
              message: error instanceof Error ? error.message : String(error),
            });
          }
        }
      }
    } finally {
      this.#reading = false;
      this.#schedule();
    }
  }
  #schedule() {
    clearTimeout(this.#timer);
    const range = this.#recording?.loadedRange;
    if (this.#paused || !range || !this.#recording) {
      return;
    }
    if (range.start <= 0 && range.end >= this.#recording.duration) {
      this.#post({
        type: "preload-status",
        id: this.#generation,
        state: "complete",
      });
      return;
    }
    this.#timer = setTimeout(() => {
      void this.#preload();
    }, 500);
  }
  async #preload() {
    const recording = this.#recording;
    const range = recording?.loadedRange;
    if (this.#reading || this.#pending || this.#paused || !range) {
      return;
    }
    const id = this.#generation;
    const step = Math.max(5, range.end - range.start);
    const start = Math.max(0, range.start - step / 2);
    const end = Math.min(
      recording.duration,
      range.end + step - (range.start - start),
    );
    this.#reading = true;
    this.#post({ type: "preload-status", id, state: "loading" });
    try {
      const expanded = await this.#source.readWindow(
        start,
        end,
        () => id === this.#generation,
      );
      if (expanded && id === this.#generation) {
        this.#recording = expanded;
        this.#post({ type: "prefetched", id, recording: expanded });
      }
    } catch {
      // Limits or unavailable future data must not break the user's current view.
      if (id === this.#generation) {
        this.#paused = true;
        this.#post({ type: "preload-status", id, state: "paused" });
      }
    } finally {
      this.#resume();
    }
  }
  #resume() {
    this.#reading = false;
    if (this.#pending) {
      void this.#drain();
    } else {
      this.#schedule();
    }
  }
}
