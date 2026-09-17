// cspell:words Consolas
import {
  chunkRows,
  groupChunks,
  groupTimeRange,
  matchesChannel,
  type ChunkGroup,
  type Grouping,
  type TimelineRow,
} from "./layout.ts";
import {
  lowerBound,
  timeLabel,
  type Recording,
  type ChannelRow,
  type MessageMark,
  type ChunkInfo,
} from "./model.ts";

export const COLORS = [
  "#5aafff",
  "#43cb96",
  "#d6b64c",
  "#f17d7d",
  "#ba8af5",
  "#53cbd2",
];
export interface Selection {
  channel?: ChannelRow;
  message?: MessageMark;
  chunk?: ChunkInfo;
  unchunked?: boolean;
}
const RULER = 42;
const FONT = "12px ui-monospace, SFMono-Regular, Consolas, monospace";

export class Timeline {
  #ctx: CanvasRenderingContext2D;
  #width = 1;
  #height = 1;
  #labelWidth = 246;
  #rowHeight = 48;
  #scrollY = 0;
  #start = 0;
  #span = 2;
  #recording?: Recording;
  #chunksById = new Map<number, ChunkInfo>();
  #rows: TimelineRow[] = [];
  #grouping: Grouping = "channel";
  #groups?: ChunkGroup[];
  #expanded = new Set<number | "loose">();
  #filter = "";
  #raf = 0;
  #drag?: {
    x: number;
    y: number;
    start: number;
    scrollY: number;
    moved: boolean;
  };
  #hover?: Selection;
  #selected?: Selection;
  #pointer?: { x: number; y: number };
  #observer: ResizeObserver;
  #events = new AbortController();
  #focusedChunk?: ChunkInfo;
  #previousView?: {
    grouping: Grouping;
    start: number;
    span: number;
    scrollY: number;
  };
  #canvas: HTMLCanvasElement;
  #onSelect: (selection?: Selection) => void;
  #onView: (start: number, span: number, duration: number) => void;
  #tooltip: HTMLDivElement;
  #onScopeChange: (chunk: ChunkInfo | undefined, grouping: Grouping) => void;
  constructor(
    canvas: HTMLCanvasElement,
    onSelect: (selection?: Selection) => void,
    onView: (start: number, span: number, duration: number) => void,
    tooltip: HTMLDivElement,
    onScopeChange: (
      chunk: ChunkInfo | undefined,
      grouping: Grouping,
    ) => void = () => {
      /* Scope notifications are optional. */
    },
  ) {
    this.#canvas = canvas;
    this.#onSelect = onSelect;
    this.#onView = onView;
    this.#tooltip = tooltip;
    this.#onScopeChange = onScopeChange;
    const ctx = canvas.getContext("2d");
    if (!ctx) {
      throw new Error("Your browser does not support Canvas 2D.");
    }
    this.#ctx = ctx;
    this.#observer = new ResizeObserver(() => {
      this.#resize();
    });
    this.#observer.observe(canvas);
    canvas.addEventListener(
      "wheel",
      (e) => {
        e.preventDefault();
        const scale =
          e.deltaMode === 1 ? 16 : e.deltaMode === 2 ? this.#height : 1;
        if (e.ctrlKey || e.metaKey) {
          this.zoom(
            Math.exp(-e.deltaY * scale * 0.008),
            this.#anchor(e.offsetX),
          );
        } else if (e.shiftKey) {
          this.pan(
            (((e.deltaY !== 0 ? e.deltaY : e.deltaX) * scale) /
              this.#plotWidth) *
              this.#span,
          );
        } else {
          this.pan(((e.deltaX * scale) / this.#plotWidth) * this.#span);
          this.#scrollY += e.deltaY * scale;
          this.#clamp();
          this.#draw();
        }
        this.#hideTooltip();
      },
      { passive: false, signal: this.#events.signal },
    );
    canvas.addEventListener(
      "dblclick",
      (e) => {
        const chunk = this.#hit(e.offsetX, e.offsetY)?.chunk;
        if (chunk) {
          this.focusChunk(chunk.id);
        }
      },
      { signal: this.#events.signal },
    );
    canvas.addEventListener(
      "pointerdown",
      (e) => {
        if (e.button !== 0) {
          return;
        }
        canvas.focus();
        canvas.setPointerCapture(e.pointerId);
        this.#drag = {
          x: e.offsetX,
          y: e.offsetY,
          start: this.#start,
          scrollY: this.#scrollY,
          moved: false,
        };
        canvas.style.cursor = "grabbing";
        this.#hideTooltip();
      },
      { signal: this.#events.signal },
    );
    canvas.addEventListener(
      "pointermove",
      (e) => {
        if (this.#drag) {
          const dx = e.offsetX - this.#drag.x,
            dy = e.offsetY - this.#drag.y;
          if (Math.abs(dx) + Math.abs(dy) > 4) {
            this.#drag.moved = true;
          }
          if (this.#drag.moved) {
            this.#start =
              this.#drag.start - (dx / this.#plotWidth) * this.#span;
            this.#scrollY = this.#drag.scrollY - dy;
            this.#clamp();
            this.#draw();
            this.#notify();
          }
        } else {
          this.#pointer = { x: e.offsetX, y: e.offsetY };
          this.#hover = this.#hit(e.offsetX, e.offsetY);
          this.#updateTooltip();
          this.#draw();
        }
      },
      { signal: this.#events.signal },
    );
    canvas.addEventListener(
      "pointerup",
      (e) => {
        if (this.#drag && !this.#drag.moved) {
          const row =
            e.offsetY >= RULER
              ? this.#rows[
                  Math.floor(
                    (e.offsetY - RULER + this.#scrollY) / this.#rowHeight,
                  )
                ]
              : undefined;
          if (
            e.offsetY >= RULER &&
            e.offsetX < this.#labelWidth &&
            row?.kind === "group"
          ) {
            if (this.#expanded.has(row.key)) {
              this.#expanded.delete(row.key);
            } else {
              this.#expanded.add(row.key);
            }
            this.#rebuildRows();
            this.#clamp();
          }
          this.#selected =
            row?.kind === "group"
              ? { chunk: row.chunk, unchunked: !row.chunk }
              : this.#hit(e.offsetX, e.offsetY);
          this.#onSelect(this.#selected);
          this.#draw();
        }
        this.#drag = undefined;
        canvas.style.cursor = "grab";
        if (canvas.hasPointerCapture(e.pointerId)) {
          canvas.releasePointerCapture(e.pointerId);
        }
      },
      { signal: this.#events.signal },
    );
    canvas.addEventListener(
      "pointercancel",
      () => {
        this.#drag = undefined;
        canvas.style.cursor = "grab";
      },
      { signal: this.#events.signal },
    );
    canvas.addEventListener(
      "pointerleave",
      () => {
        this.#hover = undefined;
        this.#pointer = undefined;
        this.#hideTooltip();
        this.#draw();
      },
      { signal: this.#events.signal },
    );
    canvas.addEventListener(
      "keydown",
      (e) => {
        let handled = true;
        if (e.key === "+" || e.key === "=") {
          this.zoom(1.5);
        } else if (e.key === "-") {
          this.zoom(1 / 1.5);
        } else if (e.key === "ArrowLeft") {
          this.pan(-this.#span * 0.15);
        } else if (e.key === "ArrowRight") {
          this.pan(this.#span * 0.15);
        } else if (e.key === "ArrowDown") {
          this.#scrollY += this.#rowHeight;
          this.#clamp();
          this.#draw();
        } else if (e.key === "ArrowUp") {
          this.#scrollY -= this.#rowHeight;
          this.#clamp();
          this.#draw();
        } else if (e.key === "Home") {
          this.fit();
        } else if (e.key === "Escape") {
          this.clearSelection();
        } else {
          handled = false;
        }
        if (handled) {
          e.preventDefault();
        }
      },
      { signal: this.#events.signal },
    );
  }
  get #plotWidth() {
    return Math.max(1, this.#width - this.#labelWidth - 18);
  }
  get #extent() {
    return Math.max(this.#recording?.duration ?? 0, 0.001);
  }
  #anchor(x: number) {
    return Math.max(0, Math.min(1, (x - this.#labelWidth) / this.#plotWidth));
  }
  #x(time: number) {
    return (
      this.#labelWidth + ((time - this.#start) / this.#span) * this.#plotWidth
    );
  }
  #notify() {
    this.#onView(this.#start, this.#span, this.#extent);
  }
  #clamp() {
    this.#span = Math.max(
      Math.min(1e-6, this.#extent),
      Math.min(this.#extent, this.#span),
    );
    this.#start = Math.max(0, Math.min(this.#extent - this.#span, this.#start));
    this.#scrollY = Math.max(
      0,
      Math.min(
        Math.max(
          0,
          this.#rows.length * this.#rowHeight - (this.#height - RULER),
        ),
        this.#scrollY,
      ),
    );
  }
  #resize() {
    const rect = this.#canvas.getBoundingClientRect();
    this.#width = rect.width;
    this.#height = rect.height;
    this.#labelWidth = Math.min(246, Math.max(132, this.#width * 0.28));
    const dpr = window.devicePixelRatio > 0 ? window.devicePixelRatio : 1;
    this.#canvas.width = Math.round(rect.width * dpr);
    this.#canvas.height = Math.round(rect.height * dpr);
    this.#ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    this.#clamp();
    this.#draw();
  }
  public setRecording(recording: Recording): void {
    this.#focusedChunk = undefined;
    this.#previousView = undefined;
    this.#onScopeChange(undefined, this.#grouping);
    this.#recording = recording;
    this.#chunksById = new Map(
      recording.chunks.map((chunk) => [chunk.id, chunk]),
    );
    this.#groups = undefined;
    this.#expanded.clear();
    this.#scrollY = 0;
    this.#start = 0;
    this.#span = Math.min(2, Math.max(recording.duration, 0.001));
    this.clearSelection();
    this.setFilter(this.#filter);
    this.#notify();
  }
  public setFilter(filter: string): void {
    this.#filter = filter.toLowerCase();
    this.#rebuildRows();
    this.#scrollY = 0;
    this.#hover = undefined;
    this.#hideTooltip();
    this.#clamp();
    this.#draw();
  }
  #rebuildRows() {
    if (!this.#recording) {
      this.#rows = [];
      return;
    }
    if (this.#focusedChunk) {
      this.#groups ??= groupChunks(this.#recording);
      this.#rows =
        this.#groups
          .find((group) => group.chunk?.id === this.#focusedChunk!.id)
          ?.children.filter((row) =>
            matchesChannel(row.channel, this.#filter),
          ) ?? [];
    } else if (this.#grouping === "channel") {
      this.#rows = this.#recording.channels
        .filter((c) => matchesChannel(c, this.#filter))
        .map((channel) => ({
          kind: "channel",
          channel,
          messages: channel.messages,
        }));
    } else {
      this.#groups ??= groupChunks(this.#recording);
      this.#rows = chunkRows(this.#groups, this.#filter, this.#expanded);
    }
  }
  public setGrouping(grouping: Grouping): void {
    if (this.#focusedChunk) {
      this.exitChunk();
    }
    this.#grouping = grouping;
    this.#rebuildRows();
    this.#scrollY = 0;
    this.#hover = undefined;
    this.#hideTooltip();
    this.#clamp();
    this.#draw();
    // Keep the time window and inspection selection unchanged for direct comparison.
  }
  public focusChunk(id: number): void {
    const chunk = this.#chunksById.get(id);
    if (!chunk || this.#focusedChunk?.id === id) {
      return;
    }
    this.#previousView ??= {
      grouping: this.#grouping,
      start: this.#start,
      span: this.#span,
      scrollY: this.#scrollY,
    };
    this.#focusedChunk = chunk;
    this.#grouping = "channel";
    this.#rebuildRows();
    this.#scrollY = 0;
    this.#hover = undefined;
    this.#hideTooltip();
    this.#start = Number(chunk.startTime - this.#recording!.startTime) / 1e9;
    this.#span = Math.max(1e-6, Number(chunk.endTime - chunk.startTime) / 1e9);
    this.#selected = { chunk };
    this.#clamp();
    this.#onSelect(this.#selected);
    this.#onScopeChange(chunk, this.#grouping);
    this.#notify();
    this.#draw();
  }
  public exitChunk(): void {
    if (!this.#focusedChunk) {
      return;
    }
    this.#focusedChunk = undefined;
    if (this.#previousView) {
      this.#grouping = this.#previousView.grouping;
      this.#start = this.#previousView.start;
      this.#span = this.#previousView.span;
      this.#scrollY = this.#previousView.scrollY;
    }
    this.#previousView = undefined;
    this.#rebuildRows();
    this.#clamp();
    this.#hover = undefined;
    this.#hideTooltip();
    this.#onScopeChange(undefined, this.#grouping);
    this.#notify();
    this.#draw();
  }
  public destroy(): void {
    this.#events.abort();
    this.#observer.disconnect();
    if (this.#raf !== 0) {
      cancelAnimationFrame(this.#raf);
    }
    this.#raf = 0;
    this.#hideTooltip();
    this.#recording = undefined;
    this.#rows = [];
    this.#groups = undefined;
    this.#chunksById.clear();
    this.#selected = undefined;
    this.#hover = undefined;
    this.#focusedChunk = undefined;
    this.#previousView = undefined;
    this.#expanded.clear();
  }
  public setChunksExpanded({ expanded }: { expanded: boolean }): void {
    if (!this.#recording) {
      return;
    }
    this.#groups ??= groupChunks(this.#recording);
    this.#expanded = expanded
      ? new Set(this.#groups.map((group) => group.key))
      : new Set();
    this.#rebuildRows();
    this.#scrollY = 0;
    this.#hover = undefined;
    this.#hideTooltip();
    this.#clamp();
    this.#draw();
  }
  public setRowHeight(value: number): void {
    this.#rowHeight = value;
    this.#clamp();
    this.#draw();
  }
  public clearSelection(): void {
    this.#selected = undefined;
    this.#hover = undefined;
    this.#hideTooltip();
    this.#onSelect();
    this.#draw();
  }
  public zoom(factor: number, anchor = 0.5): void {
    const time = this.#start + this.#span * anchor;
    this.#span = Math.max(1e-6, Math.min(this.#extent, this.#span / factor));
    this.#start = time - this.#span * anchor;
    this.#clamp();
    this.#draw();
    this.#notify();
  }
  public pan(seconds: number): void {
    this.#start += seconds;
    this.#clamp();
    this.#draw();
    this.#notify();
  }
  public seek(fraction: number): void {
    this.#start = fraction * (this.#extent - this.#span);
    this.#clamp();
    this.#draw();
    this.#notify();
  }
  public fit(): void {
    this.#span = this.#focusedChunk
      ? Math.max(
          1e-6,
          Number(this.#focusedChunk.endTime - this.#focusedChunk.startTime) /
            1e9,
        )
      : this.#extent;
    this.#start = this.#focusedChunk
      ? Number(this.#focusedChunk.startTime - this.#recording!.startTime) / 1e9
      : 0;
    this.#clamp();
    this.#draw();
    this.#notify();
  }
  #visibleRows() {
    const first = Math.max(0, Math.floor(this.#scrollY / this.#rowHeight));
    return {
      first,
      last: Math.min(
        this.#rows.length,
        first + Math.ceil((this.#height - RULER) / this.#rowHeight) + 1,
      ),
    };
  }
  #hit(x: number, y: number): Selection | undefined {
    if (!this.#recording || y < RULER) {
      return;
    }
    const index = Math.floor((y - RULER + this.#scrollY) / this.#rowHeight),
      row = this.#rows[index];
    if (!row) {
      return;
    }
    if (row.kind === "group") {
      return { chunk: row.chunk, unchunked: !row.chunk };
    }
    const { channel, messages } = row;
    if (x < this.#labelWidth) {
      return { channel, chunk: row.chunk };
    }
    const time =
      this.#start + ((x - this.#labelWidth) / this.#plotWidth) * this.#span;
    const i = lowerBound(messages, time);
    const candidates = [messages[i], messages[i - 1]].filter(
      (m): m is MessageMark => !!m,
    );
    candidates.sort(
      (a, b) => Math.abs(a.time - time) - Math.abs(b.time - time),
    );
    const message = candidates[0];
    if (message && Math.abs(this.#x(message.time) - x) < 6) {
      return {
        channel,
        message,
        chunk:
          message.chunkId == undefined
            ? undefined
            : this.#chunksById.get(message.chunkId),
      };
    }
    if (this.#grouping === "chunk" || this.#focusedChunk) {
      return { channel, chunk: row.chunk };
    }
    // Match drawing order: later chunks are on top where extents overlap.
    for (let k = this.#recording.chunks.length - 1; k >= 0; k--) {
      const chunk = this.#recording.chunks[k]!,
        range = chunk.ranges.get(channel.id);
      if (
        range &&
        x >= this.#x(range.start) - 2 &&
        x <= this.#x(range.end) + 2
      ) {
        return { channel, chunk };
      }
    }
    return { channel };
  }
  #hideTooltip() {
    this.#tooltip.hidden = true;
  }
  #updateTooltip() {
    if (!this.#hover || !this.#pointer) {
      this.#hideTooltip();
      return;
    }
    const { channel, message, chunk } = this.#hover;
    this.#tooltip.textContent = message
      ? `${channel?.topic ?? "Unknown channel"} · ${timeLabel(
          message.time,
        )} · ${
          chunk ? `Chunk #${chunk.id}` : "Unchunked"
        } · ${message.size.toLocaleString()} B`
      : chunk
        ? `Chunk #${chunk.id} · ${
            chunk.compression
          } · ${chunk.messageCount.toLocaleString()} messages`
        : this.#hover.unchunked === true
          ? "Unchunked messages · click the label to expand channels"
          : `${channel?.id ?? "?"} · ${channel?.topic ?? "Unknown channel"}`;
    this.#tooltip.hidden = false;
    const tipWidth = this.#tooltip.offsetWidth;
    this.#tooltip.style.left = `${Math.max(
      8,
      Math.min(this.#pointer.x + 14, this.#width - tipWidth - 8),
    )}px`;
    this.#tooltip.style.top = `${Math.max(
      8,
      Math.min(this.#pointer.y + 18, this.#height - 58),
    )}px`;
  }
  #draw() {
    if (this.#raf === 0) {
      this.#raf = requestAnimationFrame(() => {
        this.#raf = 0;
        this.#render();
      });
    }
  }
  #chunkPath(chunk: ChunkInfo): Path2D[] {
    const { first, last } = this.#visibleRows(),
      paths: Path2D[] = [];
    let group: { left: number; right: number; top: number; bottom: number }[] =
      [];
    const flush = () => {
      if (group.length === 0) {
        return;
      }
      const p = new Path2D(),
        head = group[0]!;
      p.moveTo(head.left, head.top);
      p.lineTo(head.right, head.top);
      for (let i = 0; i < group.length; i++) {
        const r = group[i]!;
        p.lineTo(r.right, r.bottom);
        const next = group[i + 1];
        if (next) {
          p.lineTo(next.right, r.bottom);
        }
      }
      for (let i = group.length - 1; i >= 0; i--) {
        const r = group[i]!;
        p.lineTo(r.left, r.bottom);
        p.lineTo(r.left, r.top);
        const previous = group[i - 1];
        if (previous) {
          p.lineTo(previous.left, r.top);
        }
      }
      p.closePath();
      paths.push(p);
      group = [];
    };
    for (let row = first; row < last; row++) {
      const item = this.#rows[row]!;
      const range =
        item.kind === "channel" &&
        ((this.#grouping === "channel" && !this.#focusedChunk) ||
          item.chunk?.id === chunk.id)
          ? chunk.ranges.get(item.channel.id)
          : undefined;
      if (!range) {
        flush();
        continue;
      }
      const left = this.#x(range.start),
        right = Math.max(left + 3, this.#x(range.end));
      const top = RULER + row * this.#rowHeight - this.#scrollY;
      group.push({ left, right, top, bottom: top + this.#rowHeight });
    }
    flush();
    return paths;
  }
  #render() {
    const ctx = this.#ctx;
    ctx.clearRect(0, 0, this.#width, this.#height);
    ctx.fillStyle = "#10151b";
    ctx.fillRect(0, 0, this.#width, this.#height);
    const { first, last } = this.#visibleRows();
    ctx.save();
    ctx.beginPath();
    ctx.rect(this.#labelWidth, RULER, this.#plotWidth, this.#height - RULER);
    ctx.clip();
    for (let i = first; i < last; i++) {
      const y = RULER + i * this.#rowHeight - this.#scrollY;
      ctx.fillStyle = i % 2 === 0 ? "#192028" : "#151b22";
      ctx.fillRect(
        this.#labelWidth,
        y + 2,
        this.#plotWidth,
        this.#rowHeight - 4,
      );
    }
    const roughStep =
      this.#span / Math.max(2, Math.floor(this.#plotWidth / 110));
    const power = 10 ** Math.floor(Math.log10(roughStep));
    const step =
      [1, 2, 5, 10].map((n) => n * power).find((n) => n >= roughStep) ?? power;
    ctx.strokeStyle = "#2a323d";
    ctx.lineWidth = 1;
    for (
      let t = Math.ceil(this.#start / step) * step;
      t <= this.#start + this.#span + step * 0.01;
      t += step
    ) {
      const x = this.#x(t);
      ctx.beginPath();
      ctx.moveTo(x, RULER);
      ctx.lineTo(x, this.#height);
      ctx.stroke();
    }
    const highlighted = this.#selected?.chunk?.id ?? this.#hover?.chunk?.id;
    const visibleChunks = this.#focusedChunk
      ? [this.#focusedChunk]
      : this.#grouping === "channel"
        ? this.#recording?.chunks ?? []
        : [
            ...new Set(
              this.#rows
                .slice(first, last)
                .flatMap((row) => (row.chunk ? [row.chunk] : [])),
            ),
          ];
    for (const chunk of visibleChunks) {
      const start = Number(chunk.startTime - this.#recording!.startTime) / 1e9;
      const end = Number(chunk.endTime - this.#recording!.startTime) / 1e9;
      if (end < this.#start || start > this.#start + this.#span) {
        continue;
      }
      const color = COLORS[chunk.id % COLORS.length]!;
      ctx.fillStyle = color;
      ctx.strokeStyle = color;
      ctx.lineWidth = highlighted === chunk.id ? 2 : 1;
      for (const path of this.#chunkPath(chunk)) {
        ctx.globalAlpha =
          highlighted === chunk.id
            ? 0.32
            : highlighted == undefined
              ? 0.16
              : 0.06;
        ctx.fill(path);
        ctx.globalAlpha =
          highlighted == undefined || highlighted === chunk.id ? 0.85 : 0.3;
        ctx.stroke(path);
      }
    }
    ctx.globalAlpha = 1;
    for (let i = first; i < last; i++) {
      const row = this.#rows[i]!,
        y = RULER + i * this.#rowHeight - this.#scrollY + this.#rowHeight / 2;
      if (row.kind === "group") {
        const range = groupTimeRange(row, this.#recording!.startTime);
        if (
          !range ||
          range.end < this.#start ||
          range.start > this.#start + this.#span
        ) {
          continue;
        }
        const left = Math.max(this.#labelWidth, this.#x(range.start));
        const right = Math.min(
          this.#labelWidth + this.#plotWidth,
          Math.max(this.#x(range.start) + 3, this.#x(range.end)),
        );
        const color = row.chunk
          ? COLORS[row.chunk.id % COLORS.length]!
          : "#ffb570";
        ctx.fillStyle = color;
        ctx.strokeStyle = color;
        ctx.globalAlpha =
          highlighted == undefined || highlighted === row.chunk?.id
            ? 0.3
            : 0.12;
        ctx.fillRect(left, y - 12, right - left, 24);
        ctx.globalAlpha = 1;
        ctx.lineWidth = highlighted === row.chunk?.id ? 2 : 1;
        ctx.strokeRect(left + 0.5, y - 11.5, Math.max(1, right - left - 1), 23);
        ctx.font = FONT;
        ctx.textBaseline = "middle";
        ctx.fillStyle = "#e3edf8";
        const label = `${row.shownMessageCount.toLocaleString()} messages · ${
          row.children.length
        } channels`;
        if (right - left > ctx.measureText(label).width + 20) {
          ctx.fillText(label, left + 9, y);
        }
        continue;
      }
      const a = lowerBound(row.messages, this.#start),
        b = lowerBound(row.messages, this.#start + this.#span + 1e-9);
      let previousPixel = -Infinity;
      ctx.beginPath();
      ctx.strokeStyle = "#d7e5f3";
      ctx.lineWidth = 1;
      ctx.globalAlpha = 0.75;
      for (let n = a; n < b; n++) {
        const message = row.messages[n]!,
          x = Math.round(this.#x(message.time)) + 0.5;
        if (x === previousPixel) {
          continue;
        }
        previousPixel = x;
        ctx.moveTo(x, y - 6);
        ctx.lineTo(x, y + 6);
      }
      ctx.stroke();
      // Loose messages get orange marks, retaining a distinct identity from chunks.
      ctx.beginPath();
      ctx.strokeStyle = "#ffb570";
      previousPixel = -Infinity;
      for (let n = a; n < b; n++) {
        const m = row.messages[n]!;
        if (m.chunkId != undefined) {
          continue;
        }
        const x = Math.round(this.#x(m.time)) + 0.5;
        if (x === previousPixel) {
          continue;
        }
        previousPixel = x;
        ctx.moveTo(x, y - 7);
        ctx.lineTo(x, y + 7);
      }
      ctx.stroke();
    }
    ctx.globalAlpha = 1;
    const picked = this.#selected?.message ?? this.#hover?.message;
    if (picked) {
      ctx.strokeStyle = "#ffffff";
      ctx.setLineDash([3, 4]);
      ctx.beginPath();
      ctx.moveTo(this.#x(picked.time), RULER);
      ctx.lineTo(this.#x(picked.time), this.#height);
      ctx.stroke();
      ctx.setLineDash([]);
    }
    ctx.restore();
    // Labels and ruler stay fixed while the plotted data pans.
    ctx.fillStyle = "#111820";
    ctx.fillRect(0, 0, this.#labelWidth, this.#height);
    ctx.fillRect(0, 0, this.#width, RULER);
    ctx.font = FONT;
    ctx.textBaseline = "middle";
    ctx.fillStyle = "#8394a8";
    ctx.fillText(
      this.#grouping === "channel" ? "CHANNEL / TOPIC" : "CHUNK / CHANNEL",
      18,
      21,
    );
    ctx.save();
    ctx.beginPath();
    ctx.rect(this.#labelWidth, 0, this.#plotWidth + 18, RULER);
    ctx.clip();
    for (
      let t = Math.ceil(this.#start / step) * step;
      t <= this.#start + this.#span + step * 0.01;
      t += step
    ) {
      ctx.fillStyle = "#93a5b9";
      ctx.fillText(
        `${t.toFixed(
          Math.max(0, Math.min(9, -Math.floor(Math.log10(step)))),
        )} s`,
        this.#x(t) + 5,
        21,
      );
    }
    ctx.restore();
    ctx.save();
    ctx.beginPath();
    ctx.rect(0, RULER, this.#labelWidth - 8, this.#height - RULER);
    ctx.clip();
    for (let i = first; i < last; i++) {
      const row = this.#rows[i]!,
        y = RULER + i * this.#rowHeight - this.#scrollY;
      ctx.font = FONT;
      if (row.kind === "group") {
        const color = row.chunk
          ? COLORS[row.chunk.id % COLORS.length]!
          : "#ffb570";
        ctx.fillStyle = color;
        ctx.fillText(row.expanded ? "▾" : "▸", 14, y + this.#rowHeight / 2 - 4);
        this.#drawTruncatedText(
          row.chunk ? `Chunk #${row.chunk.id}` : "Unchunked",
          34,
          y + this.#rowHeight / 2 - 4,
          this.#labelWidth - 45,
        );
        if (this.#rowHeight >= 42) {
          ctx.fillStyle = "#8aa1b9";
          ctx.fillText(
            `${row.children.length} channels · ${
              row.expanded ? "collapse" : "expand"
            }`,
            34,
            y + this.#rowHeight / 2 + 12,
          );
        }
        continue;
      }
      ctx.fillStyle = "#64819e";
      ctx.fillText(
        String(row.channel.id).padStart(2, "0"),
        this.#grouping === "chunk" ? 27 : 18,
        y + this.#rowHeight / 2 - 4,
      );
      ctx.fillStyle = "#dbe5ef";
      this.#drawTruncatedText(
        row.channel.topic,
        70,
        y + this.#rowHeight / 2 - 4,
        this.#labelWidth - 82,
      );
      if (this.#rowHeight >= 42) {
        ctx.fillStyle = "#73869a";
        ctx.fillText(
          `${row.messages.length.toLocaleString()} messages`,
          70,
          y + this.#rowHeight / 2 + 12,
        );
      }
    }
    ctx.restore();
    ctx.strokeStyle = "#2b3542";
    ctx.beginPath();
    ctx.moveTo(this.#labelWidth - 0.5, 0);
    ctx.lineTo(this.#labelWidth - 0.5, this.#height);
    ctx.moveTo(0, RULER - 0.5);
    ctx.lineTo(this.#width, RULER - 0.5);
    ctx.stroke();
    if (this.#rows.length === 0 && this.#recording) {
      ctx.fillStyle = "#93a5b9";
      ctx.font = "14px system-ui";
      ctx.fillText(
        this.#filter ? "No matching channels" : "No channels in this recording",
        this.#labelWidth + 24,
        85,
      );
    }
    if (this.#rows.length * this.#rowHeight > this.#height - RULER) {
      const track = this.#height - RULER,
        total = this.#rows.length * this.#rowHeight;
      ctx.fillStyle = "#52647a";
      ctx.fillRect(
        this.#width - 7,
        RULER + (this.#scrollY / total) * track,
        4,
        Math.max(18, (track * track) / total),
      );
    }
  }
  #drawTruncatedText(value: string, x: number, y: number, max: number) {
    if (this.#ctx.measureText(value).width <= max) {
      this.#ctx.fillText(value, x, y);
      return;
    }
    let end = value.length;
    while (
      end > 0 &&
      this.#ctx.measureText(value.slice(0, end) + "…").width > max
    ) {
      end--;
    }
    this.#ctx.fillText(value.slice(0, end) + "…", x, y);
  }
}
