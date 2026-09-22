import type { IReadable } from "@mcap/core";
import React, {
  forwardRef,
  useEffect,
  useImperativeHandle,
  useRef,
  useState,
  useMemo,
  type CSSProperties,
} from "react";

import { InspectorLoader } from "./InspectorLoader.ts";
import type { InspectorControls, InspectorOptions } from "./InspectorTypes.ts";
import { ResizeHandle } from "./ResizeHandle.tsx";
import { SelectionDetails } from "./SelectionDetails.tsx";
import { ViewportNavigator } from "./ViewportNavigator.tsx";
import { createDemo } from "./demo.ts";
import type { Grouping } from "./layout.ts";
import {
  bytes,
  frequencyLabel,
  timeLabel,
  type ChunkInfo,
  type Recording,
} from "./model.ts";
import { Timeline, type Selection } from "./timeline.ts";
import { compressionRatio, inspectorHeight } from "./viewMetrics.ts";

export interface InspectorAppProps extends InspectorOptions {
  file?: File;
  readable?: IReadable;
  name?: string;
  createWorker: () => Worker;
}

export const InspectorApp = forwardRef<InspectorControls, InspectorAppProps>(
  function InspectorAppView(props, ref) {
    const canvas = useRef<HTMLCanvasElement>(null);
    const tooltip = useRef<HTMLDivElement>(null);
    const picker = useRef<HTMLInputElement>(null);
    const timeline = useRef<Timeline | undefined>(undefined);
    const loader = useRef<InspectorLoader | undefined>(undefined);
    const callbacks = useRef(props);
    const demoGeneration = useRef(0);
    const dragDepth = useRef(0);
    const [recording, setRecording] = useState<Recording>();
    const [selection, setSelection] = useState<Selection>();
    const [sidebar, setSidebar] = useState(true);
    const [grouping, setGrouping] = useState<Grouping>("channel");
    const [scope, setScope] = useState<ChunkInfo>();
    const [showChunks, setShowChunks] = useState(true);
    const [filter, setFilter] = useState("");
    const [view, setView] = useState({ start: 0, span: 0, duration: 0 });
    const [busy, setBusy] = useState<"catalog" | "window">();
    const [progress, setProgress] = useState(0);
    const [preload, setPreload] = useState<"loading" | "complete" | "paused">();
    const [error, setError] = useState<string>();
    const [drop, setDrop] = useState(false);
    const [rowCount, setRowCount] = useState<number>();
    const [requestedHeight, setRequestedHeight] = useState<number>();
    const [column, setColumn] = useState({ width: 246, max: 600 });
    const heightCap =
      props.maxHeight ??
      props.height ??
      (requestedHeight == undefined ? 520 : 1200);
    const resizeHeightCap = props.maxHeight ?? props.height ?? 1200;
    const minimumHeight = inspectorHeight(
      0,
      resizeHeightCap,
      props.minHeight,
      0,
    );
    const maximumHeight = inspectorHeight(
      0,
      resizeHeightCap,
      props.minHeight,
      Number.MAX_SAFE_INTEGER,
    );
    const height = inspectorHeight(
      rowCount,
      heightCap,
      props.minHeight,
      requestedHeight,
    );
    const frequency = useMemo(() => {
      let first: bigint | undefined, last: bigint | undefined;
      for (const channel of recording?.channels ?? []) {
        const start = channel.messages[0]?.logTime;
        const end = channel.messages[channel.messages.length - 1]?.logTime;
        if (start != undefined && (first == undefined || start < first)) {
          first = start;
        }
        if (end != undefined && (last == undefined || end > last)) {
          last = end;
        }
      }
      return frequencyLabel(recording?.messageCount ?? 0, first, last);
    }, [recording]);
    const ratio = useMemo(
      () => compressionRatio(recording?.chunks ?? []),
      [recording?.chunks],
    );
    useEffect(() => {
      callbacks.current = props;
    });
    useEffect(() => {
      const fail = (failure: Error) => {
        setError(failure.message);
        callbacks.current.onError?.(failure);
      };
      const mounted = new Timeline(
        canvas.current!,
        (picked) => {
          setSelection(picked);
          if (picked) {
            setSidebar(true);
          }
        },
        (start, span, duration) => {
          setView({ start, span, duration });
          loader.current?.requestWindow(start, span);
        },
        tooltip.current!,
        (chunk, mode) => {
          setScope(chunk);
          setGrouping(mode);
        },
        setRowCount,
        (width, max) => {
          setColumn({ width, max });
        },
      );
      const mountedLoader = new InspectorLoader(
        callbacks.current.createWorker,
        {
          onCatalog: (data) => {
            setError(undefined);
            setRecording(data);
            mounted.setRecording(data);
            callbacks.current.onLoad?.(data);
          },
          onWindow: (data) => {
            setError(undefined);
            setRecording(data);
            mounted.updateRecording(data);
          },
          onProgress: setProgress,
          onPreload: setPreload,
          onBusy: setBusy,
          onError: fail,
        },
      );
      timeline.current = mounted;
      loader.current = mountedLoader;
      return () => {
        // This is a request counter, not a DOM ref; invalidate in-flight demo creation.
        // eslint-disable-next-line react-hooks/exhaustive-deps
        demoGeneration.current++;
        mountedLoader.cancel();
        mounted.destroy();
        timeline.current = undefined;
        loader.current = undefined;
      };
    }, []);
    const loadFile = (file: File) => {
      demoGeneration.current++;
      setError(undefined);
      loader.current?.openFile(file);
    };
    const loadReadable = (readable: IReadable, name?: string) => {
      demoGeneration.current++;
      setError(undefined);
      void loader.current?.openReadable(readable, name);
    };
    useEffect(() => {
      if (props.file && !props.readable) {
        loadFile(props.file);
      }
    }, [props.file, props.readable]);
    useEffect(() => {
      if (props.readable) {
        loadReadable(props.readable, props.name);
      }
    }, [props.readable, props.name]);
    useImperativeHandle(ref, () => ({
      loadFile,
      loadReadable,
      setRecording: (data) => {
        demoGeneration.current++;
        loader.current?.cancel();
        setError(undefined);
        setRecording(data);
        timeline.current?.setRecording(data);
      },
      focusChunk: (id) => timeline.current?.focusChunk(id),
      exitChunk: () => timeline.current?.exitChunk(),
    }));
    async function demo() {
      const generation = ++demoGeneration.current;
      try {
        const file = await createDemo();
        if (generation === demoGeneration.current) {
          loadFile(file);
        }
      } catch (failure) {
        if (generation === demoGeneration.current) {
          const e =
            failure instanceof Error ? failure : new Error(String(failure));
          setError(e.message);
          callbacks.current.onError?.(e);
        }
      }
    }
    function closeSidebar() {
      setSidebar(false);
      timeline.current?.clearSelection();
    }
    function changeGrouping(mode: Grouping) {
      timeline.current?.setGrouping(mode);
      setGrouping(mode);
    }
    const channels = scope
      ? recording?.channels.filter((channel) => scope.ranges.has(channel.id)) ??
        []
      : recording?.channels ?? [];
    return (
      <div
        onKeyDown={(event) => {
          if (event.key === "Escape" && scope) {
            event.preventDefault();
            timeline.current?.exitChunk();
          }
        }}
        onDragEnter={(event) => {
          if (event.dataTransfer.types.includes("Files")) {
            event.preventDefault();
            dragDepth.current++;
            setDrop(true);
          }
        }}
        onDragOver={(event) => {
          if (event.dataTransfer.types.includes("Files")) {
            event.preventDefault();
            event.dataTransfer.dropEffect = "copy";
          }
        }}
        onDragLeave={() => {
          if (--dragDepth.current <= 0) {
            dragDepth.current = 0;
            setDrop(false);
          }
        }}
        onDrop={(event) => {
          event.preventDefault();
          dragDepth.current = 0;
          setDrop(false);
          const files = event.dataTransfer.files;
          if (files.length > 1) {
            setError("Please drop one MCAP file at a time.");
          } else if (files[0]) {
            loadFile(files[0]);
          }
        }}
      >
        <header>
          <div className="brand">
            <span className="brand-icon" aria-hidden="true">
              ▥
            </span>
            <h1>
              MCAP <span>Chunk explorer</span>
            </h1>
          </div>
          <div className="header-actions">
            <span className="local">Files stay in your browser</span>
            <button onClick={() => void demo()}>Load demo</button>
            <button className="primary" onClick={() => picker.current?.click()}>
              Open MCAP ↗
            </button>
            <input
              ref={picker}
              type="file"
              accept=".mcap"
              hidden
              aria-label="Choose MCAP file"
              onChange={(event) => {
                const file = event.target.files?.[0];
                if (file) {
                  loadFile(file);
                }
                event.target.value = "";
              }}
            />
          </div>
        </header>
        <main>
          <section className="file-bar" aria-label="Recording">
            <div>
              <div className="eyebrow">RECORDING</div>
              <div id="filename">{recording?.name ?? "No recording open"}</div>
            </div>
            <div className="stats">
              {recording ? (
                <>
                  <span>
                    <strong>{recording.channels.length}</strong>known channels
                  </span>
                  <span>
                    <strong>{recording.chunks.length}</strong>chunks
                  </span>
                  <span>
                    <strong>
                      {recording.messageCount.toLocaleString()} · {frequency}
                    </strong>
                    {recording.partial === true
                      ? "loaded messages"
                      : "messages"}
                  </span>
                  <span>
                    <strong>{timeLabel(recording.duration)}</strong>duration
                  </span>
                  <span>
                    <strong>{bytes(recording.fileSize)}</strong>file size
                  </span>
                  <span title="Total uncompressed chunk bytes divided by stored chunk bytes, across all chunks. Excludes file headers, indexes, and unchunked messages.">
                    <strong>
                      {ratio == undefined ? "—" : `${ratio.toFixed(2)}x`}
                    </strong>
                    compression ratio
                  </span>
                </>
              ) : (
                <span>
                  Select or drop an MCAP file to explore its structure.
                </span>
              )}
            </div>
          </section>
          <div className="toolbar">
            <label className="search">
              <span aria-hidden="true">⌕</span>
              <input
                value={filter}
                placeholder="Filter topic or channel ID"
                aria-label="Filter topic or channel ID"
                onChange={(event) => {
                  setFilter(event.target.value);
                  timeline.current?.setFilter(event.target.value);
                }}
              />
            </label>
            <div
              className="grouping"
              role="group"
              aria-label="Group timeline by"
            >
              <span>Group by</span>
              <button
                aria-pressed={grouping === "channel"}
                onClick={() => {
                  changeGrouping("channel");
                }}
              >
                Channels
              </button>
              <button
                aria-pressed={grouping === "chunk"}
                onClick={() => {
                  changeGrouping("chunk");
                }}
              >
                Chunks
              </button>
            </div>
            <button
              aria-pressed={showChunks}
              onClick={() => {
                setShowChunks(!showChunks);
                timeline.current?.setShowChunks({ visible: !showChunks });
              }}
            >
              Chunk outlines {showChunks ? "on" : "off"}
            </button>
            <div className="view-controls">
              <button
                aria-label="Zoom out"
                onClick={() => timeline.current?.zoom(1 / 1.8)}
              >
                −
              </button>
              <button
                aria-label="Zoom in"
                onClick={() => timeline.current?.zoom(1.8)}
              >
                +
              </button>
              <button onClick={() => timeline.current?.fit()}>
                {scope ? "Fit chunk" : "Fit recording"}
              </button>
              <button
                aria-pressed={sidebar}
                onClick={() => {
                  if (sidebar) {
                    closeSidebar();
                  } else {
                    setSidebar(true);
                  }
                }}
              >
                Details
              </button>
            </div>
          </div>
          {scope && (
            <div className="group-hint">
              <button onClick={() => timeline.current?.exitChunk()}>
                ← Full recording
              </button>
              <strong>Chunk #{scope.id}</strong>
              <span>Only messages in this chunk are shown.</span>
            </div>
          )}
          {!scope && grouping === "chunk" && (
            <div className="group-hint">
              <span>
                Sequential chunks share a lane; overlapping chunks use separate
                lanes. Double-click a chunk for its channel view.
              </span>
            </div>
          )}
          {error && (
            <div id="error" role="alert">
              {error}
            </div>
          )}
          <section
            className="workspace"
            style={
              { "--mcap-inspector-height": `${height}px` } as CSSProperties
            }
          >
            <div className="plot">
              <canvas
                ref={canvas}
                tabIndex={0}
                aria-label="MCAP timeline. Drag to pan, scroll for channels, Shift scroll for time, Control or Command scroll to zoom. Arrow keys pan, plus and minus zoom, Home fits recording."
              />
              <ResizeHandle
                axis="x"
                label="Resize channel and topic column"
                value={column.width}
                min={Math.min(120, column.max)}
                max={column.max}
                style={{ left: column.width }}
                onChange={(width) => timeline.current?.setLabelWidth(width)}
                onReset={() => timeline.current?.setLabelWidth(undefined)}
              />
              <div ref={tooltip} className="tooltip" hidden />
              {!recording && busy !== "catalog" && (
                <div className="empty">
                  <div className="empty-mark" aria-hidden="true">
                    ▥
                  </div>
                  <h2>See how your recording fits together.</h2>
                  <p>Drop an MCAP file here, or choose one to begin.</p>
                  <button
                    className="primary"
                    onClick={() => picker.current?.click()}
                  >
                    Choose MCAP file
                  </button>
                  <button className="text-button" onClick={() => void demo()}>
                    Explore a demo recording
                  </button>
                  <small>
                    Messages are ticks. Colored outlines show their chunks.
                  </small>
                </div>
              )}
              {busy === "catalog" && (
                <div className="loading">
                  <div className="loading-card">
                    <h2>Reading recording structure</h2>
                    <progress
                      max={1}
                      value={progress}
                      aria-label="Reading recording structure"
                    />
                    <span>{Math.round(progress * 100)}%</span>
                    <p>
                      Locating chunks without reading their message payloads.
                    </p>
                    <button onClick={() => loader.current?.cancel()}>
                      Cancel
                    </button>
                  </div>
                </div>
              )}
              {busy === "window" && (
                <div className="window-loading" role="status">
                  <span>
                    Loading visible messages… {Math.round(progress * 100)}%
                  </span>
                  <progress
                    max={1}
                    value={progress}
                    aria-label="Loading visible messages"
                  />
                </div>
              )}
            </div>
            {sidebar && (
              <aside aria-label="Details">
                <div className="inspector-heading">
                  <span className="eyebrow">DETAILS</span>
                  <button aria-label="Close details" onClick={closeSidebar}>
                    ×
                  </button>
                </div>
                <label className="channel-picker">
                  Inspect channel
                  <select
                    value={selection?.channel?.id ?? ""}
                    onChange={(event) =>
                      timeline.current?.selectChannel(
                        event.target.value === ""
                          ? undefined
                          : Number(event.target.value),
                      )
                    }
                  >
                    <option value="">Choose a channel…</option>
                    {channels.map((channel) => (
                      <option key={channel.id} value={channel.id}>
                        {channel.id} · {channel.topic}
                      </option>
                    ))}
                  </select>
                </label>
                <SelectionDetails selection={selection} recording={recording} />
              </aside>
            )}
          </section>
          <ResizeHandle
            axis="y"
            label="Resize inspector height"
            value={height}
            min={minimumHeight}
            max={maximumHeight}
            onChange={setRequestedHeight}
            onReset={() => {
              setRequestedHeight(undefined);
            }}
          />
          <ViewportNavigator
            {...view}
            onSeek={(fraction) => timeline.current?.seek(fraction)}
          />
          <footer>
            <span>
              Drag to pan · Scroll for channels · Shift + scroll for time · Ctrl
              / ⌘ + scroll to zoom
            </span>
            <span role="status">
              {busy
                ? "Loading…"
                : preload === "loading"
                  ? "Preloading in the background…"
                  : preload === "complete"
                    ? "Recording preloaded"
                    : preload === "paused"
                      ? "Preloading paused · load on demand"
                      : recording?.partial === true
                        ? "Messages load as you pan and zoom"
                        : "Ready"}
            </span>
          </footer>
        </main>
        {drop && (
          <div id="drop-overlay">
            <div>
              <span aria-hidden="true">↓</span>
              <h2>Drop your MCAP file</h2>
            </div>
          </div>
        )}
      </div>
    );
  },
);
