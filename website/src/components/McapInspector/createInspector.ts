import { createDemo } from "./demo.ts";
import {
  bytes,
  timeLabel,
  type Recording,
  type LoaderMessage,
} from "./model.ts";
import { inspectorStyles } from "./styles.ts";
import { Timeline, COLORS, type Selection } from "./timeline.ts";

interface InspectorElements extends Record<string, HTMLElement> {
  canvas: HTMLCanvasElement;
  tooltip: HTMLDivElement;
  file: HTMLInputElement;
  filter: HTMLInputElement;
  position: HTMLInputElement;
  progress: HTMLProgressElement;
  density: HTMLSelectElement;
  "channel-select": HTMLSelectElement;
  "group-channel": HTMLButtonElement;
  "group-chunk": HTMLButtonElement;
}

export interface InspectorOptions {
  /** The host bundler owns worker creation. A fresh worker is required per load. */
  createWorker: () => Worker;
  onLoad?: (recording: Recording) => void;
  onError?: (error: Error) => void;
}
export interface InspectorHandle {
  loadFile: (file: File) => void;
  setRecording: (recording: Recording) => void;
  focusChunk: (id: number) => void;
  exitChunk: () => void;
  destroy: () => void;
}

/** Mount an isolated inspector. Call destroy before reusing the host or removing it. */
export function createInspector(
  host: HTMLElement,
  options: InspectorOptions,
): InspectorHandle {
  const shadow = host.shadowRoot ?? host.attachShadow({ mode: "open" });
  if (shadow.childElementCount > 0) {
    throw new Error("The inspector host must be empty.");
  }
  const style = document.createElement("style");
  style.textContent = inspectorStyles;
  const app = document.createElement("div");
  shadow.append(style, app);
  const events = new AbortController();
  let destroyed = false;
  app.innerHTML = `
<header><div class="brand"><span class="brand-icon" aria-hidden="true">▥</span><h1>MCAP <span>Chunk explorer</span></h1></div><div class="header-actions"><span class="local">Local files · stays in your browser</span><button id="demo">Load demo</button><button id="open" class="primary">Open MCAP <span aria-hidden="true">↗</span></button><input id="file" type="file" accept=".mcap" hidden /></div></header>
<main>
<section class="file-bar" aria-label="Recording"><div><div class="eyebrow">RECORDING</div><div id="filename">No recording open</div></div><div id="stats" class="stats"><span>Select or drop an MCAP file to explore its structure.</span></div></section>
<div class="toolbar"><label class="search"><span aria-hidden="true">⌕</span><input id="filter" placeholder="Filter topic or channel ID" aria-label="Filter topic or channel ID" /></label><div class="grouping" role="group" aria-label="Group timeline by"><span>Group by</span><button id="group-channel" aria-pressed="true">Channel</button><button id="group-chunk" aria-pressed="false">Chunk</button></div><div class="legend"><span><i class="tick"></i>Message</span><span><i class="chunk"></i>Chunk</span><span><i class="loose"></i>Unchunked</span></div><div class="view-controls"><label>Rows <select id="density" aria-label="Row height"><option value="48">Comfortable</option><option value="34">Compact</option><option value="64">Expanded</option></select></label><button id="zoom-out" aria-label="Zoom out">−</button><button id="zoom-in" aria-label="Zoom in">+</button><button id="fit">Fit recording</button></div></div>
<div id="scope" class="group-hint" hidden><button id="exit-chunk">← Full recording</button><strong id="scope-name"></strong><span>Only messages in this chunk are shown.</span></div>
<div id="group-hint" class="group-hint" hidden><span>Click a left label to expand. Double-click a chunk to open its channel view.</span><button id="expand-chunks">Expand all</button><button id="collapse-chunks">Collapse all</button></div>
<div id="error" role="alert" hidden></div>
<section class="workspace"><div class="plot" id="plot"><canvas id="canvas" tabindex="0" aria-label="MCAP timeline. Drag to pan, scroll for channels, Shift scroll for time, Control or Command scroll to zoom. Arrow keys pan, plus and minus zoom, Home fits recording. Select a channel below for an accessible summary."></canvas><div id="tooltip" class="tooltip" hidden></div>
<div id="empty" class="empty"><div class="empty-mark" aria-hidden="true">▥</div><h2>See how your recording fits together.</h2><p>Drop an MCAP file here, or choose one to begin.</p><button id="choose" class="primary">Choose MCAP file</button><button id="empty-demo" class="text-button">Explore a demo recording</button><small>Messages are ticks. Colored outlines show their chunks.</small></div>
<div id="loading" class="loading" hidden><div class="loading-card"><span class="eyebrow">READING RECORDING</span><h2 id="loading-name"></h2><progress id="progress" max="1" value="0"></progress><p id="progress-label">Preparing decompression…</p><button id="cancel">Cancel</button></div></div></div>
<aside id="inspector"><div class="inspector-heading"><span class="eyebrow">INSPECTOR</span><button id="clear" aria-label="Clear selection">×</button></div><div id="detail"><h2>Follow a message.</h2><p>Hover for a quick look. Click a message or chunk to inspect its timestamps and physical location.</p><div class="sample-colors">${COLORS.map(
    (c) => `<i style="background:${c}"></i>`,
  ).join(
    "",
  )}</div><p class="muted">Colors repeat by physical chunk order. A shared color alone does not mean a shared chunk.</p></div><div class="accessible"><label for="channel-select">Inspect channel</label><select id="channel-select"><option value="">Choose a channel…</option></select></div></aside></section>
<div class="navigator"><span id="view-start">0.000 s</span><input id="position" type="range" min="0" max="10000" value="0" aria-label="Pan through recording"/><span id="view-end">0.000 s</span><span id="window-size">No data</span></div>
<footer><span>Drag to pan <b>·</b> Scroll for channels <b>·</b> Shift + scroll for time <b>·</b> Ctrl / ⌘ + scroll to zoom</span><span id="status" role="status" aria-live="polite">Ready</span></footer>
</main><div id="drop-overlay" hidden><div><span aria-hidden="true">↓</span><h2>Drop your MCAP file</h2><p>Explore messages, channels, and chunks.</p></div></div>`;
  const $ = <K extends keyof InspectorElements>(
    id: K,
  ): InspectorElements[K] => {
    const element = app.querySelector<InspectorElements[K]>(`[id="${id}"]`);
    if (!element) {
      throw new Error(`Missing inspector element: ${id}`);
    }
    return element;
  };
  let recording: Recording | undefined;
  let worker: Worker | undefined;
  let requestId = 0;
  const timeline = new Timeline(
    $("canvas"),
    showSelection,
    (start, span, duration) => {
      $("view-start").textContent = timeLabel(start);
      $("view-end").textContent = timeLabel(start + span);
      $("window-size").textContent = `${timeLabel(span)} window`;
      const input = $("position");
      input.value =
        duration > span ? String((start / (duration - span)) * 10000) : "0";
      input.disabled = duration <= span;
    },
    $("tooltip"),
    (chunk, grouping) => {
      $("scope").hidden = !chunk;
      $("scope-name").textContent = chunk ? `Chunk #${chunk.id}` : "";
      for (const mode of ["channel", "chunk"] as const) {
        $(`group-${mode}`).disabled = !!chunk;
        $(`group-${mode}`).setAttribute(
          "aria-pressed",
          String(mode === grouping),
        );
      }
      $("group-hint").hidden = !!chunk || grouping !== "chunk";
      $("fit").textContent = chunk ? "Fit chunk" : "Fit recording";
    },
  );
  $("exit-chunk").onclick = () => {
    timeline.exitChunk();
  };
  function valueRow(label: string, value: string) {
    const div = document.createElement("div");
    const dt = document.createElement("dt"),
      dd = document.createElement("dd");
    dt.textContent = label;
    dd.textContent = value;
    div.append(dt, dd);
    return div;
  }
  function showSelection(selection?: Selection) {
    const detail = $("detail");
    if (!selection) {
      detail.innerHTML =
        '<h2>Follow a message.</h2><p>Hover for a quick look. Click a message or chunk to inspect its timestamps and physical location.</p><p class="muted">Chunk outlines follow the first and last message on each channel. Gaps inside an outline do not imply missing messages.</p>';
      $("channel-select").value = "";
      return;
    }
    detail.replaceChildren();
    const { channel, message, chunk, unchunked } = selection;
    if (!channel) {
      $("channel-select").value = "";
    }
    const heading = document.createElement("h2");
    heading.textContent = message
      ? "Message"
      : chunk
        ? `Chunk #${chunk.id}`
        : unchunked === true
          ? "Unchunked messages"
          : "Channel";
    detail.append(heading);
    const dl = document.createElement("dl");
    detail.append(dl);
    const add = (label: string, value: string) => {
      dl.append(valueRow(label, value));
    };
    if (channel) {
      add("Channel ID", String(channel.id));
      add("Topic", channel.topic);
      add("Encoding", channel.encoding || "—");
      add("Schema ID", String(channel.schemaId));
      $("channel-select").value = String(channel.id);
    }
    if (unchunked === true && recording) {
      add("Messages outside chunks", recording.looseCount.toLocaleString());
    }
    if (message) {
      add("Relative log time", timeLabel(message.time));
      add("Log time · ns", String(message.logTime));
      add("Publish time · ns", String(message.publishTime));
      add("Sequence", String(message.sequence));
      add("Payload size", bytes(message.size));
      add("Belongs to", chunk ? `Chunk #${chunk.id}` : "Unchunked record");
      if (!chunk) {
        add("Record file offset", `${message.offset.toLocaleString()} B`);
      }
    } else if (channel && !chunk) {
      add("Messages", channel.messages.length.toLocaleString());
    }
    if (chunk && recording) {
      const title = document.createElement("h3");
      title.textContent = `Chunk #${chunk.id}`;
      title.style.color = COLORS[chunk.id % COLORS.length]!;
      detail.append(title);
      const chunkDl = document.createElement("dl");
      detail.append(chunkDl);
      for (const [label, value] of [
        ["Compression", chunk.compression],
        ["Messages", chunk.messageCount.toLocaleString()],
        ["Channels", String(chunk.ranges.size)],
        [
          "Start",
          timeLabel(Number(chunk.startTime - recording.startTime) / 1e9),
        ],
        ["End", timeLabel(Number(chunk.endTime - recording.startTime) / 1e9)],
        ["File offset", `${chunk.offset.toLocaleString()} B`],
        ["Record size", bytes(chunk.byteLength)],
        ["Compressed records", bytes(chunk.compressedSize)],
        ["Uncompressed records", bytes(chunk.uncompressedSize)],
      ]) {
        chunkDl.append(valueRow(label!, value!));
      }
    }
  }
  function fail(message: string) {
    options.onError?.(new Error(message));
    $("error").textContent = message;
    $("error").hidden = false;
    $("status").textContent = "Could not open file";
  }
  function stopLoading() {
    worker?.terminate();
    worker = undefined;
    $("loading").hidden = true;
  }
  function load(file: File) {
    if (destroyed) {
      throw new Error("This inspector has been destroyed.");
    }
    requestId++;
    stopLoading();
    $("error").hidden = true;
    $("loading").hidden = false;
    $("loading-name").textContent = file.name;
    $("progress").value = 0;
    $("progress-label").textContent = "Preparing decompression…";
    $("status").textContent = "Reading file…";
    try {
      worker = options.createWorker();
    } catch (error) {
      stopLoading();
      fail(error instanceof Error ? error.message : String(error));
      return;
    }
    const active = worker;
    worker.onmessage = ({ data }: MessageEvent<LoaderMessage>) => {
      if (worker !== active) {
        return;
      }
      if (data.type === "progress") {
        $("progress").value = data.fraction;
        $("progress-label").textContent = `${Math.round(
          data.fraction * 100,
        )}% · Scanning messages and chunks`;
      } else if (data.type === "error") {
        stopLoading();
        fail(data.message);
      } else {
        recording = data.recording;
        stopLoading();
        renderRecording(recording);
        options.onLoad?.(recording);
      }
    };
    worker.onerror = (event) => {
      if (worker === active) {
        stopLoading();
        fail(event.message || "The file loader stopped unexpectedly.");
      }
    };
    worker.postMessage(file);
  }
  function renderRecording(data: Recording) {
    $("empty").hidden = true;
    $("filename").textContent = data.name;
    $("stats").replaceChildren();
    for (const [value, label] of [
      [data.channels.length.toLocaleString(), "channels"],
      [data.chunks.length.toLocaleString(), "chunks"],
      [data.messageCount.toLocaleString(), "messages"],
      [timeLabel(data.duration), "duration"],
      [bytes(data.fileSize), "file size"],
    ]) {
      const span = document.createElement("span"),
        strong = document.createElement("strong");
      strong.textContent = value!;
      span.append(strong, document.createTextNode(label!));
      $("stats").append(span);
    }
    const select = $("channel-select");
    select.replaceChildren(new Option("Choose a channel…", ""));
    for (const channel of data.channels) {
      select.add(
        new Option(`${channel.id} · ${channel.topic}`, String(channel.id)),
      );
    }
    timeline.setRecording(data);
    $("status").textContent =
      `${data.messageCount.toLocaleString()} messages indexed${
        data.looseCount > 0
          ? ` · ${data.looseCount.toLocaleString()} unchunked`
          : ""
      }`;
  }
  for (const id of ["open", "choose"]) {
    $(id).onclick = () => {
      $("file").click();
    };
  }
  $("file").onchange = (e) => {
    const input = e.target as HTMLInputElement;
    const file = input.files?.[0];
    if (file) {
      load(file);
    }
    input.value = "";
  };
  async function demo() {
    const id = ++requestId;
    try {
      const file = await createDemo();
      if (!destroyed && id === requestId) {
        load(file);
      }
    } catch (e) {
      if (!destroyed && id === requestId) {
        fail(String(e));
      }
    }
  }
  $("demo").onclick = demo;
  $("empty-demo").onclick = demo;
  $("cancel").onclick = () => {
    requestId++;
    stopLoading();
    $("status").textContent = recording
      ? "Previous recording kept · loading cancelled"
      : "Loading cancelled";
  };
  $("filter").oninput = (e) => {
    timeline.setFilter((e.target as HTMLInputElement).value);
  };
  for (const grouping of ["channel", "chunk"] as const) {
    $(`group-${grouping}`).onclick = () => {
      timeline.setGrouping(grouping);
      $("group-channel").setAttribute(
        "aria-pressed",
        String(grouping === "channel"),
      );
      $("group-chunk").setAttribute(
        "aria-pressed",
        String(grouping === "chunk"),
      );
      $("group-hint").hidden = grouping !== "chunk";
    };
  }
  $("expand-chunks").onclick = () => {
    timeline.setChunksExpanded({ expanded: true });
  };
  $("collapse-chunks").onclick = () => {
    timeline.setChunksExpanded({ expanded: false });
  };
  $("density").onchange = (e) => {
    timeline.setRowHeight(Number((e.target as HTMLSelectElement).value));
  };
  $("zoom-in").onclick = () => {
    timeline.zoom(1.8);
  };
  $("zoom-out").onclick = () => {
    timeline.zoom(1 / 1.8);
  };
  $("fit").onclick = () => {
    timeline.fit();
  };
  $("position").oninput = (e) => {
    timeline.seek(Number((e.target as HTMLInputElement).value) / 10000);
  };
  $("clear").onclick = () => {
    timeline.clearSelection();
  };
  $("channel-select").onchange = (e) => {
    const value = (e.target as HTMLSelectElement).value;
    const channel = recording?.channels.find((c) => String(c.id) === value);
    showSelection(channel ? { channel } : undefined);
  };
  let dragDepth = 0;
  app.addEventListener(
    "dragenter",
    (e) => {
      if (e.dataTransfer?.types.includes("Files") === true) {
        e.preventDefault();
        dragDepth++;
        $("drop-overlay").hidden = false;
      }
    },
    { signal: events.signal },
  );
  app.addEventListener(
    "dragover",
    (e) => {
      if (e.dataTransfer?.types.includes("Files") === true) {
        e.preventDefault();
        e.dataTransfer.dropEffect = "copy";
      }
    },
    { signal: events.signal },
  );
  app.addEventListener(
    "dragleave",
    (e) => {
      e.preventDefault();
      if (--dragDepth <= 0) {
        dragDepth = 0;
        $("drop-overlay").hidden = true;
      }
    },
    { signal: events.signal },
  );
  app.addEventListener(
    "drop",
    (e) => {
      e.preventDefault();
      dragDepth = 0;
      $("drop-overlay").hidden = true;
      const files = e.dataTransfer?.files;
      if (files != undefined && files.length > 0) {
        if (files.length > 1) {
          fail("Please drop one MCAP file at a time.");
          return;
        }
        load(files[0]!);
      }
    },
    { signal: events.signal },
  );
  window.addEventListener(
    "blur",
    () => {
      dragDepth = 0;
      $("drop-overlay").hidden = true;
    },
    { signal: events.signal },
  );

  return {
    loadFile: load,
    setRecording(data) {
      if (destroyed) {
        throw new Error("This inspector has been destroyed.");
      }
      requestId++;
      stopLoading();
      recording = data;
      renderRecording(data);
    },
    focusChunk: (id) => {
      timeline.focusChunk(id);
    },
    exitChunk: () => {
      timeline.exitChunk();
    },
    destroy() {
      if (destroyed) {
        return;
      }
      destroyed = true;
      requestId++;
      stopLoading();
      events.abort();
      timeline.destroy();
      recording = undefined;
      app.replaceChildren();
      app.remove();
      style.remove();
    },
  };
}
