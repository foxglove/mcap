# MCAP inspector

Reusable React UI for exploring physical MCAP message/chunk structure. The website mounts it at `/inspect`; it is not a published npm package.

```tsx
import { McapInspector } from "./components/McapInspector/index.ts";

<McapInspector file={selectedFile} maxHeight={560} />;
// Or supply any seekable @mcap/core IReadable, including a remote range reader:
<McapInspector readable={readable} name="robot.mcap" onError={reportError} />;
```

Both inputs are optional; the component includes file selection and drag and drop. A new input object starts a new load. Prefer one input; `readable` takes precedence if both are supplied. `onLoad(recording)` fires when the structural catalog is ready, before viewport messages have loaded. Catalog snapshots have `partial: true`; `messageCount` counts loaded marks, and optional `totalMessageCount` comes from MCAP statistics. `onError(error)` reports loading failures.

The canvas height follows the visible rows (including filtering, chunk drill-down, and packed lanes), up to `maxHeight` in CSS pixels (default 520). The legacy `height` prop is an alias for this cap. The same options are accepted by `createInspector`. On narrow screens the sidebar stacks below the capped canvas.

Each instance owns its worker, canvas, view state, and shadow root. Unmount terminates its worker, disconnects the resize observer, cancels pending drawing/loading, and removes event listeners. Multiple inspectors can share a page. Importing the component is safe during server rendering.

For non-React hosts, `createInspector(element, options)` mounts the same React UI. The empty host must be exclusively owned by the inspector. Its handle exposes:

- `loadFile(file)` and `loadReadable(readable, name?)`.
- `setRecording(recording)` for an existing, complete metadata snapshot.
- `focusChunk(id)` and `exitChunk()`.
- `destroy()` to unmount and release resources before reusing the host.

An optional `createWorker` factory supports alternative bundlers. The default uses `new Worker(new URL("./loader.worker.ts", import.meta.url), { type: "module" })`. `IReadable` stays on the caller's thread; the worker requests byte ranges through a small bridge. Returned buffers are copied before transfer so a reader's borrowed storage is never detached. Terminating the worker ignores pending read results; it does not abort external I/O owned by the caller.

For `setRecording`, channel message arrays must be sorted by log time, times must be seconds relative to `recording.startTime`, and message chunk IDs must refer to unique chunk IDs. Exact nanosecond timestamps remain `bigint`.

## Navigation and inspection

- **Channels** groups messages by channel ID and topic. **Chunks** packs non-overlapping chunks into shared lanes, allocating extra lanes only for overlaps. IDs appear inside chunk outlines (or in hover details when an outline is too narrow). MCAP time ranges include both endpoints, so chunks sharing an exact timestamp occupy separate lanes. Either grouping button exits a chunk drill-down directly.
- **Double-click a chunk** to fit its time range and display its channels/messages. **Full recording** or **Escape** restores the previous grouping, time window, and vertical position.
- Click a message tick to pin its details. An amber box marks the exact tick, its channel is highlighted, and a dashed cursor marks its time.
- The channel picker sits at the top of the sidebar. Selecting a channel highlights it and scrolls its row into view.
- **×** closes the sidebar and clears its selection. The right-aligned **Details** button or a new canvas selection reopens it.
- **Chunk outlines** hides/shows chunk shapes. Ticks remain visible; chunk lanes show their combined ticks when outlines are off.
- The overview band represents the actual viewport start and end within the recording. Drag its control to pan, or focus it and use arrow keys. The exact visible interval is printed alongside it.
- Drag the canvas to pan; scroll vertically for rows, Shift + scroll for time, and Ctrl/Command + scroll to zoom. Arrow keys pan/scroll, +/− zoom, Home fits, and Escape leaves chunk drill-down or otherwise clears the canvas selection.
- Rows always use the comfortable height. Filtering matches channel IDs and topic names.
- Recording stats include the aggregate compression ratio: summed uncompressed chunk bytes divided by summed stored chunk bytes, displayed as a multiplier such as `2.13x`. This excludes file overhead and unchunked messages; recordings without chunk data show a dash.

## On-demand reading and limits

The first view covers up to five seconds. Opening a source builds a structural catalog by reading top-level record headers and metadata, seeking past message payloads, chunk bodies, attachments, and indexes. This is a header scan across the file, **not** a complete payload scan. It works with indexed, unindexed, and mixed loose/chunked recordings, including overlapping chunks. A file with many top-level records or a high-latency range reader can still take time to catalog.

Vertical scrolling does not request data. The canvas retains its latest completed time range: zooming in or panning within that range performs no worker request and shows no loading indicator, even if the worker LRU evicted those chunks. Repeated notifications share a pending request; returning to retained data cancels an outstanding request for a different interval. Moving outside the retained range can still require reads after cache eviction.

Panning and zooming outside the retained range load only chunks that overlap the requested interval. A centered, non-blocking progress indicator reports chunk processing weighted by stored bytes, followed by final metadata preparation. Progress advances at chunk boundaries; it cannot update during synchronous decompression of a single large chunk. Compressed chunks are read/decompressed in full; individual messages cannot be extracted without decompressing their containing chunk. The worker retains only message metadata, filters visible ticks to the requested interval, and preserves complete per-channel extents for chunk outlines. The worker retains the full chunk metadata backing its latest completed range (up to 2,000,000 marks) plus an LRU of at most 32 other chunks / 200,000 marks; payload bytes are discarded. Stale viewport results are ignored, and superseded work stops at chunk boundaries.

After 500 ms without a pending navigation request, the worker expands the completed range, retaining prior chunks so expansion does not reread them. Expansion continues toward the full recording within the same metadata limit. Completed snapshots quietly expand the canvas cache, so fully preloaded ranges need no further loading indicator or disk/network reads. Foreground requests preempt background work at chunk boundaries; stale results are ignored. The footer reports background loading, completion, or a pause. Memory limits or errors in future ranges pause preloading without discarding the current view; foreground navigation can still load a smaller range.

Loose messages require reading each message's fixed header to discover timestamps and channel membership; their payloads are skipped. A channel defined only inside an earlier chunk in an unindexed file may require decoding earlier chunks to discover that definition. Summary channel definitions avoid this fallback.

Individual reads/decompressed chunks are capped at 512 MiB; catalogs and decoded viewport workloads at 2,000,000 records/messages. A viewport that exceeds the limit reports an error and can be narrowed by zooming in. These ceilings are not a guarantee that every browser has sufficient memory. This tool inspects structure and validates decoded chunk CRCs; it is not a complete file integrity validator.

## Code organization

`InspectorApp.tsx`, `SelectionDetails.tsx`, and `ViewportNavigator.tsx` render declarative React UI. `McapInspector.tsx` provides style isolation, and `createInspector.ts` is a thin mounting adapter. `timeline.ts` owns canvas interactions, `source.ts` performs seekable catalog/window reads, and `InspectorLoader.ts` owns the worker bridge and stale-result handling. `WindowScheduler.ts` prioritizes navigation over idle preloading. `parse.ts` retains the independent full-scan parser for callers/tests that need a complete metadata snapshot.

Run `yarn workspace website test`, `yarn workspace website lint:ci`, and `yarn workspace website build` from the repository root.
