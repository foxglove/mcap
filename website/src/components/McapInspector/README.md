# MCAP inspector

A reusable React component for exploring MCAP message/chunk structure. The site mounts it at `/inspect`. It is not a published npm package.

```tsx
import { McapInspector } from "./components/McapInspector/index.ts";

<McapInspector
  file={selectedFile} // Optional; the component also has a picker and drop zone.
  height={560}
  onLoad={(recording) => console.log(recording.messageCount)}
  onError={(error) => console.error(error)}
/>;
```

Each instance owns its worker, canvas, controls, and view state. Styles are isolated in a shadow root; file drops are scoped to the component. React unmount terminates the worker, aborts listeners, disconnects the resize observer, and cancels pending drawing. Multiple inspectors can share a page. Importing the React component does not access browser globals, so it can be rendered by Docusaurus on the server.

`file` is optional: passing a new `File` starts a load, while omitting it leaves file selection to the user. `onLoad` fires once a file is fully parsed. `onError` reports loading failures; the previous recording is retained after a failure or cancellation. Callback changes do not remount the inspector.

For a framework-independent host, use `createInspector(element, { createWorker, onLoad, onError })`. It returns `loadFile(file)`, `setRecording(recording)`, `focusChunk(id)`, `exitChunk()`, and `destroy()`. Supply a worker factory using your bundler's worker support:

```ts
const inspector = createInspector(element, {
  createWorker: () =>
    new Worker(
      new URL("./components/McapInspector/loader.worker.ts", import.meta.url),
      {
        type: "module",
      },
    ),
});
inspector.loadFile(file);
// When the containing view is removed:
inspector.destroy();
```

The host must be empty and exclusively owned by the controller. Call `destroy()` before remounting into the same host. `setRecording()` accepts the metadata model directly, allowing an application to supply its own loading pipeline. Supplied channel message arrays must be sorted by log time, `time` must be seconds relative to `recording.startTime`, and chunk ranges must use that same origin. Chunk IDs must be unique; messages refer to those IDs. Worker creation lives in the React adapter, not in the controller, so alternate hosts can provide a compatible worker factory.

## Navigation

- **Channel** groups all messages by channel ID and topic; outlines show physical chunk membership.
- **Chunk** gives physical chunks separate rows, ordered by file offset. Click a left label to expand channel rows, or use **Expand all** / **Collapse all**.
- **Double-click a chunk** in either view to show only its channels/messages and fit its time range. **Full recording** restores the previous grouping, time window, and vertical position.
- Drag to pan; vertical scrolling moves through rows. Shift + scroll pans time; Ctrl/Command + scroll zooms around the pointer. The bottom slider pans time.
- With the canvas focused, arrows pan/scroll, + and − zoom, Home fits the view, and Escape clears selection. Controls and channel inspection are also available through native buttons/selects.
- Filtering matches channel IDs and topic names. Physical chunk bars retain their complete time ranges even when the channel list is filtered. Colors repeat, so inspect the chunk ID to disambiguate.

Message marks use log time. Exact nanosecond log/publish timestamps are retained as `bigint`; relative canvas coordinates subtract the recording's origin first. Unchunked messages are distinct and remain available for inspection.

## File processing and bounds

Files stay in the browser. A dedicated worker uses the MCAP stream reader, with physical top-level records fed separately so timestamp overlap cannot change chunk membership. Indexed and unindexed files are supported; a valid footer/trailing magic is required. `@mcap/support` provides the decompression handlers. No message payload or schema bytes are retained after scanning.

Reads use a 4 MiB window or a single larger record. Decompression still needs a complete chunk. The in-memory _metadata_ index is explicitly capped at 2,000,000 messages; individual physical records and declared decompressed chunks are capped at 512 MiB. These are safety ceilings, not a guarantee that every browser can open files of that size. Limits reject the file clearly rather than silently sampling or truncating it. The previous recording is kept while a replacement loads. This structural inspector does not decode message payloads or replace a complete MCAP integrity validator.

`timeline.ts` contains only canvas/view interactions; `layout.ts` partitions metadata by physical membership; `parse.ts` is independent of the DOM. The parser and grouping regression tests include overlapping, compressed, mixed, and unchunked data.
