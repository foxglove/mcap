# @mcap/support

[MCAP](https://mcap.dev/) is a modular container format and logging library for pub/sub messages with arbitrary message serialization. It is primarily intended for use in robotics applications, and works well under various workloads, resource constraints, and durability requirements.

The `@mcap/support` package provides utilities for working with MCAP files that use [well-known compression formats](https://mcap.dev/specification/appendix.html), from Node.js and browsers.

## Usage examples

### Reading MCAP files in a browser

```ts
import { loadDecompressHandlers } from "@mcap/support";
import { BlobReadable } from "@mcap/browser";
import { McapIndexedReader } from "@mcap/core";

/**
 * For <input type="file"/>, listen for "change" events.
 *
 * For drag & drop, listen for "drop" events. (Note that you must also listen for "dragover" events
 * and call event.preventDefault(), to enable listening for drop events.)
 */
async function onInputOrDrop(event: Event) {
  let file: File | undefined;
  if (event instanceof DragEvent) {
    file = event.dataTransfer?.files[0];
    event.preventDefault();
  } else if (event.target instanceof HTMLInputElement) {
    file = event.target.files?.[0];
  } else {
    throw new Error(`Unexpected event type: ${event.type}`);
  }
  if (!file) {
    throw new Error("No file selected");
  }
  const decompressHandlers = await loadDecompressHandlers();
  const reader = await McapIndexedReader.Initialize({
    readable: new BlobReadable(file),
    decompressHandlers,
  });
}
```

### Reading MCAP files in Node.js

```ts
import { loadDecompressHandlers } from "@mcap/support";
import { FileHandleReadable } from "@mcap/nodejs";
import { McapIndexedReader } from "@mcap/core";
import { open } from "fs/promises";

const decompressHandlers = await loadDecompressHandlers();
const fileHandle = await open("file.mcap", "r");
const reader = await McapIndexedReader.Initialize({
  readable: new FileHandleReadable(fileHandle),
  decompressHandlers,
});
```

## License

`@mcap/support` is licensed under the [MIT License](https://opensource.org/licenses/MIT).

## Stay in touch

Join our [Discord community](https://foxglove.dev/chat) to ask questions, share feedback, and stay up to date on what our team is working on.
