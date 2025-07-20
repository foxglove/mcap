# @mcap/support

[MCAP](https://mcap.dev/) is a modular container format and logging library for pub/sub messages with arbitrary message serialization. It is primarily intended for use in robotics applications, and works well under various workloads, resource constraints, and durability requirements.

The `@mcap/support` package provides utilities for working with MCAP files that use [well-known compression formats](https://mcap.dev/specification/appendix.html), from Node.js and browsers.

## Usage examples

### Reading MCAP files in a browser

```ts
import { loadDecompressHandlers } from "@mcap/support";
import { BlobReadable } from "@mcap/browser";
import { McapIndexedReader } from "@mcap/core";

async function onInputOrDrop(event: InputEvent | DragEvent) {
  let file: File | undefined;

  if ("dataTransfer" in event && event.dataTransfer) {
    // DragEvent
    file = event.dataTransfer.files[0];
  } else if ("target" in event && event.target) {
    // InputEvent
    file = (event.target as HTMLInputElement).files?.[0];
  }

  if (!file) {
    console.error("No file found");
    return;
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
