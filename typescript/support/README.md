# @mcap/support

[MCAP](https://mcap.dev/) is a modular container format and logging library for pub/sub messages with arbitrary message serialization. It is primarily intended for use in robotics applications, and works well under various workloads, resource constraints, and durability requirements.

The `@mcap/support` package provides utilities for working with MCAP files that use [well-known compression formats and encodings](https://mcap.dev/specification/appendix.html), from Node.js and browsers.

## Usage examples

### Reading MCAP files in a browser

TODO

### Reading MCAP files in Node.js

```ts
import { loadDecompressHandlers } from "@mcap/support";
import { FileHandleReadable } from "@mcap/support/nodejs";
const decompressHandlers = await loadDecompressHandlers();

const fileHandle = await open("file.mcap", "r");

const reader = await McapIndexedReader.Initialize({
  readable: new FileHandleReadable(fileHandle),
  decompressHandlers,
});
```

### Writing MCAP files with Node.js

```ts
import zstd from "@foxglove/wasm-zstd";
import { FileHandleWritable } from "@mcap/support/nodejs";
import { open } from "fs/promises";

const fileHandle = await open("file.mcap", "w");

await zstd.isLoaded;
const writer = new McapWriter({
  writable: new FileHandleWritable(fileHandle),
  compressChunk: (data) => ({
    compression: "zstd",
    compressedData: zstd.compress(data),
  }),
});
```

## License

`@mcap/support` is licensed under the [MIT License](https://opensource.org/licenses/MIT).

## Stay in touch

Join our [Slack channel](https://foxglove.dev/slack) to ask questions, share feedback, and stay up to date on what our team is working on.
