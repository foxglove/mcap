# @mcap/nodejs

[MCAP](https://github.com/foxglove/mcap) is a modular container format and logging library for pub/sub messages with arbitrary message serialization. It is primarily intended for use in robotics applications, and works well under various workloads, resource constraints, and durability requirements.

The `@mcap/nodejs` package provides utilities for working with MCAP files from Node.js.

## Usage examples

### Reading MCAP files

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

### Writing MCAP files

```ts
import zstd from "@foxglove/wasm-zstd";
import { FileHandleWritable } from "@mcap/support/nodejs";
import { McapWriter } from "@mcap/core";
import { open } from "fs/promises";

await zstd.isLoaded;
const fileHandle = await open("file.mcap", "wx");
const writer = new McapWriter({
  writable: new FileHandleWritable(fileHandle),
  compressChunk: (data) => ({
    compression: "zstd",
    compressedData: zstd.compress(data),
  }),
});
```

## License

`@mcap/nodejs` is licensed under the [MIT License](https://opensource.org/licenses/MIT).

## Stay in touch

Join our [Slack channel](https://foxglove.dev/join-slack) to ask questions, share feedback, and stay up to date on what our team is working on.
