# @mcap/core

[MCAP](https://mcap.dev/) is a modular container format and logging library for pub/sub messages with arbitrary message serialization. It is primarily intended for use in robotics applications, and works well under various workloads, resource constraints, and durability requirements.

The `@mcap/core` package provides low-level readers and writers for the MCAP format in TypeScript.

## Examples

Examples of how to use the `@mcap/core` APIs can be found in the [TypeScript examples folder](https://github.com/foxglove/mcap/tree/main/typescript/examples) in the MCAP repo.

### Emit chunks without expanding them

Set `emitChunks: true` on `McapStreamReader` to return only records from the outer stream, in file order. This corresponds to Python’s `emit_chunks`, Go’s `EmitChunks`, and Rust’s `with_emit_chunks` options. A `Chunk` retains its original compressed `records` payload; its following `MessageIndex` records are returned separately. Consumers decide whether to group, decompress, or inspect chunks. No decompression handlers are required, and unknown compression algorithms are accepted. Even uncompressed chunks remain opaque.

```ts
import { McapStreamReader } from "@mcap/core";

const reader = new McapStreamReader({ emitChunks: true });
// input is an AsyncIterable<Uint8Array>, such as a Node.js readable stream.
for await (const bytes of input) {
  reader.append(bytes);
  for (let record; (record = reader.nextRecord()) != undefined; ) {
    if (record.type === "Chunk") {
      console.log(record.compression, record.records);
    } else if (record.type === "MessageIndex") {
      console.log(record.channelId, record.records);
    }
  }
}
if (!reader.done()) {
  throw new Error("Incomplete MCAP stream");
}
```

`append()` copies its input, so input buffers can be reused after it returns. Byte arrays in outer records are owned copies in all modes, so subsequent appends do not overwrite retained payloads. With `emitChunks: true`, payloads may also be modified without affecting subsequent reads.

The chunk options have the following behavior:

| Options               | Behavior                                         |
| --------------------- | ------------------------------------------------ |
| Default               | Expand chunks and emit their contents            |
| `includeChunks: true` | Emit chunks, then expand and emit their contents |
| `emitChunks: true`    | Emit only outer records; never expand chunks     |

`emitChunks` takes precedence when both options are true. Decompression handlers are ignored in this mode.

Magic, record parsing, duplicate-header, and footer/trailing-byte checks apply in all modes. Nonzero attachment CRCs are checked by default (`validateCrcs: false` disables this). With `emitChunks: true`, the reader does not validate chunk contents or uncompressed size/CRC: checking a compressed chunk's uncompressed CRC requires decompression. It allows messages without a prior channel definition, since channel definitions may be inside chunks. Conflicting outer channel definitions are still rejected. Consumers are responsible for validating any chunks they expand. In the default mode, the reader expands chunks and validates chunk CRCs and message/channel relationships. Data-section and summary CRCs are not validated in either mode.

An undefined `nextRecord()` result may mean more input is needed. Check `done()` at end of input even when `bytesRemaining()` is zero. The `noMagicPrefix` option permits starting at a record boundary without the initial magic, but `done()` still requires a footer and trailing magic.

## License

`@mcap/core` is licensed under the [MIT License](https://opensource.org/licenses/MIT).

## Stay in touch

Join our [Discord community](https://foxglove.dev/chat) to ask questions, share feedback, and stay up to date on what our team is working on.
