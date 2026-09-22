# @mcap/core

[MCAP](https://mcap.dev/) is a modular container format and logging library for pub/sub messages with arbitrary message serialization. It is primarily intended for use in robotics applications, and works well under various workloads, resource constraints, and durability requirements.

The `@mcap/core` package provides low-level readers and writers for the MCAP format in TypeScript.

## Examples

Examples of how to use the `@mcap/core` APIs can be found in the [TypeScript examples folder](https://github.com/foxglove/mcap/tree/main/typescript/examples) in the MCAP repo.

### Reading outer records without expanding chunks

`McapRawStreamReader` uses the same incremental API as `McapStreamReader`, but returns only records from the outer stream, in file order. A `Chunk` retains its original compressed `records` payload; its following `MessageIndex` records are returned separately. Consumers decide whether to group, decompress, or inspect chunks. No decompression handlers are required, and unknown compression algorithms are accepted. Even uncompressed chunks remain opaque.

```ts
import { McapRawStreamReader } from "@mcap/core";

const reader = new McapRawStreamReader();
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

`append()` copies its input, and returned byte arrays are owned copies. Input buffers can be reused after `append()` returns, and returned payloads can be retained or modified across further reads and appends.

The raw reader preserves the stream reader's magic, record parsing, duplicate-header, and footer/trailing-byte checks. Nonzero attachment CRCs are checked by default (`validateCrcs: false` disables this). It does not validate chunk contents or uncompressed size/CRC: checking a compressed chunk's uncompressed CRC requires decompression. It also does not validate message/channel relationships, since channel definitions may be inside chunks, or data-section and summary CRCs. `McapStreamReader` adds chunk expansion, chunk CRC checks, and message/channel validation; its `includeChunks` option still expands chunk contents.

An undefined `nextRecord()` result may mean more input is needed. Check `done()` at end of input even when `bytesRemaining()` is zero. The `noMagicPrefix` option permits starting at a record boundary without the initial magic, but `done()` still requires a footer and trailing magic.

## License

`@mcap/core` is licensed under the [MIT License](https://opensource.org/licenses/MIT).

## Stay in touch

Join our [Discord community](https://foxglove.dev/chat) to ask questions, share feedback, and stay up to date on what our team is working on.
