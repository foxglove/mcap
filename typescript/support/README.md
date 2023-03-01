# @mcap/support

The `@mcap/support` package provides high-level helper classes and functions for reading and writing MCAP files and deserializing well-known MCAP message formats.

### @mcap/support API

- `loadDecompressHandlers(): Promise<DecompressHandlers>` - Loads all decompression handlers required to initialize an MCAP reader.
- `parseChannel(channel: Channel): ParsedChannel` - Process a channel/schema and extract information that can be used to deserialize messages on the channel, and schemas into @foxglove/message-definition format.
- `parseFlatbufferSchema(schemaName: string, schemaArray: Uint8Array): { datatypes: MessageDefinitionMap; deserializer: (buffer: ArrayBufferView) => unknown }` - Parse a flatbuffer schema into `@foxglove/message-definition` format and a deserializer function.

### @mcap/support/nodejs API

- `FileHandleReadable` - A `IReadable` stream that reads from a node.js `FileHandle` object.
- `FileHandleWritable` - A `IWritable` stream that writes to a node.js `FileHandle` object.

## License

[MIT License](/LICENSE). Contributors are required to accept the [Contributor License Agreement](https://github.com/foxglove/cla).
