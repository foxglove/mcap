---
description: Read, write, and visualize MCAP files containing FlatBuffers data.
---

# FlatBuffers

## Read and write MCAP

If you're starting from scratch, you can write code that allows **writes your FlatBuffers data to MCAP files** and subsequently **reads your FlatBuffers data from your MCAP files**.

See also the [official FlatBuffers docs](https://google.github.io/flatbuffers/) to learn more about reading and writing FlatBuffers.

### Examples

- [TypeScript](https://github.com/foxglove/mcap/tree/main/typescript/examples/flatbufferswriter) - [Writing Foxglove schemas to MCAP file](https://github.com/foxglove/mcap/blob/main/typescript/examples/flatbufferswriter/scripts/main.ts)

## Visualize MCAP

To play back MCAP files containing FlatBuffers-encoded data in [Foxglove](https://app.foxglove.dev/), be sure the MCAP Channel's `message_encoding` field is set to `flatbuffer`. Note that in order for Foxglove to read FlatBuffer data from channels in MCAP files, the [MCAP Schema record](https://mcap.dev/specification/index.html#schema-op0x03) should contain the binary flatbuffer schema (`.bfbs`) content in its `data` field. They can be compiled using `flatc -b --schema [...args]`, using the `.fbs` files as input. For more info, see the [MCAP specification for FlatBuffers encoding](https://mcap.dev/specification/appendix.html#flatbuffer_1).
