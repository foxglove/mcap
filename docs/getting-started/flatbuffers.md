---
description: Read, write, and visualize MCAP files containing Flatbuffer data.
---

# Flatbuffers

## Read and write MCAP

If you're starting from scratch, you can write code that allows you to **write your Flatbuffer data to MCAP files** and subsequently **read your Flatbuffer data from your MCAP files**.

### Examples

- [TypeScript](https://github.com/foxglove/mcap/tree/main/typescript/examples/flatbufferswriter) - [Writing Foxglove schemas to MCAP file](https://github.com/foxglove/mcap/blob/main/typescript/examples/flatbufferswriter/scripts/main.ts)

## Visualize MCAP

[Foxglove Studio](https://foxglove.dev/studio) supports playing back local and remote MCAP files containing Flatbuffer data using the `flatbuffer` encoding. Note that in order for Foxglove Studio read flatbuffer data from channels in MCAP files, the binary flatbuffer schema (`.bfbs`) must be passed to `data` when calling `registerSchema` to the channel in order to read those messages. They can be compiled using `flatc -b --schema [...args]`, using the `.fbs` files as input.
