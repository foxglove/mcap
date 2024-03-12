---
sidebar_position: 2
---

# Concepts

MCAP organizes its data via messages, channels, and schemas.

## Message

The unit of communication between nodes in the pub/sub system.

## Channel

A stream of messages which have the same type, or schema. Often corresponds to a connection between a publisher and a subscriber.

## Schema

A description of the structure and contents of messages on a channel, e.g. a [Protobuf Schema](https://protobuf.dev/programming-guides/proto3/) or [JSON Schema](https://json-schema.org/).

:::info
The [foxglove/schemas](https://github.com/foxglove/schemas) repo provides [pre-defined schemas](https://docs.foxglove.dev/docs/visualization/message-schemas/introduction) for Foxglove visualizations.
:::
