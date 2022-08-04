---
title: "Messages, channels, & schemas"
slug: "core-concepts"
hidden: false
createdAt: "2022-07-21T19:28:47.865Z"
updatedAt: "2022-07-26T22:15:53.477Z"
---
MCAP organizes its data via messages, channels, and schemas.

## Message
The unit of communication between nodes in the pub/sub system.

## Channel
A stream of messages which have the same type, or schema. Often corresponds to a connection between a publisher and a subscriber.

## Schema
A description of the structure and contents of messages on a channel, e.g. a Protobuf FileDescriptorSet or JSON Schema.

> The [@foxglove/schemas](https://github.com/foxglove/schemas) repo provides pre-defined schema definitions for [Foxglove Studio](https://foxglove.dev/studio) visualizations. Write messages that adhere to these schemas to an MCAP file, then visualize and debug this data using Studio's [panels](https://foxglove.dev/docs/studio/panels/introduciton).