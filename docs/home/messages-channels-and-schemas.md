# Messages, Channels and Schemas

MCAP organizes its data via messages, channels, and schemas.

## Message

The unit of communication between nodes in the pub/sub system.

## Channel

A stream of messages which have the same type, or schema. Often corresponds to a connection between a publisher and a subscriber.

## Schema

A description of the structure and contents of messages on a channel, e.g. a Protobuf [FileDescriptorSet](https://developers.google.com/protocol-buffers/docs/reference/java/com/google/protobuf/DescriptorProtos.FileDescriptorSet) or JSON Schema.

> The [@foxglove/schemas](https://github.com/foxglove/schemas) repo provides pre-defined schema definitions for [Foxglove Studio](https://foxglove.dev/studio) visualizations. Write messages that adhere to these schemas to an MCAP file, then visualize and debug this data using Studio's [panels](https://foxglove.dev/docs/studio/panels/introduciton).
