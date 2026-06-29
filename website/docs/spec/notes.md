---
sidebar_position: 3
---

# Implementation Notes

The following notes may be useful for users of the MCAP format, including implementers of readers and writers.

MCAP is intended to support efficient, indexed reading of messages and generation of summary data in both local and remote contexts. "Seeking" should be imagined to incur either a disk seek or an HTTP range request to an object store—the latter being significantly more costly. In both random access and summarization, features may be unsupported due to choices taken by the writer of the file. For instance, statistics may not include channel message counts, or there may be no message index present. If the index data section is empty, the `index_offset` in the file footer will be set to zero.

## Scanning for records on specific topics within an interval

The index is designed to support fast local and remote seek/filter operations with minimal seeking or range request overhead. The operation of the index for message reading is as follows:

1. Client queries for all messages on topics /a, /b, /c between t0 and t1
2. Reader reads the fixed-length footer off the end of the file
3. Reader parses the index_offset from the footer, and starts reading from that offset to the end of the file. During this read it will encounter the following in order:
   - A run of Channel records, one per channel in the file
   - A run of Message Group Index records, one per chunk in the file
   - The attachment index records
   - The statistics record

The reader in this case will stop after the chunk index records.

4. Using the channel records at the start of the read, the reader converts topic names to channel IDs.
5. Using the chunk index records, the reader locates the chunks that must be read, based on the requested start times, channel IDs, and end times. These chunks will be a contiguous run.
6. Readers may access the message data in at least two ways,
   - “full scan”: Seek from the chunk index to the start of the chunk using chunk_offset. Read/decompress the entire chunk, discarding messages not on the requested channels. Skip through the index data and into the next chunk if it is targeted too.
   - “index scan”: Consult the message_index_offsets field in the chunk index record, and use it to locate specific message indexes after the chunk for the channels of interest. These message indexes can be used to obtain a list of offsets, which the reader can seek to and extract messages from.

Which of these options is preferable will tend to depend on the proportion of topics in use, as well as potentially whether the storage system is local or remote.

## Listing and accessing attachments

The format provides the ability to list attachments contained within the file, and quickly extract them from the file contents. To list/select attachments in the file:

1. Read the fixed-length footer and seek to the start of the index data section.
2. Scan forward until encountering the attachment index, then read attachment index records until encountering a record that is not an attachment index.
3. The records covered in the previous read will include attachment names, types, sizes, and timestamps. These can be used to fill out a list of attachments for selection.
4. To select an attachment from the file, seek to the associated offset in the file and unpack the file content from the attachment record.

## Accessing summary statistics

The format provides for fast local or remote access to summary information in the same style as "rosbag info", with the intent of functional parity with rosbag info. For reference, here is an example of the rosbag info output:

```
path:         demo.bag
version:      2.0
duration:     7.8s
start:        Mar 21 2017 19:26:20.10 (1490149580.10)
end:          Mar 21 2017 19:26:27.88 (1490149587.88)
size:         67.1 MB
messages:     1606
compression:        lz4 [79/79 chunks; 56.23%]
uncompressed:       119.1 MB @ 15.3 MB/s
compressed:    67.0 MB @  8.6 MB/s (56.23%)
types:        diagnostic_msgs/DiagnosticArray [60810da900de1dd6ddd437c3503511da]
              radar_driver/RadarTracks        [6a2de2f790cb8bb0e149d45d297462f8]
              sensor_msgs/CompressedImage     [8f7a12909da2c9d3332d540a0977563f]
              sensor_msgs/PointCloud2         [1158d486dd51d683ce2f1be655c3c181]
              sensor_msgs/Range               [c005c34273dc426c67a020a87bc24148]
              tf2_msgs/TFMessage              [94810edda583a504dfda3829e70d7eec]
topics:        /diagnostics               52 msgs    : diagnostic_msgs/DiagnosticArray
              /image_color/compressed       234 msgs    : sensor_msgs/CompressedImage
              /radar/points             156 msgs    : sensor_msgs/PointCloud2
              /radar/range              156 msgs    : sensor_msgs/Range
              /radar/tracks             156 msgs    : radar_driver/RadarTracks
              /tf                       774 msgs    : tf2_msgs/TFMessage
              /velodyne_points           78 msgs    : sensor_msgs/PointCloud2
```

The reader will recover this data from the index as follows:

1. Read the fixed length footer and seek to the index_offset.
2. Read the run of channel records that follow to get topic names, types, and MD5 data (which in case of ROS1 will be in the user data section), as well as channel IDs to interpret the chunk index records.
3. After the channel are the chunk index records, if the file is chunked. From each chunk index record extract the compression algorithm and compressed/uncompressed size. From these the reader can compute the compression statistics shown in the rosbag info summary. For unchunked files this field is omitted.
4. The MCAP version of “rosbag info” will display information about included attachments as well. After reading the chunk index records, the attachment index records will be scanned and incorporated into the summary.
5. Finally, the statistics record is used to compute the start, end, total, and per-channel message counts. The per-channel message counts must be grouped/summed over topics for display.

The only difference between the chunked and unchunked versions of this output will be the chunk compression statistics (“compressed”, “uncompressed”, “compression”), which will be omitted in the case of unchunked files. The summary should be very fast to generate in either local or remote contexts, requiring no seeking around the file to visit chunks.

The above is not meant to prescribe a summary formatting, but to demonstrate that parity with the rosbag summary is supported by MCAP. There are other details we may consider including, like references to per-channel encryption or compression if these features get uptake. We could also enable more interaction with the channel records, such as quickly obtaining schemas from the file for particular topics.

## Message fields

Every [Message](./index.md#message-op0x05) record carries exactly two timestamps (`log_time` and `publish_time`) and no other per-message attributes. _Message fields_ allow a message to carry an arbitrary number of additional named, typed values without modifying the (frozen) Message record. This covers two use cases with one mechanism:

- **Additional timestamps** — e.g. an indexable, seekable `publish_time`, or a `sensor_time`.
- **Per-message metadata** — arbitrary values that vary per message and are carried out of band from the message payload (for example, attributes from a pub/sub middleware), without inflating the channel count or re-wrapping an already-encoded payload.

The feature is fully backward compatible: it is built entirely from records with new opcodes, which existing readers skip, so existing readers and writers are unaffected and only readers that wish to access the values need updating. Messages that carry no fields incur no overhead.

The feature is composed of four records:

- [Field](./index.md#field-op0x10) declares, file-globally, a `uint16` ID, a `name`, an `encoding` (logical type), a `length` (physical wire width), and flags (including `indexed`). It is written like a Schema or Channel record (before first use, and optionally duplicated in the summary).
- [Message Fields](./index.md#message-fields-op0x11) carries the `(field_id, value)` pairs for one message and is written **immediately after** that message.
- [Field Index](./index.md#field-index-op0x12) and [Field Chunk Index](./index.md#field-chunk-index-op0x13) mirror the standard Message Index and Chunk Index, but key on an indexed field's value instead of `log_time`, so a reader can seek and prune by it.

The `encoding`/`length` split means a value is parsed using only its `length` (so unknown encodings remain skippable) and interpreted using its `encoding`. See [field encodings](./registry.md#field-encodings).

### Reading fields during a linear scan

While iterating records (in the data section or within a decompressed chunk), a reader pairs each Message with the Message Fields record that immediately follows it:

1. Read a Message record.
2. Peek at the next record. If it is a Message Fields record whose `channel_id` matches, attach its values to the message just read; otherwise the message has no fields.
3. Resolve each `field_id` to its name/encoding/length using the Field records seen so far, and parse each value by its `length`.

Readers that do not understand opcode `0x11` skip it and observe only `log_time` and `publish_time`.

### Seeking by a field value

To seek to messages by an indexed field `F` (rather than `log_time`):

1. Read the summary section and collect the Field records to map the desired name to its ID, plus the Field Chunk Index records for that ID.
2. Use the `min_value`/`max_value` of each Field Chunk Index record to select candidate chunks. Note that, unlike `log_time`, a field is not guaranteed to be monotonic, so candidate chunk ranges may overlap and a query interval may match more chunks than the equivalent `log_time` query would.
3. For each candidate chunk, either full-scan the chunk, or consult its Field Index records (located via `message_index_offsets`) to obtain message offsets directly, seek to each Message, and read the trailing Message Fields record.

### Writing considerations

- Because association is positional, a Message Fields record MUST be written immediately after its Message, with no records in between, in the same record stream. Tools that copy records through verbatim preserve this pairing; tools that re-emit messages via a message-level API must be updated to carry the fields records along.
- Declaring fields per file keeps `merge` operations cheap: messages from channels that do not use a given field simply omit it rather than padding with default values.
- Attributes that are constant for all messages on a channel should be stored in the channel's `metadata` map rather than repeated per message.
