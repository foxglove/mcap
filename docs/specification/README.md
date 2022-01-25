# MCAP File Format Specification

[tlv wiki]: https://en.wikipedia.org/wiki/Type-length-value
[profiles]: ./profiles
[compression formats]: ./compression/supported-compression-formats.md
[explanatory notes]: ./notes/explanatory-notes.md
[diagram unchunked]: ./diagrams/unchunked.png
[diagram chunked]: ./diagrams/chunked.png
[feature explanations]: ./notes/explanatory-notes.md#feature-explanations

> Status: DRAFT

## Overview

MCAP is a container file format for recording _messages_ on _channels_ with
support for arbitrary message serialization formats.


## Format Description

An MCAP file is structured as a sequence of magic bytes, followed by a series of records, followed a sequence of magic bytes:

    <MAGIC>[<record type><record length><record>...]<MAGIC>

Record type is a single byte opcode, and record length is a little endian uint64 value.

The magic bytes are:

    0x89, M, C, A, P, 0x30, \r, \n

> Note: The version byte (ASCII zero 0x30) following "MCAP" will be updated to 1 (0x31) upon ratification of this specification. Until then, backward compatibility is not guaranteed.

## Serialization and Notation

The section below uses the following data types and serialization choices. In all cases integers are serialized little endian:

- **Timestamp**: uint64 nanoseconds since a user-understood epoch (i.e unix epoch, robot boot time, etc)
- **String**: a uint32-prefixed UTF8 string
- **KeyValues<T1, T2>**: A uint32 length-prefixed association of key-value pairs

```
<length><T1 (key)><T2 (value)><T1 (key)><T2 (value)>
```

- **Array<T>**: A uint32 length-prefixed array.

```
<length><T><T><T>
```

An empty Array consists of a zero-value length prefix.

- **Bytes**: refers to an array of bytes, without a length prefix. If a length prefix is required a designation like "uint32 length-prefixed bytes" will be used.

## Records


MCAP files may contain a variety of record types. Record types are single-byte **opcodes**. Record opcodes in the range 0x01-0x7F are reserved for future MCAP format usage. 0x80-0xFF are reserved for application extensions and user proposals.

The first record in every file must be a Header (op=0x01) and the last record must be a Footer (op=0x02).



### Header (op=0x01)
The first record in every MCAP file is a header.

### Footer (op=0x02)
The last record in every MCAP file is a footer.

### Channel Info (op=0x03)

A Channel Info record defines an encoded stream of messages on a topic.

Channel Info records are uniquely identified within a file by their channel ID.
A Channel Info record must occur at least once in the file prior to any message
referring to its channel ID.

### Message (op=0x04)

A message record encodes a single timestamped message on a channel.

The message encoding must match that of the channel info record corresponding to
the message's channel ID.

### Chunk (op=0x05)

A Chunk contains a batch of channel info and message records. The batch of
records contained in a chunk may be compressed or uncompressed.

All messages in the chunk must reference channel infos recorded prior in
the file (or the current chunk).

### Message Index (op=0x06)

A Message Index record maps timestamps to message records within a chunk.

A sequence of Message Index records occurs immediately after each chunk. Exactly
one Message Index record must exist in the sequence for every channel on which a
message occurs inside the chunk.

### Chunk Index (op=0x07)
A Chunk Index record contains the location of a Chunk record and its
associated Message Index records.

A Chunk Index record exists for every Chunk in the file.

### Attachment (op=0x08)

Attachment records contain artifacts such as text, core dumps, or calibration data.

Attachment records must not appear within a chunk.

### Attachment Index (op=0x09)

An Attachment Index record contains the location of an attachment in the file. An Attachment Index record exists for every Attachment record in the file.

### Statistics (op=0x0A)

A Statistics record contains summary information about the recorded data. It is the last record in the file before the footer.


## Serialization
### Data types
### Records



The diagrams below show two possible variants - a file that is chunked and indexed, i.e making full use of the features, and one that is unchunked but contains statistics.

![Chunked][diagram chunked]

![Unchunked][diagram unchunked]
### Header (op=0x01)

The first record in every MCAP file is a header.

| Bytes | Name | Type | Description |
| --- | --- | --- | --- |
| 4 + N | profile | String | The profile to use for interpretation of channel info user data. If the value matches one of the [supported profiles][profiles], the channel info user data section should be structured to match the description in the corresponding profile. This field may also be supplied empty, or containing a framework that is not one of those recognized. |
| N | library | String | freeform string for writer to specify its name, version, or other information for use in debugging |
| N | metadata | KeyValues<string, string> | Example keys: robot_id, git_sha, timezone, run_id. |

### Footer (op=0x02)

The last record in every MCAP file is a footer.

| Bytes | Name | Type | Description |
| --- | --- | --- | --- |
| 8 | index_offset | uint64 | Pointer to start of index section. If there are no records in the index section, this should be zero. |
| 4 | index_crc | uint32 | CRC32 checksum of all data from index_offset through the byte immediately preceding this CRC. A value of zero indicates that CRC validation should not be performed. |

A file without a footer is **corrupt**, indicating the writer process encountered an unclean shutdown. It may be possible to recover data from a corrupt file.

### Channel Info (op=0x03)

Identifies a stream of messages on a particular topic and includes information about how the messages should be decoded by readers. A channel info record must occur in the file prior to any message that references its Channel ID. Channel IDs must uniquely identify a channel across the entire file. If message indexing is in use, the Channel Info section of the index data section must also be in use.

| Bytes | Name | Type | Description | Example |
| --- | --- | --- | --- | --- |
| 2 | id | uint16 | Channel ID 1 | 1 |
| 4 + N | topic_name | String | Topic | /diagnostics |
| 4 + N | encoding | String | Message Encoding | cdr, cbor, ros1, protobuf, etc. |
| 4 + N | schema_name | String | Schema Name | std_msgs/Header |
| 4 + N | schema | uint32 length-prefixed bytes | Schema |  |
| N | user_data | KeyValues<string, string> | Metadata about this channel | used to encode protocol-specific details like callerid, latching, QoS profiles... Refer to [supported profiles][profiles]. |

### Message (op=0x04)

A message record encodes a single timestamped message on a particular channel. In a given file, messages must appear either inside Chunks, or outside Chunks. A file may not contain both chunked and unchunked messages.

| Bytes | Name | Type | Description |
| --- | --- | --- | --- |
| 2 | channel_id | uint16 | Channel ID |
| 4 | sequence | uint32 | Optional message counter assigned by publisher. If not assigned by publisher, must be recorded by the recorder. |
| 8 | publish_time | Timestamp | Time at which the message was published. If not available, must be set to the record time. |
| 8 | record_time | Timestamp | Time at which the message was recorded by the recorder process. |
| N | message_data | Bytes | Message data, to be decoded according to the schema of the channel. |

### Chunk (op=0x05)

A Chunk is a collection of compressed channel info and message records. If message indexing is in use, Chunks are required.

| Bytes | Name | Type | Description | Example |
| --- | --- | --- | --- | --- |
| 8 | uncompressed_size | uint64 | Uncompressed size of of the "records" section. |
| 4 | uncompressed_crc | uint32 | CRC32 checksum of uncompressed "records" section. A value of zero indicates that CRC validation should not be performed. |
| 4 + N | compression | String | compression algorithm | lz4, zstd, "". A zero-length string indicates no compression. Refer to [supported compression formats][compression formats]. |
| N | records | Bytes | Concatenated records, compressed with the algorithm in the "compression" field. |

### Message Index (op=0x06)

The Message Index record maps timestamps to message offsets. If message indexing is in use, following each chunk, a message index record is written for each channel in the chunk preceding. All message index records for a chunk must immediately follow the chunk in a contiguous run of records.

| Bytes | Name | Type | Description |
| --- | --- | --- | --- |
| 2 | channel_id | uint16 | Channel ID. |
| N | records | Array<{ Timestamp, uint64 }> | Array of record_time and offset for each record. Offset is relative to the start of the uncompressed chunk data. |

### Chunk Index (op=0x07)

The Chunk Index records form a coarse index of timestamps to chunk offsets, along with the locations of the message index records associated with those chunks. They are found in the chunk index portion of the index data section. If message indexing is in use, Chunk Indexes are required. A Chunk Index record must be preceded in the index data section by Channel Info records for any channels that it references.

| Bytes | Name | Type | Description |
| --- | --- | --- | --- |
| 8 | start_time | Timestamp | First message record timestamp in the chunk. |
| 8 | end_time | Timestamp | Last message record timestamp in the chunk. |
| 8 | offset | uint64 | Offset to the chunk record from the start of the file. |
| N | message_index_offsets | KeyValues<uint16, uint64> | Mapping from channel ID to the offset of the message index record for that channel after the chunk, from the start of the file. Duplicate keys are not allowed. |
| 8 | message_index_length | uint64 | Total length in bytes of the message index records after the chunk, including lengths and opcodes. |
| 4 + N | compression | String | The compression used on this chunk. Refer to [supported compression formats][compression formats]. |
| 8 | compressed_size | uint64 | The compressed size of the chunk. |
| 8 | uncompressed_size | uint64 | The uncompressed size of the chunk. |

### Attachment (op=0x08)

Attachments can be used to attach artifacts such as calibration data, text, or core dumps. Attachment records must not appear within a chunk.

| Bytes | Name | Type | Description |
| --- | --- | --- | --- |
| 4 + N | name | String | Name of the attachment, e.g "scene1.jpg". |
| 8 | record_time | Timestamp | Time at which the attachment was recorded. |
| 4 + N | content_type | String | MIME Type (e.g "text/plain"). |
| 8 + N | data | uint64 length-prefixed bytes | Attachment data. |
| 4 | crc | uint32 | CRC32 checksum of preceding fields in the record. A value of zero indicates that CRC validation should not be performed. |

### Attachment Index (op=0x09)

The attachment index is an index to named attachments within the file. One record is recorded per attachment in the file. The attachment index records are written to the attachment index portion of the message data section.

| Bytes | Name | Type | Description |
| --- | --- | --- | --- |
| 8 | record_time | Timestamp | Timestamp at which the attachment was recorded. |
| 8 | data_size | uint64 | Size of the attachment data. |
| 4 + N | name | String | Name of the attachment. |
| 4 + N | content_type | String | MIME type of the attachment. |
| 8 | offset | uint64 | Byte offset to the attachment, from the start of the file. |

### Statistics (op=0x0A)

The statistics record contains statistics about the recorded data. It is the last record in the file before the footer. The record must be preceded in the index data section by Channel Info records for any channels referenced in the `channel_message_counts` field. If this is undesirable but some statistics are still desired, the field may be set to a zero-length map. The statistics record is optional.

| Bytes | Name | Type | Description |
| --- | --- | --- | --- |
| 8 | message_count | uint64 | Number of messages in the file across all topics. |
| 4 | channel_count | uint32 | Number of channels in the file across all topics. |
| 4 | attachment_count | uint32 | Number of attachments in the file. |
| 4 | chunk_count | uint32 | Number of chunks in the file. |
| N | channel_message_counts | KeyValues<uint16, uint64> | Mapping from channel ID to total message count for the channel. Duplicate keys are not allowed. |

## Further Reading

- [Feature explanations][feature explanations]: includes usage details that may be useful to implementers of readers or writers.
