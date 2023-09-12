---
sidebar_label: Specification
sidebar_position: 1
---

# MCAP Format Specification

[compression_formats]: ./registry.md#well-known-compression-formats
[message_encodings]: ./registry.md#well-known-message-encodings
[schema_encodings]: ./registry.md#well-known-schema-encodings
[profiles]: ./registry.md#well-known-profiles
[feature_explanations]: ./notes.md#feature-explanations

## Overview

MCAP is a modular container file format for recording timestamped [pub/sub](https://en.wikipedia.org/wiki/Publish–subscribe_pattern) messages with arbitrary serialization formats.

MCAP files are designed to work well under various workloads, resource constraints, and durability requirements.

A [Kaitai Struct](http://kaitai.io) description for the MCAP format is provided at [mcap.ksy](https://github.com/foxglove/mcap/blob/main/website/docs/spec/mcap.ksy).

## File Structure

A valid MCAP file is structured as follows. The Summary and Summary Offset sections are optional.

    <Magic><Header><Data section>[<Summary section>][<Summary Offset section>]<Footer><Magic>

The Data, Summary, and Summary Offset sections are structured as sequences of **records**:

    [<record type><record content length><record><record type><record content length><record>...]

Files not conforming to this structure are considered malformed.

### Magic

An MCAP file must begin and end with the following [magic bytes](https://en.wikipedia.org/wiki/File_format#Magic_number):

    0x89, M, C, A, P, 0x30, \r, \n

The byte following "MCAP" is the major version byte. `0x30` is the ASCII character `0`. Any changes to this specification document (i.e. adding fields to records, introducing new records) will be binary backward-compatible within the major version.

### Header

The first record after the leading magic bytes is the [Header](#header-op0x01) record.

    <0x01><record content length><record>

### Footer

The last record before the trailing magic bytes is the [Footer](#footer-op0x02) record.

    <0x02><record content length><record>

### Data Section

The data section contains records with message data, attachments, and supporting records.

The following records are allowed to appear in the data section:

- [Schema](#schema-op0x03)
- [Channel](#channel-op0x04)
- [Message](#message-op0x05)
- [Attachment](#attachment-op0x09)
- [Chunk](#chunk-op0x06)
- [Message Index](#message-index-op0x07)
- [Metadata](#metadata-op0x0C)
- [Data End](#data-end-op0x0F)

The last record in the data section MUST be the [Data End](#data-end-op0x0F) record.

#### Use of chunk records

MCAP files can have Schema, Channel, and Message records written directly to the data section, or they can be written into Chunk records to facilitate indexing and compression. For MCAPs that include [Chunk Index](#chunk-index-op0x08) records in the summary section, all Message records should be written into Chunk records.

> Why? The presence of Chunk Index records in the summary section indicates to readers that the MCAP is indexed, and they can use those records to look up messages by log time or topic. However, Message records outside of chunks cannot be indexed, and may not be found by readers using the index.

### Summary Section

The optional summary section contains records for fast lookup of file information or other data section records.

The following records are allowed to appear in the summary section:

- [Schema](#schema-op0x03)
- [Channel](#channel-op0x04)
- [Chunk Index](#chunk-index-op0x08)
- [Attachment Index](#attachment-index-op0x0A)
- [Metadata Index](#metadata-index-op0x0D)
- [Statistics](#statistics-op0x0B)

All records in the summary section MUST be grouped by opcode.

> Why? Grouping Summary records by record opcode enables more efficient indexing of the summary in the Summary Offset section.

Channel records in the summary are duplicates of Channel records throughout the Data section.

Schema records in the summary are duplicates of Schema records throughout the Data section.

### Summary Offset Section

The optional summary offset section contains [Summary Offset](#summary-offset-op0x0E) records for fast lookup of summary section records.

The summary offset section aids random access reading.

## Records

MCAP files may contain a variety of records. Records are identified by a single-byte **opcode**. Record opcodes in the range 0x01-0x7F are reserved for future MCAP format usage. 0x80-0xFF are reserved for application extensions and user proposals. 0x00 is not a valid opcode.

All MCAP records are serialized as follows:

    <record type><record content length><record content>

Record type is a single byte opcode, and record content length is a uint64 value.

Records may be extended by adding new fields at the end of existing fields. Readers should ignore any unknown fields.

> The Footer and Message records will not be extended, since their formats do not allow for backward-compatible size changes.

Each record definition below contains a `Type` column. See the [Serialization](#serialization) section on how to serialize each type.

### Header (op=0x01)

| Bytes | Name    | Type   | Description                                                                                                                                                                                                                                                                                                              |
| ----- | ------- | ------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| 4 + N | profile | String | The profile is used for indicating requirements for fields throughout the file (encoding, user_data, etc). If the value matches one of the [well-known profiles][profiles], the file should conform to the profile. This field may also be supplied empty or containing a framework that is not one of those recognized. |
| 4 + N | library | String | Free-form string for writer to specify its name, version, or other information for use in debugging                                                                                                                                                                                                                      |

### Footer (op=0x02)

A Footer record contains end-of-file information. It must be the last record in the file. Readers using the index to read the file will begin with by reading the footer and trailing magic.

| Bytes | Name                 | Type   | Description                                                                                                                                                                                                       |
| ----- | -------------------- | ------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 8     | summary_start        | uint64 | Byte offset of the start of file to the first record in the summary section. If there are no records in the summary section this should be 0.                                                                     |
| 8     | summary_offset_start | uint64 | Byte offset from the start of the first record in the summary offset section. If there are no Summary Offset records this value should be 0.                                                                      |
| 4     | summary_crc          | uint32 | A CRC32 of all bytes from the start of the Summary section up through and including the end of the previous field (summary_offset_start) in the footer record. A value of 0 indicates the CRC32 is not available. |

### Schema (op=0x03)

A Schema record defines an individual schema.

Schema records are uniquely identified within a file by their schema ID. A Schema record must occur at least once in the file prior to any Channel referring to its ID. Any two schema records sharing a common ID must be identical.

| Bytes | Name     | Type                         | Description                                                                                                                                 |
| ----- | -------- | ---------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------- |
| 2     | id       | uint16                       | A unique identifier for this schema within the file. Must not be zero                                                                       |
| 4 + N | name     | String                       | An identifier for the schema.                                                                                                               |
| 4 + N | encoding | String                       | Format for the schema. The [well-known schema encodings][schema_encodings] are preferred. An empty string indicates no schema is available. |
| 4 + N | data     | uint32 length-prefixed Bytes | Must conform to the schema encoding. If `encoding` is an empty string, `data` should be 0 length.                                           |

Schema records may be duplicated in the summary section. A Schema record with an id of zero is invalid and should be ignored by readers.

### Channel (op=0x04)

A Channel record defines an encoded stream of messages on a topic.

Channel records are uniquely identified within a file by their channel ID. A Channel record must occur at least once in the file prior to any message referring to its channel ID. Any two channel records sharing a common ID must be identical.

| Bytes | Name             | Type                  | Description                                                                                                 |
| ----- | ---------------- | --------------------- | ----------------------------------------------------------------------------------------------------------- |
| 2     | id               | uint16                | A unique identifier for this channel within the file.                                                       |
| 2     | schema_id        | uint16                | The schema for messages on this channel. A schema_id of 0 indicates there is no schema for this channel.    |
| 4 + N | topic            | String                | The channel topic.                                                                                          |
| 4 + N | message_encoding | String                | Encoding for messages on this channel. The [well-known message encodings][message_encodings] are preferred. |
| 4 + N | metadata         | `Map<string, string>` | Metadata about this channel                                                                                 |

Channel records may be duplicated in the summary section.

### Message (op=0x05)

A message record encodes a single timestamped message on a channel.

The message encoding and schema must match that of the Channel record corresponding to the message's channel ID.

| Bytes | Name         | Type      | Description                                                                                                                                                                                                                               |
| ----- | ------------ | --------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 2     | channel_id   | uint16    | Channel ID                                                                                                                                                                                                                                |
| 4     | sequence     | uint32    | Optional message counter to detect message gaps. If your middleware publisher provides a sequence number you can use that, or you can assign a sequence number in the recorder, or set to zero if this is not relevant for your workflow. |
| 8     | log_time     | Timestamp | Time at which the message was recorded.                                                                                                                                                                                                   |
| 8     | publish_time | Timestamp | Time at which the message was published. If not available, must be set to the log time.                                                                                                                                                   |
| N     | data         | Bytes     | Message data, to be decoded according to the schema of the channel.                                                                                                                                                                       |

### Chunk (op=0x06)

A Chunk contains a batch of Schema, Channel, and Message records. The batch of records contained in a chunk may be compressed or uncompressed.

All messages in the chunk must reference channels recorded earlier in the file (in a previous chunk, earlier in the current chunk, or earlier in the data section).

| Bytes | Name               | Type                         | Description                                                                                                                                                |
| ----- | ------------------ | ---------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 8     | message_start_time | Timestamp                    | Earliest message log_time in the chunk. Zero if the chunk has no messages.                                                                                 |
| 8     | message_end_time   | Timestamp                    | Latest message log_time in the chunk. Zero if the chunk has no messages.                                                                                   |
| 8     | uncompressed_size  | uint64                       | Uncompressed size of the `records` field.                                                                                                                  |
| 4     | uncompressed_crc   | uint32                       | CRC32 checksum of uncompressed `records` field. A value of zero indicates that CRC validation should not be performed.                                     |
| 4 + N | compression        | String                       | compression algorithm. i.e. `zstd`, `lz4`, `""`. An empty string indicates no compression. Refer to [well-known compression formats][compression_formats]. |
| 8 + N | records            | uint64 length-prefixed Bytes | Repeating sequences of `<record type><record content length><record content>`. Compressed with the algorithm in the `compression` field.                   |

### Message Index (op=0x07)

A Message Index record allows readers to locate individual message records within a chunk by their timestamp.

A sequence of Message Index records occurs immediately after each chunk. Exactly one Message Index record must exist in the sequence for every channel on which a message occurs inside the chunk.

| Bytes | Name       | Type                              | Description                                                                                                   |
| ----- | ---------- | --------------------------------- | ------------------------------------------------------------------------------------------------------------- |
| 2     | channel_id | uint16                            | Channel ID.                                                                                                   |
| 4 + N | records    | `Array<Tuple<Timestamp, uint64>>` | Array of log_time and offset for each record. Offset is relative to the start of the uncompressed chunk data. |

Messages outside of chunks cannot be indexed.

### Chunk Index (op=0x08)

A Chunk Index record contains the location of a Chunk record and its associated Message Index records.

A Chunk Index record exists for every Chunk in the file.

| Bytes | Name                  | Type                  | Description                                                                                                                                                                              |
| ----- | --------------------- | --------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 8     | message_start_time    | Timestamp             | Earliest message log_time in the chunk. Zero if the chunk has no messages.                                                                                                               |
| 8     | message_end_time      | Timestamp             | Latest message log_time in the chunk. Zero if the chunk has no messages.                                                                                                                 |
| 8     | chunk_start_offset    | uint64                | Offset to the chunk record from the start of the file.                                                                                                                                   |
| 8     | chunk_length          | uint64                | Byte length of the chunk record, including opcode and length prefix.                                                                                                                     |
| 4 + N | message_index_offsets | `Map<uint16, uint64>` | Mapping from channel ID to the offset of the message index record for that channel after the chunk, from the start of the file. An empty map indicates no message indexing is available. |
| 8     | message_index_length  | uint64                | Total length in bytes of the message index records after the chunk.                                                                                                                      |
| 4 + N | compression           | String                | The compression used within the chunk. Refer to [well-known compression formats][compression_formats]. This field should match the the value in the corresponding Chunk record.          |
| 8     | compressed_size       | uint64                | The size of the chunk `records` field.                                                                                                                                                   |
| 8     | uncompressed_size     | uint64                | The uncompressed size of the chunk `records` field. This field should match the value in the corresponding Chunk record.                                                                 |

A Schema and Channel record MUST exist in the summary section for all channels referenced by chunk index records.

> Why? The typical use case for file readers using an index is fast random access to a specific message timestamp. Channel is a prerequisite for decoding Message record data. Without an easy-to-access copy of the Channel records, readers would need to search for Channel records from the start of the file, degrading random access read performance.

### Attachment (op=0x09)

Attachment records contain auxiliary artifacts such as text, core dumps, calibration data, or other arbitrary data.

Attachment records must not appear within a chunk.

| Bytes | Name        | Type                         | Description                                                                                                              |
| ----- | ----------- | ---------------------------- | ------------------------------------------------------------------------------------------------------------------------ |
| 8     | log_time    | Timestamp                    | Time at which the attachment was recorded.                                                                               |
| 8     | create_time | Timestamp                    | Time at which the attachment was created. If not available, must be set to zero.                                         |
| 4 + N | name        | String                       | Name of the attachment, e.g "scene1.jpg".                                                                                |
| 4 + N | media_type  | String                       | [Media type](https://en.wikipedia.org/wiki/Media_type) (e.g "text/plain").                                               |
| 8 + N | data        | uint64 length-prefixed Bytes | Attachment data.                                                                                                         |
| 4     | crc         | uint32                       | CRC32 checksum of preceding fields in the record. A value of zero indicates that CRC validation should not be performed. |

### Metadata (op=0x0C)

A metadata record contains arbitrary user data in key-value pairs.

| Bytes | Name     | Type                  | Description                                         |
| ----- | -------- | --------------------- | --------------------------------------------------- |
| 4 + N | name     | String                | Example: `my_company_name_hardware_info`.           |
| 4 + N | metadata | `Map<string, string>` | Example keys: `part_id`, `serial`, `board_revision` |

### Data End (op=0x0F)

A Data End record indicates the end of the data section.

> Why? When reading a file from start to end, there is ambiguity when the data section ends and the summary section starts because some records (i.e. Channel) can repeat for summary data. The Data End record provides a clear delineation the data section has ended.

| Bytes | Name             | Type   | Description                                                                                                                    |
| ----- | ---------------- | ------ | ------------------------------------------------------------------------------------------------------------------------------ |
| 4     | data_section_crc | uint32 | CRC32 of all bytes from the beginning of the file up to the DataEnd record. A value of 0 indicates the CRC32 is not available. |

### Attachment Index (op=0x0A)

An Attachment Index record contains the location of an attachment in the file. An Attachment Index record exists for every Attachment record in the file.

| Bytes | Name        | Type      | Description                                                                                  |
| ----- | ----------- | --------- | -------------------------------------------------------------------------------------------- |
| 8     | offset      | uint64    | Byte offset from the start of the file to the attachment record.                             |
| 8     | length      | uint64    | Byte length of the attachment record, including opcode and length prefix.                    |
| 8     | log_time    | Timestamp | Time at which the attachment was recorded.                                                   |
| 8     | create_time | Timestamp | Time at which the attachment was created. If not available, must be set to zero.             |
| 8     | data_size   | uint64    | Size of the attachment data.                                                                 |
| 4 + N | name        | String    | Name of the attachment.                                                                      |
| 4 + N | media_type  | String    | [Media type](https://en.wikipedia.org/wiki/Media_type) of the attachment (e.g "text/plain"). |

### Metadata Index (op=0x0D)

A metadata index record contains the location of a metadata record within the file.

| Bytes | Name   | Type   | Description                                                          |
| ----- | ------ | ------ | -------------------------------------------------------------------- |
| 8     | offset | uint64 | Byte offset from the start of the file to the metadata record.       |
| 8     | length | uint64 | Total byte length of the record, including opcode and length prefix. |
| 4 + N | name   | String | Name of the metadata record.                                         |

### Statistics (op=0x0B)

A Statistics record contains summary information about the recorded data. The statistics record is optional, but the file should contain at most one.

| Bytes | Name                   | Type                  | Description                                                                                                             |
| ----- | ---------------------- | --------------------- | ----------------------------------------------------------------------------------------------------------------------- |
| 8     | message_count          | uint64                | Number of Message records in the file.                                                                                  |
| 2     | schema_count           | uint16                | Number of unique schema IDs in the file, not including zero.                                                            |
| 4     | channel_count          | uint32                | Number of unique channel IDs in the file.                                                                               |
| 4     | attachment_count       | uint32                | Number of Attachment records in the file.                                                                               |
| 4     | metadata_count         | uint32                | Number of Metadata records in the file.                                                                                 |
| 4     | chunk_count            | uint32                | Number of Chunk records in the file.                                                                                    |
| 8     | message_start_time     | Timestamp             | Earliest message log_time in the file. Zero if the file has no messages.                                                |
| 8     | message_end_time       | Timestamp             | Latest message log_time in the file. Zero if the file has no messages.                                                  |
| 4 + N | channel_message_counts | `Map<uint16, uint64>` | Mapping from channel ID to total message count for the channel. An empty map indicates this statistic is not available. |

When using a Statistics record with a non-empty channel_message_counts, the Summary Data section MUST contain a copy of all Channel records. The Channel records MUST occur prior to the statistics record.

> Why? The typical use case for tools is to provide a listing of the types and quantities of messages stored in the file. Without an easy to access copy of the Channel records, tools would need to linearly scan the file for Channel records to display what types of messages exist in the file.

### Summary Offset (op=0x0E)

A Summary Offset record contains the location of records within the summary section. Each Summary Offset record corresponds to a group of summary records with the same opcode.

| Bytes | Name         | Type   | Description                                                              |
| ----- | ------------ | ------ | ------------------------------------------------------------------------ |
| 1     | group_opcode | uint8  | The opcode of all records in the group.                                  |
| 8     | group_start  | uint64 | Byte offset from the start of the file of the first record in the group. |
| 8     | group_length | uint64 | Total byte length of all records in the group.                           |

## Serialization

### Fixed-width types

Multi-byte integers (`uint16`, `uint32`, `uint64`) are serialized using [little-endian byte order](https://en.wikipedia.org/wiki/Endianness).

### String

Strings are serialized using a `uint32` byte length followed by the string data, which should be valid [UTF-8](https://en.wikipedia.org/wiki/UTF-8).

    <byte length><utf-8 bytes>

### Bytes

Bytes is sequence of bytes with no additional requirements.

    <bytes>

### Tuple<first_type, second_type>

Tuple represents a pair of values. The first value has type first_type and the second has type second_type.

Tuple is serialized by serializing the first value and then the second value:

    <first value><second value>

Example `Tuple<uint8, uint32>`:

    <uint8><uint32>

Example `Tuple<uint16, string>`:

    <uint16><string>

    <uint16><uint32><utf-8 bytes>

### Array<array_type>

Arrays are serialized using a `uint32` byte length followed by the serialized array elements.

    <byte length><serialized element><serialized element>...

An array of uint64 is specified as `Array<uint64>` and serialized as:

    <byte length><uint64><uint64><uint64>...

> Since arrays use a `uint32` byte length prefix, the maximum size of the serialized array elements cannot exceed 4,294,967,295 bytes.

### Timestamp

`uint64` nanoseconds since a user-understood epoch (i.e unix epoch, robot boot time, etc.)

### Map<key_type, value_type>

A Map is an [association](https://en.wikipedia.org/wiki/Associative_array) of unique keys to values.

Maps are serialized using a `uint32` byte length followed by the serialized map key/value entries. The key and value entries are serialized according to their `key_type` and `value_type`.

    <byte length><key><value><key><value>...

A `Map<string, string>` would be serialized as:

    <byte length><uint32 key length><utf-8 key bytes><uint32 value length><utf-8 value bytes>...

A serialization which has duplicate keys may cause indeterminate decoding.

## Diagrams

The following diagrams demonstrate various valid MCAP files.

### Empty file

The smallest valid MCAP file, containing no data.

```
[Header]
[Footer]
```

### Single Message

An MCAP file containing 1 message.

```
[Header]
[Schema A]
[Channel 1 (A)]
[Message on Channel 1]
[Footer]
```

### Single Attachment

An MCAP file containing 1 attachment

```
[Header]
[Attachment]
[Footer]
```

### Multiple Messages

```
[Header]
[Schema A]
[Channel 1 (A)]
[Channel 2 (A)]
[Message on 1]
[Message on 1]
[Message on 2]
[Schema B]
[Channel 3 (B)]
[Attachment]
[Message on 3]
[Message on 1]
[Footer]
```

### Messages in Chunks

A writer may choose to put messages in Chunks to compress record data. This MCAP file does not use any index records.

```
[Header]
[Chunk]
  [Schema A]
  [Channel 1 (A)]
  [Channel 2 (A)]
  [Message on 1]
  [Message on 1]
  [Message on 2]
[Attachment]
[Chunk]
  [Schema B]
  [Channel 3 (B)]
  [Message on 3]
  [Message on 1]
[Footer]
```

### Multiple Messages with Summary Data

```
[Header]
[Schema A]
[Channel 1 (A)]
[Channel 2 (A)]
[Message on 1]
[Message on 1]
[Message on 2]
[Schema B]
[Channel 3 (B)]
[Attachment]
[Message on 3]
[Message on 1]
[Data End]
[Statistics]
[Schema A]
[Schema B]
[Channel 1]
[Channel 2]
[Channel 3]
[Summary Offset 0x01]
[Footer]
```

### Multiple Messages with Chunk Indices

```
[Header]
[Chunk A]
  [Schema A]
  [Channel 1 (A)]
  [Channel 2 (B)]
  [Message on 1]
  [Message on 1]
  [Message on 2]
[Message Index 1]
[Message Index 2]
[Attachment 1]
[Chunk B]
  [Schema B]
  [Channel 3 (B)]
  [Message on 3]
  [Message on 1]
[Message Index 3]
[Message Index 1]
[Data End]
[Schema A]
[Schema B]
[Channel 1]
[Channel 2]
[Channel 3]
[Chunk Index A]
[Chunk Index B]
[Attachment Index 1]
[Statistics]
[Summary Offset 0x01]
[Summary Offset 0x05]
[Summary Offset 0x07]
[Summary Offset 0x08]
[Footer]
```

## Further Reading

- [Feature explanations][feature_explanations]: includes usage details that may be useful to implementers of readers or writers.
