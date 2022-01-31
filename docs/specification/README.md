# MCAP File Format Specification

> Status: DRAFT

## Overview

MCAP is a modular container file format for recording timestamped [pub/sub](https://en.wikipedia.org/wiki/Publish–subscribe_pattern) messages with arbitrary serialization formats.

MCAP files are designed to work well under various workloads, resource constraints, and durability requirements.

- [Structure](#file-structure)
  - [Header](#header)
  - [Footer](#footer)
  - [Data Section](#data-section)
  - [Summary Section](#summary-section)
- [Records](#records)
  - [Header](#header-op0x01)
  - [Footer](#footer-op0x02)
  - [Channel Info](#channel-info-op0x03)
  - [Message](#message-op0x04)
  - [Chunk](#chunk-op0x05)
  - [Message Index](#message-index-op0x06)
  - [Chunk Index](#chunk-index-op0x07)
  - [Attachment](#attachment-op0x08)
  - [Attachment Index](#attachment-index-op0x09)
  - [Statistics](#statistics-op0x0A)
  - [Metadata](#metadata-op0x0B)
  - [Metadata Index](#metadata-op0x0C)
  - [Summary Offset](#summary-offset-op0x0D)
  - [Data End](#data-end-op0x0E)
- [Serialization](#serialization)

## File Structure

A valid MCAP file is structured as follows. The Summary and Summary Offset sections are optional.

    <Magic><Header><Data section>[<Summary section>][<Summary Offset section>]<Footer><Magic>

The Data, Summary, and Summary Offset sections are structured as sequences of **records**:

    [<record type><record content length><record><record type><record content length><record>...]

Files not conforming to this structure are considered malformed.

### Magic

An MCAP file must begin and end with the following [magic bytes](https://en.wikipedia.org/wiki/File_format#Magic_number):

    0x89, M, C, A, P, 0x30, \r, \n

> Note: The version byte (ASCII zero 0x30) following "MCAP" will be updated to 1 (0x31) upon ratification of this specification. Until then, backward compatibility is not guaranteed.

### Header

The first record after the leading magic bytes is the [Header](#header) record.

    <0x01><record content length><record>

### Footer

The last record before the trailing magic bytes is the [Footer](#footer) record.

    <0x02><record content length><record>

### Data Section

The data section contains records with message data, attachments, and supporting records.

The following records are allowed to appear in the data section:

- Channel Info
- Message
- Attachment
- Chunk
- Message Index
- Metadata
- Data End

The last record in the data section MUST be the [Data End](#data-end-op0x0E) record.

### Summary Section

The optional summary section contains records for fast lookup of file information or other data section records.

The following records are allowed to appear in the summary section:

- Channel Info
- Chunk Index
- Attachment Index
- Statistics
- Metadata Index

All records in the summary section MUST be grouped by opcode.

> Why? Grouping Summary records by record opcode enables more efficient indexing of the summary in the Summary Offset section.

Channel Info records in the summary are duplicates of Channel Info records throughout the Data section.

### Summary Offset Section

The optional summary offset section contains [Summary Offset](#summary-offset-op0x0D) records for fast lookup of summary section records.

The summary offset section aids random access reading.

## Records

MCAP files may contain a variety of records. Records are identified by a single-byte **opcode**. Record opcodes in the range 0x01-0x7F are reserved for future MCAP format usage. 0x80-0xFF are reserved for application extensions and user proposals.

All MCAP records are serialized as follows:

    <record type><record content length><record content>

Record type is a single byte opcode, and record content length is a uint64 value.

Records may be extended by adding new fields at the end of existing fields. Readers should ignore any unknown fields.

The Footer record will not be extended.

### Header (op=0x01)

| Bytes | Name | Type | Description |
| --- | --- | --- | --- |
| 4 + N | profile | String | The profile is used for indicating requirements for fields throughout the file (encoding, user_data, etc). If the value matches one of the [well-known profiles][profiles], the file should conform to the profile. This field may also be supplied empty or containing a framework that is not one of those recognized. When specifying a custom profile, prefer the `x-` prefix to avoid conflict with future well-known profiles. |
| 4 + N | library | String | Free-form string for writer to specify its name, version, or other information for use in debugging |

### Footer (op=0x02)

A Footer record contains end-of-file information. It must be the last record in the file. Readers using the index to read the file will begin with by reading the footer and trailing magic.

| Bytes | Name | Type | Description |
| --- | --- | --- | --- |
| 8 | summary_start | uint64 | Byte offset of the start of file to the first record in the summary section. If there are no records in the summary section this should be 0. |
| 8 | summary_offset_start | uint64 | Byte offset from the start of the first record in the summary offset section. If there are no Summary Offset records this value should be 0. |
| 4 | summary_crc | uint32 | A CRC32 of all bytes from the start of the Summary section up through the end of the previous field in the footer record. A value of 0 indicates the CRC32 is not available. |

### Channel Info (op=0x03)

A Channel Info record defines an encoded stream of messages on a topic.

Channel Info records are uniquely identified within a file by their channel ID. A Channel Info record must occur at least once in the file prior to any message referring to its channel ID.

| Bytes | Name | Type | Description |
| --- | --- | --- | --- |
| 2 | id | uint16 | A unique identifier for this channel within the file. |
| 4 + N | topic | String | The channel topic. |
| 4 + N | message_encoding | String | Encoding for messages on this channel. The value should be one of the [well-known message encodings](./well-known-encodings.md). Custom values should use `x-` prefix. |
| 4 + N | schema_encoding | String | Format for the schema. The value should be one of the [well-known schema formats](./well-known-schema-formats.md). Custom values should use the `x-` prefix. |
| 4 + N | schema | uint32 lengh prefixed Bytes | Schema should conform to the schema_encoding. |
| 4 + N | schema_name | String | An identifier for the schema. The schema name should conform to any schema_encoding requirements. |
| 4 + N | metadata | Array<Tuple<string, string>> | Metadata about this channel |

Channel Info records may be duplicated in the summary section.

### Message (op=0x04)

A message record encodes a single timestamped message on a channel.

The message encoding must match that of the channel info record corresponding to the message's channel ID.

| Bytes | Name | Type | Description |
| --- | --- | --- | --- |
| 2 | channel_id | uint16 | Channel ID |
| 4 | sequence | uint32 | Optional message counter assigned by publisher. If not assigned by publisher, must be recorded by the recorder. |
| 8 | publish_time | Timestamp | Time at which the message was published. If not available, must be set to the record time. |
| 8 | record_time | Timestamp | Time at which the message was recorded by the recorder process. |
| N | message_data | Bytes | Message data, to be decoded according to the schema of the channel. |

### Chunk (op=0x05)

A Chunk contains a batch of channel info and message records. The batch of records contained in a chunk may be compressed or uncompressed.

All messages in the chunk must reference channel infos recorded earlier in the file (in a previous chunk or earlier in the current chunk).

| Bytes | Name | Type | Description |
| --- | --- | --- | --- |
| 8 | start_time | Timestamp | Earliest message record_time in the chunk. |
| 8 | end_time | Timestamp | Latest message record_time in the chunk. |
| 8 | uncompressed_size | uint64 | Uncompressed size of the `records` field. |
| 4 | uncompressed_crc | uint32 | CRC32 checksum of uncompressed `records` field. A value of zero indicates that CRC validation should not be performed. |
| 4 + N | compression | String | compression algorithm. i.e. `lz4`, `zstd`, `""`. An empty string indicates no compression. Refer to [well-known compression formats][compression formats]. |
| N | records | Bytes | Repeating sequences of `<record type><record content length><record content>`. Compressed with the algorithm in the `compression` field. |

### Message Index (op=0x06)

A Message Index record allows readers to locate individual message records within a chunk by their timestamp.

A sequence of Message Index records occurs immediately after each chunk. Exactly one Message Index record must exist in the sequence for every channel on which a message occurs inside the chunk.

| Bytes | Name | Type | Description |
| --- | --- | --- | --- |
| 2 | channel_id | uint16 | Channel ID. |
| 4 + N | records | Array<Tuple<Timestamp, uint64>> | Array of record_time and offset for each record. Offset is relative to the start of the uncompressed chunk data. |

Messages outside of chunks cannot be indexed.

### Chunk Index (op=0x07)

A Chunk Index record contains the location of a Chunk record and its associated Message Index records.

A Chunk Index record exists for every Chunk in the file.

| Bytes | Name | Type | Description |
| --- | --- | --- | --- |
| 8 | start_time | Timestamp | Earliest message record_time in the chunk. |
| 8 | end_time | Timestamp | Latest message record_time in the chunk. |
| 8 | chunk_start_offset | uint64 | Offset to the chunk record from the start of the file. |
| 8 | chunk_length | uint64 | The byte length of the chunk record. |
| 4 + N | message_index_offsets | Map<uint16, uint64> | Mapping from channel ID to the offset of the message index record for that channel after the chunk, from the start of the file. An empty map indicates no message indexing is available. |
| 8 | message_index_length | uint64 | Total length in bytes of the message index records after the chunk. |
| 4 + N | compression | String | The compression used within the chunk. Refer to [well-known compression formats formats][compression formats]. This field should match the the value in the corresponding Chunk record. |
| 8 | compressed_size | uint64 | The size of the chunk `records` field. |
| 8 | uncompressed_size | uint64 | The uncompressed size of the chunk `records` field. This field should match the value in the corresponding Chunk record. |

A Channel Info record MUST exist in the summary section for all channels referenced by chunk index records.

> Why? The typical use case for file readers using an index is fast random access to a specific message timestamp. Channel Info is a prerequisite for decoding Message record data. Without an easy-to-access copy of the Channel Info records, readers would need to search for Channel Info records from the start of the file, degrading random access read performance.

### Attachment (op=0x08)

Attachment records contain auxiliary artifacts such as text, core dumps, calibration data, or other arbitrary data.

Attachment records must not appear within a chunk.

| Bytes | Name | Type | Description |
| --- | --- | --- | --- |
| 4 + N | name | String | Name of the attachment, e.g "scene1.jpg". |
| 8 | created_at | Timestamp | Time at which the attachment was created. |
| 8 | record_time | Timestamp | Time at which the attachment was recorded. |
| 4 + N | content_type | String | MIME Type (e.g "text/plain"). |
| 8 + N | data | uint64 length-prefixed Bytes | Attachment data. |
| 4 | crc | uint32 | CRC32 checksum of preceding fields in the record. A value of zero indicates that CRC validation should not be performed. |

### Attachment Index (op=0x09)

An Attachment Index record contains the location of an attachment in the file. An Attachment Index record exists for every Attachment record in the file.

| Bytes | Name | Type | Description |
| --- | --- | --- | --- |
| 8 | offset | uint64 | Byte offset from the start of the file to the attachment record. |
| 8 | length | uint64 | Byte length of the record. |
| 8 | record_time | Timestamp | Timestamp at which the attachment was recorded. |
| 8 | data_size | uint64 | Size of the attachment data. |
| 4 + N | name | String | Name of the attachment. |
| 4 + N | content_type | String | MIME type of the attachment. |

### Statistics (op=0x0A)

A Statistics record contains summary information about the recorded data. The statistics record is optional, but the file should contain at most one.

| Bytes | Name | Type | Description |
| --- | --- | --- | --- |
| 8 | message_count | uint64 | Number of messages in the file across all topics. |
| 4 | channel_count | uint32 | Number of channels in the file across all topics. |
| 4 | attachment_count | uint32 | Number of attachments in the file. |
| 4 | chunk_count | uint32 | Number of chunks in the file. |
| 4 + N | channel_message_counts | Map<uint16, uint64> | Mapping from channel ID to total message count for the channel. An empty map indicates this statistic is not available. |

When using a Statistics record with channel_message_counts, the Summary Data section MUST contain a copy of all Channel Info records. The Channel Info records MUST occur prior to the statistics record.

> Why? The typical usecase for tools is to provide a listing of the types and quantities of messages stored in the file. Without an easy to access copy of the Channel Info records, tools would need to linearly scan the file for Channel Info records to display what types of messages exist in the file.

### Metadata (op=0x0B)

A metadata record contains arbitrary user data in key-value pairs.

| Bytes | Name | Type | Description |
| --- | --- | --- | --- |
| 4 + N | name | String | Example: `map_metadata`. |
| 4 + N | metadata | Array<Tuple<string, string>> | Example keys: `robot_id`, `git_sha`, `timezone`, `run_id`. |

### Metadata Index (op=0x0C)

A metadata record contains arbitrary user data in key-value pairs.

| Bytes | Name | Type | Description |
| --- | --- | --- | --- |
| 8 | offset | uint64 | Byte offset from the start of the file to the metadata record. |
| 8 | length | uint64 | Total byte length of the record. |
| 4 + N | name | String | Name of the metadata record. |

### Summary Offset (op=0x0D)

A Summary Offset record contains the location of records within the summary section. Each Summary Offset record corresponds to a group of summary records with the same opcode.

| Bytes | Name | Type | Description |
| --- | --- | --- | --- |
| 1 | group_opcode | uint8 | The opcode of all records in the group. |
| 8 | group_start | uint64 | Byte offset from the start of the file of the first record in the group. |
| 8 | group_length | uint64 | Total byte length of all records in the group. |

### Data End (op=0x0E)

A Data End record indicates the end of the data section.

> Why? When reading a file from start to end, there is ambiguity when the data section ends and the summary section starts because some records (i.e. Channel Info) can repeat for summary data. The Data End record provides a clear dilineation the data section has ended.

| Bytes | Name | Type | Description |
| --- | --- | --- | --- |
| 4 | data_section_crc | int32 | CRC32 of all bytes in the data section. A value of 0 indicates the CRC32 is not available. |

## Serialization

### Fixed-width types

Multi-byte integers (uint16, uint32, uint64) are serialized using [little-endian byte order](https://en.wikipedia.org/wiki/Endianness).

### String

Strings are serialized using a uint32 byte length followed by the string data, which should be valid [UTF-8](https://en.wikipedia.org/wiki/UTF-8).

    <byte length><utf-8 bytes>

### Bytes

Bytes is sequence of bytes with no additional requirements.

    <bytes>

### Tuple<first_type, second_type>

Tuple represents a pair of values. The first value has type first_type and the second has type second_type.

Tuple is serialized by serializing the first value and then the second value:

    <first value><second value>

A Tuple<uint8, uint32>:

    <uint8><uint32>

A Tuple<uint16, string>:

    <uint16><string>

    <uint16><uint32><utf-8 bytes>

### Array<array_type>

Arrays are serialized using a uint32 byte length followed by the serialized array elements.

    <byte length><serialized element><serialized element>...

An array of uint32 is specified as Array<uint32> and serialized as:

    <byte length><uint32><uint32><uint32>...

### Timestamp

uint64 nanoseconds since a user-understood epoch (i.e unix epoch, robot boot time, etc.)

### Map<key_type, value_type>

A Map is an [association](https://en.wikipedia.org/wiki/Associative_array) of keys to values. Duplicate keys are not allowed.

A map is serialized as an array of tuples i.e. `Array<Tuple<key_type, value_type>>`. See array and tuple serialization.

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
[Channel Info 1]
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
[Channel Info 1]
[Channel Info 2]
[Message on 1]
[Message on 1]
[Message on 2]
[Channel Info 3]
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
  [Channel Info 1]
  [Channel Info 2]
  [Message on 1]
  [Message on 1]
  [Message on 2]
[Attachment]
[Chunk]
  [Channel Info 3]
  [Message on 3]
  [Message on 1]
[Footer]
```

### Multiple Messages with Summary Data

```
[Header]
[Channel Info 1]
[Channel Info 2]
[Message on 1]
[Message on 1]
[Message on 2]
[Channel Info 3]
[Attachment]
[Message on 3]
[Message on 1]
[Data End]
[Statistics]
[Channel Info 1]
[Channel Info 2]
[Channel Info 3]
[Summary Offset 0x01]
[Footer]
```

### Multiple Messages with Chunk Indices

```
[Header]
[Chunk A]
  [Channel Info 1]
  [Channel Info 2]
  [Message on 1]
  [Message on 1]
  [Message on 2]
[Message Index 1]
[Message Index 2]
[Attachment 1]
[Chunk B]
  [Channel Info 3]
  [Message on 3]
  [Message on 1]
[Message Index 3]
[Message Index 1]
[Data End]
[Channel Info 1]
[Channel Info 2]
[Channel Info 3]
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

- [Feature explanations][feature explanations]: includes usage details that may be useful to implementers of readers or writers.

[profiles]: ./profiles
[compression formats]: ./compression/supported-compression-formats.md
[explanatory notes]: ./notes/explanatory-notes.md
[feature explanations]: ./notes/explanatory-notes.md#feature-explanations
