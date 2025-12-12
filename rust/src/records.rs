//! Raw records parsed from an MCAP file
//!
//! See <https://github.com/foxglove/mcap/tree/main/docs/specification>
//!
//! You probably want to user higher-level interfaces, like
//! [`Message`](crate::Message), [`Channel`](crate::Channel), and [`Schema`](crate::Schema),
//! read from iterators like [`MessageStream`](crate::MessageStream).

use std::{borrow::Cow, collections::BTreeMap};

use binrw::*;

use crate::{McapError, McapResult};

/// Opcodes for MCAP file records.
///
/// "Records are identified by a single-byte opcode.
/// Record opcodes in the range 0x01-0x7F are reserved for future MCAP format usage.
/// 0x80-0xFF are reserved for application extensions and user proposals."
pub mod op {
    pub const HEADER: u8 = 0x01;
    pub const FOOTER: u8 = 0x02;
    pub const SCHEMA: u8 = 0x03;
    pub const CHANNEL: u8 = 0x04;
    pub const MESSAGE: u8 = 0x05;
    pub const CHUNK: u8 = 0x06;
    pub const MESSAGE_INDEX: u8 = 0x07;
    pub const CHUNK_INDEX: u8 = 0x08;
    pub const ATTACHMENT: u8 = 0x09;
    pub const ATTACHMENT_INDEX: u8 = 0x0A;
    pub const STATISTICS: u8 = 0x0B;
    pub const METADATA: u8 = 0x0C;
    pub const METADATA_INDEX: u8 = 0x0D;
    pub const SUMMARY_OFFSET: u8 = 0x0E;
    pub const DATA_END: u8 = 0x0F;
}

/// A raw record from an MCAP file.
///
/// For records with large slices of binary data (schemas, messages, chunks...),
/// we use a [`CoW`](std::borrow::Cow) that can either borrow directly from the mapped file,
/// or hold its own buffer if it was decompressed from a chunk.
#[derive(Debug)]
pub enum Record<'a> {
    Header(Header),
    Footer(Footer),
    Schema {
        header: SchemaHeader,
        data: Cow<'a, [u8]>,
    },
    Channel(Channel),
    Message {
        header: MessageHeader,
        data: Cow<'a, [u8]>,
    },
    Chunk {
        header: ChunkHeader,
        data: Cow<'a, [u8]>,
    },
    MessageIndex(MessageIndex),
    ChunkIndex(ChunkIndex),
    Attachment {
        header: AttachmentHeader,
        data: Cow<'a, [u8]>,
        crc: u32,
    },
    AttachmentIndex(AttachmentIndex),
    Statistics(Statistics),
    Metadata(Metadata),
    MetadataIndex(MetadataIndex),
    SummaryOffset(SummaryOffset),
    DataEnd(DataEnd),
    /// A record of unknown type
    Unknown {
        opcode: u8,
        data: Cow<'a, [u8]>,
    },
}

impl Record<'_> {
    pub fn opcode(&self) -> u8 {
        match &self {
            Record::Header(_) => op::HEADER,
            Record::Footer(_) => op::FOOTER,
            Record::Schema { .. } => op::SCHEMA,
            Record::Channel(_) => op::CHANNEL,
            Record::Message { .. } => op::MESSAGE,
            Record::Chunk { .. } => op::CHUNK,
            Record::MessageIndex(_) => op::MESSAGE_INDEX,
            Record::ChunkIndex(_) => op::CHUNK_INDEX,
            Record::Attachment { .. } => op::ATTACHMENT,
            Record::AttachmentIndex(_) => op::ATTACHMENT_INDEX,
            Record::Statistics(_) => op::STATISTICS,
            Record::Metadata(_) => op::METADATA,
            Record::MetadataIndex(_) => op::METADATA_INDEX,
            Record::SummaryOffset(_) => op::SUMMARY_OFFSET,
            Record::DataEnd(_) => op::DATA_END,
            Record::Unknown { opcode, .. } => *opcode,
        }
    }

    /// Moves this value into a fully-owned variant with no borrows. This should be free for
    /// already-owned values.
    pub fn into_owned(self) -> Record<'static> {
        match self {
            Record::Header(header) => Record::Header(header),
            Record::Footer(footer) => Record::Footer(footer),
            Record::Schema { header, data } => Record::Schema {
                header,
                data: Cow::Owned(data.into_owned()),
            },
            Record::Channel(channel) => Record::Channel(channel),
            Record::Message { header, data } => Record::Message {
                header,
                data: Cow::Owned(data.into_owned()),
            },
            Record::Chunk { header, data } => Record::Chunk {
                header,
                data: Cow::Owned(data.into_owned()),
            },
            Record::MessageIndex(index) => Record::MessageIndex(index),
            Record::ChunkIndex(index) => Record::ChunkIndex(index),
            Record::Attachment { header, data, crc } => Record::Attachment {
                header,
                data: Cow::Owned(data.into_owned()),
                crc,
            },
            Record::AttachmentIndex(index) => Record::AttachmentIndex(index),
            Record::Statistics(statistics) => Record::Statistics(statistics),
            Record::Metadata(metadata) => Record::Metadata(metadata),
            Record::MetadataIndex(index) => Record::MetadataIndex(index),
            Record::SummaryOffset(offset) => Record::SummaryOffset(offset),
            Record::DataEnd(end) => Record::DataEnd(end),
            Record::Unknown { opcode, data } => Record::Unknown {
                opcode,
                data: Cow::Owned(data.into_owned()),
            },
        }
    }
}

#[binrw]
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
struct McapString {
    #[br(temp)]
    #[bw(calc = inner.len() as u32)]
    pub len: u32,

    #[br(count = len, try_map = String::from_utf8)]
    #[bw(map = |s| s.as_bytes())]
    pub inner: String,
}

/// Avoids taking a copy to turn a String to an McapString for serialization
#[binrw::writer(writer, endian)]
fn write_string(s: &String) -> BinResult<()> {
    (s.len() as u32).write_options(writer, endian, ())?;
    (s.as_bytes()).write_options(writer, endian, ())?;
    Ok(())
}

#[binrw::parser(reader, endian)]
fn parse_vec<T: BinRead<Args<'static> = ()>>() -> BinResult<Vec<T>> {
    let mut parsed = Vec::new();

    // Length of the map in BYTES, not records.
    let byte_len: u32 = BinRead::read_options(reader, endian, ())?;
    let pos = reader.stream_position()?;

    while (reader.stream_position()? - pos) < byte_len as u64 {
        parsed.push(T::read_options(reader, endian, ())?);
    }

    Ok(parsed)
}

#[expect(clippy::ptr_arg)]
#[binrw::writer(writer, endian)]
fn write_vec<T: BinWrite<Args<'static> = ()>>(v: &Vec<T>) -> BinResult<()> {
    use std::io::SeekFrom;
    let start = writer.stream_position()?;
    (!0u32).write_options(writer, endian, ())?; // Revisit...
    for e in v.iter() {
        e.write_options(writer, endian, ())?;
    }
    let end = writer.stream_position()?;
    let data_len = end - start - 4;
    writer.seek(SeekFrom::Start(start))?;
    (data_len as u32).write_options(writer, endian, ())?;
    assert_eq!(writer.seek(SeekFrom::End(0))?, end);
    Ok(())
}

#[derive(Debug, Clone, Eq, PartialEq, BinRead, BinWrite)]
pub struct Header {
    #[br(map = |s: McapString| s.inner )]
    #[bw(write_with = write_string)]
    pub profile: String,

    #[br(map = |s: McapString| s.inner )]
    #[bw(write_with = write_string)]
    pub library: String,
}

#[derive(Debug, Default, Clone, Copy, Eq, PartialEq, BinRead, BinWrite)]
pub struct Footer {
    pub summary_start: u64,
    pub summary_offset_start: u64,
    pub summary_crc: u32,
}

#[derive(Debug, Clone, Eq, PartialEq, BinRead, BinWrite)]
pub struct SchemaHeader {
    pub id: u16,

    #[br(map = |s: McapString| s.inner )]
    #[bw(write_with = write_string)]
    pub name: String,

    #[br(map = |s: McapString| s.inner )]
    #[bw(write_with = write_string)]
    pub encoding: String,
}

#[binrw::parser(reader, endian)]
fn parse_string_map() -> BinResult<BTreeMap<String, String>> {
    let mut parsed = BTreeMap::new();

    // Length of the map in BYTES, not records.
    let byte_len: u32 = BinRead::read_options(reader, endian, ())?;
    let pos = reader.stream_position()?;

    while (reader.stream_position()? - pos) < byte_len as u64 {
        let k = McapString::read_options(reader, endian, ())?;
        let v = McapString::read_options(reader, endian, ())?;
        if let Some(_prev) = parsed.insert(k.inner, v.inner) {
            return Err(binrw::Error::Custom {
                pos,
                err: Box::new("Duplicate keys in map"),
            });
        }
    }

    Ok(parsed)
}

#[binrw::writer(writer, endian)]
fn write_string_map(s: &BTreeMap<String, String>) -> BinResult<()> {
    // Ugh: figure out total number of bytes to write:
    let mut byte_len = 0;
    for (k, v) in s {
        byte_len += 8; // Four bytes each for lengths of key and value
        byte_len += k.len();
        byte_len += v.len();
    }

    (byte_len as u32).write_options(writer, endian, ())?;
    let pos = writer.stream_position()?;

    for (k, v) in s {
        write_string(k, writer, endian, ())?;
        write_string(v, writer, endian, ())?;
    }
    assert_eq!(writer.stream_position()?, pos + byte_len as u64);
    Ok(())
}

#[binrw::writer(writer, endian)]
fn write_int_map<K: BinWrite<Args<'static> = ()>, V: BinWrite<Args<'static> = ()>>(
    s: &BTreeMap<K, V>,
) -> BinResult<()> {
    // Ugh: figure out total number of bytes to write:
    let mut byte_len = 0;
    for _ in s.values() {
        // Hack: We're assuming serialized size of the value is its in-memory size.
        // For ints of all flavors, this should be true.
        byte_len += core::mem::size_of::<K>();
        byte_len += core::mem::size_of::<V>();
    }

    (byte_len as u32).write_options(writer, endian, ())?;
    let pos = writer.stream_position()?;

    for (k, v) in s {
        k.write_options(writer, endian, ())?;
        v.write_options(writer, endian, ())?;
    }
    assert_eq!(writer.stream_position()?, pos + byte_len as u64);
    Ok(())
}

#[binrw::parser(reader, endian)]
fn parse_int_map<K: BinRead<Args<'static> = ()> + std::cmp::Ord, V: BinRead<Args<'static> = ()>>(
) -> BinResult<BTreeMap<K, V>> {
    let mut parsed = BTreeMap::new();

    // Length of the map in BYTES, not records.
    let byte_len: u32 = BinRead::read_options(reader, endian, ())?;
    let pos = reader.stream_position()?;

    while (reader.stream_position()? - pos) < byte_len as u64 {
        let k = K::read_options(reader, endian, ())?;
        let v = V::read_options(reader, endian, ())?;
        if let Some(_prev) = parsed.insert(k, v) {
            return Err(binrw::Error::Custom {
                pos,
                err: Box::new("Duplicate keys in map"),
            });
        }
    }

    Ok(parsed)
}

#[derive(Debug, Clone, Eq, PartialEq, BinRead, BinWrite)]
pub struct Channel {
    pub id: u16,
    pub schema_id: u16,

    #[br(map = |s: McapString| s.inner )]
    #[bw(write_with = write_string)]
    pub topic: String,

    #[br(map = |s: McapString| s.inner )]
    #[bw(write_with = write_string)]
    pub message_encoding: String,

    #[br(parse_with = parse_string_map)]
    #[bw(write_with = write_string_map)]
    pub metadata: BTreeMap<String, String>,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, BinRead, BinWrite)]
pub struct MessageHeader {
    pub channel_id: u16,
    pub sequence: u32,

    pub log_time: u64,

    pub publish_time: u64,
}

impl MessageHeader {
    pub(crate) fn serialized_len(&self) -> u64 {
        2 + 4 + 8 + 8
    }
}

#[derive(Debug, Clone, Eq, PartialEq, BinRead, BinWrite)]
pub struct ChunkHeader {
    pub message_start_time: u64,

    pub message_end_time: u64,

    pub uncompressed_size: u64,

    pub uncompressed_crc: u32,

    #[br(map = |s: McapString| s.inner )]
    #[bw(write_with = write_string)]
    pub compression: String,

    pub compressed_size: u64,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, BinRead, BinWrite)]
pub struct MessageIndexEntry {
    pub log_time: u64,

    pub offset: u64,
}

#[derive(Debug, Clone, Eq, PartialEq, BinRead, BinWrite)]
pub struct MessageIndex {
    pub channel_id: u16,

    #[br(parse_with = parse_vec)]
    #[bw(write_with = write_vec)]
    pub records: Vec<MessageIndexEntry>,
}

#[derive(Debug, Clone, Eq, PartialEq, BinRead, BinWrite)]
pub struct ChunkIndex {
    pub message_start_time: u64,

    pub message_end_time: u64,

    pub chunk_start_offset: u64,

    pub chunk_length: u64,

    #[br(parse_with = parse_int_map)]
    #[bw(write_with = write_int_map)]
    pub message_index_offsets: BTreeMap<u16, u64>,

    pub message_index_length: u64,

    #[br(map = |s: McapString| s.inner )]
    #[bw(write_with = write_string)]
    pub compression: String,

    pub compressed_size: u64,

    pub uncompressed_size: u64,
}

impl ChunkIndex {
    /// Returns the offset in the file to the start of compressed chunk data.
    /// This can be useful for retrieving just the compressed content of a chunk given its index.
    /// Returns [`McapError::TooLong`] if the resulting offset would be greater than [`u64::MAX`].
    pub fn compressed_data_offset(&self) -> McapResult<u64> {
        let res = self.chunk_start_offset.checked_add(
            1 // opcode
            + 8 // chunk record length
            + 8 // start time
            + 8 // end time
            + 8 // uncompressed size
            + 4 // CRC
            + 4 // compression string length
            + (self.compression.len() as u64) // 32-bit compression string length
            + 8, // compressed size
        );
        match res {
            Some(n) => Ok(n),
            None => Err(McapError::BadChunkStartOffset(self.chunk_start_offset)),
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, BinRead, BinWrite)]
pub struct AttachmentHeader {
    pub log_time: u64,

    pub create_time: u64,

    #[br(map = |s: McapString| s.inner )]
    #[bw(write_with = write_string)]
    pub name: String,

    #[br(map = |s: McapString| s.inner )]
    #[bw(write_with = write_string)]
    pub media_type: String,
}

#[derive(Debug, Clone, Eq, PartialEq, BinRead, BinWrite)]
pub struct AttachmentIndex {
    pub offset: u64,

    pub length: u64,

    pub log_time: u64,

    pub create_time: u64,

    pub data_size: u64,

    #[br(map = |s: McapString| s.inner )]
    #[bw(write_with = write_string)]
    pub name: String,

    #[br(map = |s: McapString| s.inner )]
    #[bw(write_with = write_string)]
    pub media_type: String,
}

#[derive(Debug, Default, Clone, Eq, PartialEq, BinRead, BinWrite)]
pub struct Statistics {
    pub message_count: u64,
    pub schema_count: u16,
    pub channel_count: u32,
    pub attachment_count: u32,
    pub metadata_count: u32,
    pub chunk_count: u32,

    pub message_start_time: u64,

    pub message_end_time: u64,

    #[br(parse_with = parse_int_map)]
    #[bw(write_with = write_int_map)]
    pub channel_message_counts: BTreeMap<u16, u64>,
}

#[derive(Debug, Clone, Eq, PartialEq, BinRead, BinWrite)]
pub struct Metadata {
    #[br(map = |s: McapString| s.inner )]
    #[bw(write_with = write_string)]
    pub name: String,

    #[br(parse_with = parse_string_map)]
    #[bw(write_with = write_string_map)]
    pub metadata: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Eq, PartialEq, BinRead, BinWrite)]
pub struct MetadataIndex {
    pub offset: u64,

    pub length: u64,

    #[br(map = |s: McapString| s.inner )]
    #[bw(write_with = write_string)]
    pub name: String,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, BinRead, BinWrite)]
pub struct SummaryOffset {
    pub group_opcode: u8,
    pub group_start: u64,
    pub group_length: u64,
}

#[derive(Debug, Default, Clone, Copy, Eq, PartialEq, BinRead, BinWrite)]
pub struct DataEnd {
    pub data_section_crc: u32,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn string_parse() {
        let ms: McapString = Cursor::new(b"\x04\0\0\0abcd").read_le().unwrap();
        assert_eq!(
            ms,
            McapString {
                inner: String::from("abcd")
            }
        );

        assert!(Cursor::new(b"\x05\0\0\0abcd")
            .read_le::<McapString>()
            .is_err());

        let mut written = Vec::new();
        Cursor::new(&mut written)
            .write_le(&McapString {
                inner: String::from("hullo"),
            })
            .unwrap();
        assert_eq!(&written, b"\x05\0\0\0hullo");
    }

    #[test]
    fn header_parse() {
        let expected = b"\x04\0\0\0abcd\x03\0\0\x00123";

        let h: Header = Cursor::new(expected).read_le().unwrap();
        assert_eq!(h.profile, "abcd");
        assert_eq!(h.library, "123");

        let mut written = Vec::new();
        Cursor::new(&mut written).write_le(&h).unwrap();
        assert_eq!(written, expected);
    }

    #[test]
    fn test_message_header_len() {
        let header = MessageHeader {
            sequence: 1,
            log_time: 2,
            channel_id: 3,
            publish_time: 4,
        };

        let len = header.serialized_len();

        let mut buf = vec![];
        Cursor::new(&mut buf).write_le(&header).unwrap();

        assert_eq!(len as usize, buf.len());
    }
}
