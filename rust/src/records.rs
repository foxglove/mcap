//! Raw records parsed from an MCAP file
//!
//! See <https://github.com/foxglove/mcap/tree/main/docs/specification>
//!
//! You probably want to user higher-level interfaces, like
//! [`Message`](crate::Message), [`Channel`](crate::Channel), and [`Schema`](crate::Schema),
//! read from iterators like [`MesssageStream`](crate::MessageStream).

use std::{
    borrow::Cow,
    collections::BTreeMap,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use binrw::io::{Read, Seek, Write};
use binrw::*;
use serde::{Deserialize, Serialize};

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
    pub const END_OF_DATA: u8 = 0x0F;
}

/// A raw record from an MCAP file.
///
/// For records with large slices of binary data (schemas, messages, chunks...),
/// we use a [`CoW`](std::borrow::Cow) that can either borrow directly from the mapped file,
/// or hold its own buffer if it was decompressed from a chunk.
#[derive(Debug, Serialize, Deserialize)]
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
        data: &'a [u8],
    },
    MessageIndex(MessageIndex),
    ChunkIndex(ChunkIndex),
    Attachment {
        header: AttachmentHeader,
        data: &'a [u8],
    },
    AttachmentIndex(AttachmentIndex),
    Statistics(Statistics),
    Metadata(Metadata),
    MetadataIndex(MetadataIndex),
    SummaryOffset(SummaryOffset),
    EndOfData(EndOfData),
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
            Record::EndOfData(_) => op::END_OF_DATA,
            Record::Unknown { opcode, .. } => *opcode,
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
fn write_string<W: binrw::io::Write + binrw::io::Seek>(
    s: &String,
    w: &mut W,
    opts: &WriteOptions,
    args: (),
) -> BinResult<()> {
    (s.len() as u32).write_options(w, opts, args)?;
    (s.as_bytes()).write_options(w, opts, args)?;
    Ok(())
}

fn parse_vec<T: binrw::BinRead<Args = ()>, R: Read + Seek>(
    reader: &mut R,
    ro: &ReadOptions,
    args: (),
) -> BinResult<Vec<T>> {
    let mut parsed = Vec::new();

    // Length of the map in BYTES, not records.
    let byte_len: u32 = BinRead::read_options(reader, ro, args)?;
    let pos = reader.stream_position()?;

    while (reader.stream_position()? - pos) < byte_len as u64 {
        parsed.push(T::read_options(reader, ro, args)?);
    }

    Ok(parsed)
}

#[allow(clippy::ptr_arg)] // needed to match binrw macros
fn write_vec<W: binrw::io::Write + binrw::io::Seek, T: binrw::BinWrite<Args = ()>>(
    v: &Vec<T>,
    w: &mut W,
    opts: &WriteOptions,
    args: (),
) -> BinResult<()> {
    use std::io::SeekFrom;

    let start = w.stream_position()?;
    (!0u32).write_options(w, opts, args)?; // Revisit...
    for e in v.iter() {
        e.write_options(w, opts, args)?;
    }
    let end = w.stream_position()?;
    let data_len = end - start - 4;
    w.seek(SeekFrom::Start(start))?;
    (data_len as u32).write_options(w, opts, args)?;
    assert_eq!(w.seek(SeekFrom::End(0))?, end);
    Ok(())
}

#[derive(Debug, Clone, Eq, PartialEq, BinRead, BinWrite, Serialize, Deserialize)]
pub struct Header {
    #[br(map = |s: McapString| s.inner )]
    #[bw(write_with = write_string)]
    pub profile: String,

    #[br(map = |s: McapString| s.inner )]
    #[bw(write_with = write_string)]
    pub library: String,
}

#[derive(Debug, Default, Clone, Copy, Eq, PartialEq, BinRead, BinWrite, Serialize, Deserialize)]
pub struct Footer {
    pub summary_start: u64,
    pub summary_offset_start: u64,
    pub summary_crc: u32,
}

#[derive(Debug, Clone, Eq, PartialEq, BinRead, BinWrite, Serialize, Deserialize)]
pub struct SchemaHeader {
    pub id: u16,

    #[br(map = |s: McapString| s.inner )]
    #[bw(write_with = write_string)]
    pub name: String,

    #[br(map = |s: McapString| s.inner )]
    #[bw(write_with = write_string)]
    pub encoding: String,

    pub data_len: u32,
}

fn parse_string_map<R: Read + Seek>(
    reader: &mut R,
    ro: &ReadOptions,
    args: (),
) -> BinResult<BTreeMap<String, String>> {
    let mut parsed = BTreeMap::new();

    // Length of the map in BYTES, not records.
    let byte_len: u32 = BinRead::read_options(reader, ro, args)?;
    let pos = reader.stream_position()?;

    while (reader.stream_position()? - pos) < byte_len as u64 {
        let k = McapString::read_options(reader, ro, args)?;
        let v = McapString::read_options(reader, ro, args)?;
        if let Some(_prev) = parsed.insert(k.inner, v.inner) {
            return Err(binrw::Error::Custom {
                pos,
                err: Box::new("Duplicate keys in map"),
            });
        }
    }

    Ok(parsed)
}

fn write_string_map<W: Write + Seek>(
    s: &BTreeMap<String, String>,
    w: &mut W,
    opts: &WriteOptions,
    args: (),
) -> BinResult<()> {
    // Ugh: figure out total number of bytes to write:
    let mut byte_len = 0;
    for (k, v) in s {
        byte_len += 8; // Four bytes each for lengths of key and value
        byte_len += k.len();
        byte_len += v.len();
    }

    (byte_len as u32).write_options(w, opts, args)?;
    let pos = w.stream_position()?;

    for (k, v) in s {
        write_string(k, w, opts, args)?;
        write_string(v, w, opts, args)?;
    }
    assert_eq!(w.stream_position()?, pos + byte_len as u64);
    Ok(())
}

fn write_int_map<K: BinWrite<Args = ()>, V: BinWrite<Args = ()>, W: Write + Seek>(
    s: &BTreeMap<K, V>,
    w: &mut W,
    opts: &WriteOptions,
    args: (),
) -> BinResult<()> {
    // Ugh: figure out total number of bytes to write:
    let mut byte_len = 0;
    for _ in s.values() {
        // Hack: We're assuming serialized size of the value is its in-memory size.
        // For ints of all flavors, this should be true.
        byte_len += core::mem::size_of::<K>();
        byte_len += core::mem::size_of::<V>();
    }

    (byte_len as u32).write_options(w, opts, args)?;
    let pos = w.stream_position()?;

    for (k, v) in s {
        k.write_options(w, opts, args)?;
        v.write_options(w, opts, args)?;
    }
    assert_eq!(w.stream_position()?, pos + byte_len as u64);
    Ok(())
}

fn parse_int_map<K, V, R>(reader: &mut R, ro: &ReadOptions, args: ()) -> BinResult<BTreeMap<K, V>>
where
    K: BinRead<Args = ()> + std::cmp::Ord,
    V: BinRead<Args = ()>,
    R: Read + Seek,
{
    let mut parsed = BTreeMap::new();

    // Length of the map in BYTES, not records.
    let byte_len: u32 = BinRead::read_options(reader, ro, args)?;
    let pos = reader.stream_position()?;

    while (reader.stream_position()? - pos) < byte_len as u64 {
        let k = K::read_options(reader, ro, args)?;
        let v = V::read_options(reader, ro, args)?;
        if let Some(_prev) = parsed.insert(k, v) {
            return Err(binrw::Error::Custom {
                pos,
                err: Box::new("Duplicate keys in map"),
            });
        }
    }

    Ok(parsed)
}

#[derive(Debug, Clone, Eq, PartialEq, BinRead, BinWrite, Serialize, Deserialize)]
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

pub fn system_time_to_nanos(d: &SystemTime) -> u64 {
    let ns = d.duration_since(UNIX_EPOCH).unwrap().as_nanos();
    assert!(ns <= u64::MAX as u128);
    ns as u64
}

pub fn nanos_to_system_time(n: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_nanos(n)
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, BinRead, BinWrite, Serialize, Deserialize)]
pub struct MessageHeader {
    pub channel_id: u16,
    pub sequence: u32,

    pub log_time: u64,

    pub publish_time: u64,
}

#[derive(Debug, Clone, Eq, PartialEq, BinRead, BinWrite, Serialize, Deserialize)]
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

#[derive(Debug, Clone, Copy, Eq, PartialEq, BinRead, BinWrite, Serialize, Deserialize)]
pub struct MessageIndexEntry {
    pub log_time: u64,

    pub offset: u64,
}

#[derive(Debug, Clone, Eq, PartialEq, BinRead, BinWrite, Serialize, Deserialize)]
pub struct MessageIndex {
    pub channel_id: u16,

    #[br(parse_with = parse_vec)]
    #[bw(write_with = write_vec)]
    pub records: Vec<MessageIndexEntry>,
}

#[derive(Debug, Clone, Eq, PartialEq, BinRead, BinWrite, Serialize, Deserialize)]
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

#[derive(Debug, Clone, Eq, PartialEq, BinRead, BinWrite, Serialize, Deserialize)]
pub struct AttachmentHeader {
    pub log_time: u64,

    pub create_time: u64,

    #[br(map = |s: McapString| s.inner )]
    #[bw(write_with = write_string)]
    pub name: String,

    #[br(map = |s: McapString| s.inner )]
    #[bw(write_with = write_string)]
    pub content_type: String,

    pub data_len: u64,
}

#[derive(Debug, Clone, Eq, PartialEq, BinRead, BinWrite, Serialize, Deserialize)]
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
    pub content_type: String,
}

#[derive(Debug, Default, Clone, Eq, PartialEq, BinRead, BinWrite, Serialize, Deserialize)]
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

#[derive(Debug, Clone, Eq, PartialEq, BinRead, BinWrite, Serialize, Deserialize)]
pub struct Metadata {
    #[br(map = |s: McapString| s.inner )]
    #[bw(write_with = write_string)]
    pub name: String,

    #[br(parse_with = parse_string_map)]
    #[bw(write_with = write_string_map)]
    pub metadata: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Eq, PartialEq, BinRead, BinWrite, Serialize, Deserialize)]
pub struct MetadataIndex {
    pub offset: u64,

    pub length: u64,

    #[br(map = |s: McapString| s.inner )]
    #[bw(write_with = write_string)]
    pub name: String,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, BinRead, BinWrite, Serialize, Deserialize)]
pub struct SummaryOffset {
    pub group_opcode: u8,
    pub group_start: u64,
    pub group_length: u64,
}

#[derive(Debug, Default, Clone, Copy, Eq, PartialEq, BinRead, BinWrite, Serialize, Deserialize)]
pub struct EndOfData {
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
}
