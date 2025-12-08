//! A library for manipulating [Foxglove MCAP](https://github.com/foxglove/mcap) files,
//! both reading:
//!
//! ```no_run
//! use std::fs;
//!
//! use anyhow::{Context, Result};
//! use camino::Utf8Path;
//! use memmap2::Mmap;
//!
//! fn map_mcap<P: AsRef<Utf8Path>>(p: P) -> Result<Mmap> {
//!     let fd = fs::File::open(p.as_ref()).context("Couldn't open MCAP file")?;
//!     unsafe { Mmap::map(&fd) }.context("Couldn't map MCAP file")
//! }
//!
//! fn read_it() -> Result<()> {
//!     let mapped = map_mcap("in.mcap")?;
//!
//!     for message in mcap::MessageStream::new(&mapped)? {
//!         println!("{:?}", message?);
//!         // Or whatever else you'd like to do...
//!     }
//!     Ok(())
//! }
//! ```
//! or writing:
//! ```no_run
//! use std::{collections::BTreeMap, fs, io::BufWriter};
//!
//! use anyhow::Result;
//!
//! use mcap::{Channel, records::MessageHeader, Writer};
//!
//! fn write_it() -> Result<()> {
//!     // To set the profile or compression options, see mcap::WriteOptions.
//!     let mut out = Writer::new(
//!         BufWriter::new(fs::File::create("out.mcap")?)
//!     )?;
//!
//!     // Channels and schemas are automatically assigned ID as they're serialized,
//!     // and automatically deduplicated with `Arc` when deserialized.
//!     let channel_id = out.add_channel(0, "cool stuff", "application/octet-stream", &BTreeMap::new())?;
//!
//!     out.write_to_known_channel(
//!         &MessageHeader {
//!             channel_id,
//!             sequence: 25,
//!             log_time: 6,
//!             publish_time: 24
//!         },
//!         &[1, 2, 3]
//!     )?;
//!     out.write_to_known_channel(
//!         &MessageHeader {
//!             channel_id,
//!             sequence: 32,
//!             log_time: 23,
//!             publish_time: 25
//!         },
//!         &[3, 4, 5]
//!     )?;
//!
//!     out.finish()?;
//!
//!     Ok(())
//! }
//! ```

pub mod read;
pub mod records;
#[cfg(feature = "tokio")]
pub mod tokio;
pub mod write;

mod chunk_sink;
mod io_utils;
pub mod sans_io;

use std::{borrow::Cow, collections::BTreeMap, fmt, sync::Arc};

use thiserror::Error;

#[derive(Debug, Error)]
pub enum McapError {
    #[error("tried to write bytes to an attachment but no attachment was in progress")]
    AttachmentNotInProgress,
    #[error("tried to write {excess} more bytes to attachment than the requested attachment length {attachment_length}")]
    AttachmentTooLarge { excess: u64, attachment_length: u64 },
    #[error("tried to finish writing attachment but current length {current} was not expected length {expected}")]
    AttachmentIncomplete { current: u64, expected: u64 },
    #[error("Bad magic number")]
    BadMagic,
    #[error("Footer record couldn't be found at the end of the file, before the magic bytes")]
    BadFooter,
    #[error("Attachment CRC failed (expected {saved:08X}, got {calculated:08X}")]
    BadAttachmentCrc { saved: u32, calculated: u32 },
    #[error("Chunk CRC failed (expected {saved:08X}, got {calculated:08X})")]
    BadChunkCrc { saved: u32, calculated: u32 },
    #[error("Data section CRC failed (expected {saved:08X}, got {calculated:08X})")]
    BadDataCrc { saved: u32, calculated: u32 },
    #[error("Summary section CRC failed (expected {saved:08X}, got {calculated:08X})")]
    BadSummaryCrc { saved: u32, calculated: u32 },
    #[error("Index offset and length didn't point to the expected record type")]
    BadIndex,
    #[error("Attachment length ({header}) exceeds space in record ({available})")]
    BadAttachmentLength { header: u64, available: u64 },
    #[error("Chunk length ({header}) exceeds space in record ({available})")]
    BadChunkLength { header: u64, available: u64 },
    #[error("Schema length ({header}) exceeds space in record ({available})")]
    BadSchemaLength { header: u32, available: u32 },
    #[error("Private records must have an opcode >= 0x80, got {opcode:#04x}")]
    PrivateRecordOpcodeIsReserved { opcode: u8 },
    #[error("Channel `{0}` has multiple records that don't match.")]
    ConflictingChannels(String),
    #[error("Schema `{0}` has multiple records that don't match.")]
    ConflictingSchemas(String),
    #[error("Record parse failed")]
    Parse(#[from] binrw::Error),
    #[error("I/O error from writing, or reading a compression stream")]
    Io(#[from] std::io::Error),
    #[error("Schema has an ID of 0")]
    InvalidSchemaId,
    #[error("MCAP file ended in the middle of a record")]
    UnexpectedEof,
    #[error("Chunk ended in the middle of a record")]
    UnexpectedEoc,
    #[error("Message {0} referenced unknown channel {1}")]
    UnknownChannel(u32, u16),
    #[error("Channel `{0}` referenced unknown schema {1}")]
    UnknownSchema(String, u16),
    #[error("Unsupported compression format `{0}`")]
    UnsupportedCompression(String),
    #[error("Error during decompression: `{0}`")]
    DecompressionError(String),
    #[error("chunk size option exceeds usize max: `{0}`")]
    ChunkBufferTooLarge(u64),
    #[error("record with opcode {opcode:02x} length exceeds limit: `{len}`")]
    RecordTooLarge { opcode: u8, len: u64 },
    #[error("chunk (de)compressed length exceeds limit: `{0}`")]
    ChunkTooLarge(u64),
    #[error("chunk start offset is out of file range: {0}")]
    BadChunkStartOffset(u64),
    #[error("cannot write more than 65536 channels to one MCAP")]
    TooManyChannels,
    #[error("cannot write more than 65535 schemas to one MCAP")]
    TooManySchemas,
    #[error("indexed reader received chunk data with unexpected offset or length")]
    UnexpectedChunkDataInserted,
    #[error("attempted another write after a write method failed")]
    AttemptedWriteAfterFailure,
    #[error("file has more bytes after end magic")]
    BytesAfterEndMagic,
}

pub type McapResult<T> = Result<T, McapError>;

/// Magic bytes for the MCAP format
pub const MAGIC: &[u8] = &[0x89, b'M', b'C', b'A', b'P', 0x30, b'\r', b'\n'];

/// Compression options for chunks of channels, schemas, and messages in an MCAP file
#[derive(Debug, Copy, Clone)]
pub enum Compression {
    #[cfg(feature = "zstd")]
    Zstd,
    #[cfg(feature = "lz4")]
    Lz4,
}

/// Describes a schema used by one or more [Channel]s in an MCAP file
///
/// The [`CoW`](std::borrow::Cow) can either borrow directly from the mapped file,
/// or hold its own buffer if it was decompressed from a chunk.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct Schema<'a> {
    pub id: u16,
    pub name: String,
    pub encoding: String,
    pub data: Cow<'a, [u8]>,
}

impl fmt::Debug for Schema<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Schema")
            .field("name", &self.name)
            .field("encoding", &self.encoding)
            .finish_non_exhaustive()
    }
}

/// Describes a channel which [Message]s are published to in an MCAP file
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Channel<'a> {
    pub id: u16,
    pub topic: String,
    pub schema: Option<Arc<Schema<'a>>>,

    pub message_encoding: String,
    pub metadata: BTreeMap<String, String>,
}

/// An event in an MCAP file, published to a [Channel]
///
/// The [`CoW`](std::borrow::Cow) can either borrow directly from the mapped file,
/// or hold its own buffer if it was decompressed from a chunk.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Message<'a> {
    pub channel: Arc<Channel<'a>>,
    pub sequence: u32,
    pub log_time: u64,
    pub publish_time: u64,
    pub data: Cow<'a, [u8]>,
}

/// An attachment and its metadata in an MCAP file
#[derive(Debug, PartialEq, Eq)]
pub struct Attachment<'a> {
    pub log_time: u64,
    pub create_time: u64,
    pub name: String,
    pub media_type: String,
    pub data: Cow<'a, [u8]>,
}

pub use read::{parse_record, MessageStream, Summary};
pub use write::{WriteOptions, Writer};

// The following assertions ensure that the MCAP components can be sent between threads.
mod assertions {
    use super::*;
    use static_assertions::assert_impl_all;
    use std::io::Cursor;

    assert_impl_all!(Writer<Cursor<Vec<u8>>>: Send);
    assert_impl_all!(MessageStream: Send);
    assert_impl_all!(sans_io::LinearReader: Send);
    #[cfg(feature = "tokio")]
    assert_impl_all!(tokio::linear_reader::LinearReader<Cursor<Vec<u8>>>: Send);
}
