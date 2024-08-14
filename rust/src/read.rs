//! Read MCAP files
//!
//! MCAPs are read from a byte slice instead of a [`Read`] trait object.
//! This helps us avoid unnecessary copies, since [`Schema`]s and [`Message`]s
//! can refer directly to their data.
//!
//! Consider [memory-mapping](https://docs.rs/memmap/0.7.0/memmap/struct.Mmap.html)
//! the file - the OS will load (and cache!) it on-demand, without any
//! further system calls.
use std::{
    borrow::Cow,
    collections::{BTreeMap, HashMap},
    fmt,
    io::{self, prelude::*, Cursor},
    mem::size_of,
    sync::Arc,
};

use binrw::prelude::*;
use byteorder::{ReadBytesExt, LE};
use crc32fast::hash as crc32;
use enumset::{enum_set, EnumSet, EnumSetType};
use log::*;

use crate::{
    io_utils::CountingCrcReader,
    records::{self, op, Record},
    Attachment, Channel, McapError, McapResult, Message, Schema, MAGIC,
};

/// Nonstandard reading options, e.g.,
/// to be more lenient when trying to recover incomplete/damaged files.
///
/// More may be added in future releases.
#[derive(EnumSetType, Debug)]
pub enum Options {
    /// Don't require the MCAP file to end with its magic bytes.
    IgnoreEndMagic,
}

/// Scans a mapped MCAP file from start to end, returning each record.
///
/// You probably want a [MessageStream] instead - this yields the raw records
/// from the file without any postprocessing (decompressing chunks, etc.)
/// and is mostly meant as a building block for higher-level readers.
pub struct LinearReader<'a> {
    buf: &'a [u8],
    malformed: bool,
}

impl<'a> LinearReader<'a> {
    /// Create a reader for the given file,
    /// checking [`MAGIC`] bytes on both ends.
    pub fn new(buf: &'a [u8]) -> McapResult<Self> {
        Self::new_with_options(buf, enum_set!())
    }

    /// Create a reader for the given file with special options.
    pub fn new_with_options(buf: &'a [u8], options: EnumSet<Options>) -> McapResult<Self> {
        if !buf.starts_with(MAGIC)
            || (!options.contains(Options::IgnoreEndMagic)
                && (!buf.ends_with(MAGIC) || buf.len() < 2 * MAGIC.len()))
        {
            return Err(McapError::BadMagic);
        }
        let buf = &buf[MAGIC.len()..];
        if buf.ends_with(MAGIC) {
            Ok(Self::sans_magic(&buf[0..buf.len() - MAGIC.len()]))
        } else {
            Ok(Self::sans_magic(buf))
        }
    }

    /// Like [`new()`](Self::new), but assumes `buf` has the magic bytes sliced off.
    ///
    /// Useful for iterating through slices of an MCAP file instead of the whole thing.
    pub fn sans_magic(buf: &'a [u8]) -> Self {
        Self {
            buf,
            malformed: false,
        }
    }

    /// Returns the number of unprocessed bytes
    /// (sans the file's starting and ending magic)
    ///
    /// Used to calculate offsets for the data section et al.
    fn bytes_remaining(&self) -> usize {
        self.buf.len()
    }
}

impl<'a> Iterator for LinearReader<'a> {
    type Item = McapResult<records::Record<'a>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.buf.is_empty() {
            return None;
        }

        // After an unrecoverable error (due to something wonky in the file),
        // don't keep trying to walk it.
        if self.malformed {
            return None;
        }

        let record = match read_record_from_slice(&mut self.buf) {
            Ok(k) => k,
            Err(e) => {
                self.malformed = true;
                return Some(Err(e));
            }
        };

        Some(Ok(record))
    }
}

/// Read a record and advance the slice
fn read_record_from_slice<'a>(buf: &mut &'a [u8]) -> McapResult<records::Record<'a>> {
    if buf.len() < (size_of::<u64>() + size_of::<u8>()) {
        warn!("Malformed MCAP - not enough space for record + length!");
        return Err(McapError::UnexpectedEof);
    }

    let op = read_u8(buf);
    let len = read_u64(buf);

    if buf.len() < len as usize {
        warn!(
            "Malformed MCAP - record with length {len}, but only {} bytes remain",
            buf.len()
        );
        return Err(McapError::UnexpectedEof);
    }

    let body = &buf[..len as usize];
    debug!("slice: opcode {op:02X}, length {len}");
    let record = read_record(op, body)?;
    trace!("       {:?}", record);

    *buf = &buf[len as usize..];
    Ok(record)
}

/// Given a record's opcode and its slice, read it into a [Record]
fn read_record(op: u8, body: &[u8]) -> McapResult<records::Record<'_>> {
    macro_rules! record {
        ($b:ident) => {{
            let mut cur = Cursor::new($b);
            let res = cur.read_le()?;
            res
        }};
    }

    Ok(match op {
        op::HEADER => Record::Header(record!(body)),
        op::FOOTER => Record::Footer(record!(body)),
        op::SCHEMA => {
            let mut c = Cursor::new(body);
            let header: records::SchemaHeader = c.read_le()?;
            let data_len = c.read_u32::<LE>()?;
            let mut data = &body[c.position() as usize..];

            if data_len > data.len() as u32 {
                return Err(McapError::BadSchemaLength {
                    header: data_len,
                    available: data.len() as u32,
                });
            }
            data = &data[..data_len as usize];
            Record::Schema {
                header,
                data: Cow::Borrowed(data),
            }
        }
        op::CHANNEL => Record::Channel(record!(body)),
        op::MESSAGE => {
            let mut c = Cursor::new(body);
            let header = c.read_le()?;
            let data = Cow::Borrowed(&body[c.position() as usize..]);
            Record::Message { header, data }
        }
        op::CHUNK => {
            let mut c = Cursor::new(body);
            let header: records::ChunkHeader = c.read_le()?;
            let mut data = &body[c.position() as usize..];
            if header.compressed_size > data.len() as u64 {
                return Err(McapError::BadChunkLength {
                    header: header.compressed_size,
                    available: data.len() as u64,
                });
            }
            data = &data[..header.compressed_size as usize];
            Record::Chunk {
                header,
                data: Cow::Borrowed(data),
            }
        }
        op::MESSAGE_INDEX => Record::MessageIndex(record!(body)),
        op::CHUNK_INDEX => Record::ChunkIndex(record!(body)),
        op::ATTACHMENT => {
            let mut c = Cursor::new(body);
            let header: records::AttachmentHeader = c.read_le()?;
            let data_len = c.read_u64::<LE>()?;
            let header_len = c.position() as usize;

            let mut data = &body[header_len..body.len() - 4];
            if data_len > data.len() as u64 {
                return Err(McapError::BadAttachmentLength {
                    header: data_len,
                    available: data.len() as u64,
                });
            }
            data = &data[..data_len as usize];
            let crc: u32 = Cursor::new(&body[header_len + data.len()..]).read_le()?;

            // We usually leave CRCs to higher-level readers -
            // (ChunkReader, read_summary(), etc.) - but
            //
            // 1. We can trivially check it here without checking other records,
            //    decompressing anything, or doing any other non-trivial work
            //
            // 2. Since the CRC depends on the serialized header, it doesn't make
            //    much sense to have users check it.
            //    (What would they do? lol reserialize the header?)
            if crc != 0 {
                let calculated = crc32(&body[..header_len + data.len()]);
                if crc != calculated {
                    return Err(McapError::BadAttachmentCrc {
                        saved: crc,
                        calculated,
                    });
                }
            }

            Record::Attachment {
                header,
                data: Cow::Borrowed(data),
            }
        }
        op::ATTACHMENT_INDEX => Record::AttachmentIndex(record!(body)),
        op::STATISTICS => Record::Statistics(record!(body)),
        op::METADATA => Record::Metadata(record!(body)),
        op::METADATA_INDEX => Record::MetadataIndex(record!(body)),
        op::SUMMARY_OFFSET => Record::SummaryOffset(record!(body)),
        op::DATA_END => Record::DataEnd(record!(body)),
        opcode => Record::Unknown {
            opcode,
            data: Cow::Borrowed(body),
        },
    })
}

enum ChunkDecompressor<'a> {
    Null(LinearReader<'a>),
    /// This is not used when both `zstd` and `lz4` features are disabled.
    #[allow(dead_code)]
    Compressed(Option<CountingCrcReader<Box<dyn Read + Send + 'a>>>),
}

/// Streams records out of a [Chunk](Record::Chunk), decompressing as needed.
pub struct ChunkReader<'a> {
    header: records::ChunkHeader,
    decompressor: ChunkDecompressor<'a>,
}

impl<'a> ChunkReader<'a> {
    pub fn new(header: records::ChunkHeader, data: &'a [u8]) -> McapResult<Self> {
        let decompressor = match header.compression.as_str() {
            #[cfg(feature = "zstd")]
            "zstd" => ChunkDecompressor::Compressed(Some(CountingCrcReader::new(Box::new(
                zstd::Decoder::new(data)?,
            )))),

            #[cfg(not(feature = "zstd"))]
            "zstd" => panic!("Unsupported compression format: zstd"),

            #[cfg(feature = "lz4")]
            "lz4" => ChunkDecompressor::Compressed(Some(CountingCrcReader::new(Box::new(
                lz4_flex::frame::FrameDecoder::new(data),
            )))),

            #[cfg(not(feature = "lz4"))]
            "lz4" => panic!("Unsupported compression format: lz4"),

            "" => {
                if header.uncompressed_size != header.compressed_size {
                    warn!(
                        "Chunk is uncompressed, but claims different compress/uncompressed lengths"
                    );
                }

                if header.uncompressed_crc != 0 {
                    let calculated = crc32(data);
                    if header.uncompressed_crc != calculated {
                        return Err(McapError::BadChunkCrc {
                            saved: header.uncompressed_crc,
                            calculated,
                        });
                    }
                }

                ChunkDecompressor::Null(LinearReader::sans_magic(data))
            }
            wat => return Err(McapError::UnsupportedCompression(wat.to_string())),
        };

        Ok(Self {
            header,
            decompressor,
        })
    }
}

impl<'a> Iterator for ChunkReader<'a> {
    type Item = McapResult<records::Record<'a>>;

    fn next(&mut self) -> Option<Self::Item> {
        match &mut self.decompressor {
            ChunkDecompressor::Null(r) => r.next(),
            ChunkDecompressor::Compressed(stream) => {
                // If we consumed the stream last time to get the CRC,
                // or because of an error, we're done.
                if stream.is_none() {
                    return None;
                }

                let s = stream.as_mut().unwrap();

                let record = match read_record_from_chunk_stream(s) {
                    Ok(k) => k,
                    Err(e) => {
                        *stream = None; // Don't try to recover.
                        return Some(Err(e));
                    }
                };

                // If we've read all there is to read...
                if s.position() >= self.header.uncompressed_size {
                    // Get the CRC.
                    let calculated = stream.take().unwrap().finalize();

                    // If the header stored a CRC
                    // and it doesn't match what we have, complain.
                    if self.header.uncompressed_crc != 0
                        && self.header.uncompressed_crc != calculated
                    {
                        return Some(Err(McapError::BadChunkCrc {
                            saved: self.header.uncompressed_crc,
                            calculated,
                        }));
                    }
                    // All good!
                }

                Some(Ok(record))
            }
        }
    }
}

/// Like [read_record_from_slice], but for a decompression stream
fn read_record_from_chunk_stream<'a, R: Read>(r: &mut R) -> McapResult<records::Record<'a>> {
    let op = r.read_u8()?;
    let len = r.read_u64::<LE>()?;

    debug!("chunk: opcode {op:02X}, length {len}");
    let record = match op {
        op::SCHEMA => {
            let mut record = Vec::with_capacity(len as usize);
            r.take(len).read_to_end(&mut record)?;
            if len as usize != record.len() {
                return Err(McapError::UnexpectedEoc);
            }

            let mut c = Cursor::new(&record);
            let header: records::SchemaHeader = c.read_le()?;
            let data_len = c.read_u32::<LE>()?;

            let header_end = c.position();

            // Should we rotate and shrink instead?
            let mut data = record.split_off(header_end as usize);

            if data_len > data.len() as u32 {
                return Err(McapError::BadSchemaLength {
                    header: data_len,
                    available: data.len() as u32,
                });
            }
            data.truncate(data_len as usize);
            Record::Schema {
                header,
                data: Cow::Owned(data),
            }
        }
        op::CHANNEL => {
            let mut record = Vec::with_capacity(len as usize);
            r.take(len).read_to_end(&mut record)?;
            if len as usize != record.len() {
                return Err(McapError::UnexpectedEoc);
            }

            let mut c = Cursor::new(&record);
            let channel: records::Channel = c.read_le()?;

            if c.position() != record.len() as u64 {
                warn!(
                    "Channel {}'s length doesn't match its record length",
                    channel.topic
                );
            }

            Record::Channel(channel)
        }
        op::MESSAGE => {
            // Optimization: messages are the mainstay of the file,
            // so allocate the header and the data separately to avoid having
            // to split them up or move them around later.
            // Fortunately, message headers are fixed length.
            const HEADER_LEN: u64 = 22;

            let mut header_buf = Vec::with_capacity(HEADER_LEN as usize);
            r.take(HEADER_LEN).read_to_end(&mut header_buf)?;
            if header_buf.len() as u64 != HEADER_LEN {
                return Err(McapError::UnexpectedEoc);
            }
            let header: records::MessageHeader = Cursor::new(header_buf).read_le()?;

            let mut data = Vec::with_capacity((len - HEADER_LEN) as usize);
            r.take(len - HEADER_LEN).read_to_end(&mut data)?;
            if data.len() as u64 != len - HEADER_LEN {
                return Err(McapError::UnexpectedEoc);
            }

            Record::Message {
                header,
                data: Cow::Owned(data),
            }
        }
        wut => return Err(McapError::UnexpectedChunkRecord(wut)),
    };
    trace!("       {:?}", record);
    Ok(record)
}

/// Like [`LinearReader`], but unpacks chunks' records into its stream
pub struct ChunkFlattener<'a> {
    top_level: LinearReader<'a>,
    dechunk: Option<ChunkReader<'a>>,
    malformed: bool,
}

impl<'a> ChunkFlattener<'a> {
    pub fn new(buf: &'a [u8]) -> McapResult<Self> {
        Self::new_with_options(buf, enum_set!())
    }

    pub fn new_with_options(buf: &'a [u8], options: EnumSet<Options>) -> McapResult<Self> {
        let top_level = LinearReader::new_with_options(buf, options)?;
        Ok(Self {
            top_level,
            dechunk: None,
            malformed: false,
        })
    }

    fn bytes_remaining(&self) -> usize {
        self.top_level.bytes_remaining()
    }
}

impl<'a> Iterator for ChunkFlattener<'a> {
    type Item = McapResult<records::Record<'a>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.malformed {
            return None;
        }

        let n: Option<Self::Item> = loop {
            // If we're reading from a chunk, do that until it returns None.
            if let Some(d) = &mut self.dechunk {
                match d.next() {
                    Some(d) => break Some(d),
                    None => self.dechunk = None,
                }
            }
            // Fall through - if we didn't extract a record from a chunk
            // (or that chunk ended), move on to the next top-level record.
            match self.top_level.next() {
                // If it's a chunk, get a new chunk reader going...
                Some(Ok(Record::Chunk { header, data })) => {
                    // Chunks from the LinearReader will always borrow from the file.
                    // (Getting a normal reference to the underlying data back
                    // frees us from returning things that reference this local Cow.)
                    let data: &'a [u8] = match data {
                        Cow::Borrowed(b) => b,
                        Cow::Owned(_) => unreachable!(),
                    };

                    self.dechunk = match ChunkReader::new(header, data) {
                        Ok(d) => Some(d),
                        Err(e) => break Some(Err(e)),
                    };
                    // ...then continue the loop to get the first item from the chunk.
                }
                // If it's not a chunk, just yield it.
                not_a_chunk => break not_a_chunk,
            }
        };

        // Give up on errors
        if matches!(n, Some(Err(_))) {
            self.malformed = true;
        }
        n
    }
}

/// Parses schemas and channels and wires them together
#[derive(Debug, Default)]
struct ChannelAccumulator<'a> {
    schemas: HashMap<u16, Arc<Schema<'a>>>,
    channels: HashMap<u16, Arc<Channel<'a>>>,
}

impl<'a> ChannelAccumulator<'a> {
    fn add_schema(&mut self, header: records::SchemaHeader, data: Cow<'a, [u8]>) -> McapResult<()> {
        if header.id == 0 {
            return Err(McapError::InvalidSchemaId);
        }

        let schema = Arc::new(Schema {
            name: header.name.clone(),
            encoding: header.encoding,
            data,
        });

        if let Some(preexisting) = self.schemas.insert(header.id, schema.clone()) {
            // Oh boy, we have this schema already.
            // It had better be identital.
            if schema != preexisting {
                return Err(McapError::ConflictingSchemas(header.name));
            }
        }
        Ok(())
    }

    fn add_channel(&mut self, chan: records::Channel) -> McapResult<()> {
        // The schema ID can be 0 for "no schema",
        // Or must reference some previously-read schema.
        let schema = if chan.schema_id == 0 {
            None
        } else {
            match self.schemas.get(&chan.schema_id) {
                Some(s) => Some(s.clone()),
                None => {
                    return Err(McapError::UnknownSchema(chan.topic, chan.schema_id));
                }
            }
        };

        let channel = Arc::new(Channel {
            topic: chan.topic.clone(),
            schema,
            message_encoding: chan.message_encoding,
            metadata: chan.metadata,
        });
        if let Some(preexisting) = self.channels.insert(chan.id, channel.clone()) {
            // Oh boy, we have this channel already.
            // It had better be identital.
            if preexisting != channel {
                return Err(McapError::ConflictingChannels(chan.topic));
            }
        }
        Ok(())
    }

    fn get(&self, chan_id: u16) -> Option<Arc<Channel<'a>>> {
        self.channels.get(&chan_id).cloned()
    }
}

/// Reads all messages from the MCAP file---in the order they were written---and
/// perform needed validation (CRCs, etc.) as we go.
///
/// Unlike [`MessageStream`], this iterator returns the raw [`MessageHeader`](records::MessageHeader)
/// and message data instead of constructing a [`Message`].
/// This can be useful for situations where you don't need the specifics of each
/// message's [`Channel`], but just want to be able to discriminate them _by_ their channel
/// (e.g., build some map of `Channel -> Vec<Message>`).
///
/// This stops at the end of the data section and does not read the summary.
pub struct RawMessageStream<'a> {
    full_file: &'a [u8],
    records: ChunkFlattener<'a>,
    done: bool,
    channeler: ChannelAccumulator<'static>,
}

impl<'a> RawMessageStream<'a> {
    pub fn new(buf: &'a [u8]) -> McapResult<Self> {
        Self::new_with_options(buf, enum_set!())
    }

    pub fn new_with_options(buf: &'a [u8], options: EnumSet<Options>) -> McapResult<Self> {
        let full_file = buf;
        let records = ChunkFlattener::new_with_options(buf, options)?;

        Ok(Self {
            full_file,
            records,
            done: false,
            channeler: ChannelAccumulator::default(),
        })
    }

    /// Gets the channel with the given ID (presumably from a [`MessageHeader`](records::MessageHeader))
    pub fn get_channel(&self, channel_id: u16) -> Option<Arc<Channel<'a>>> {
        self.channeler.get(channel_id)
    }
}

pub struct RawMessage<'a> {
    pub header: records::MessageHeader,
    pub data: Cow<'a, [u8]>,
}

impl<'a> Iterator for RawMessageStream<'a> {
    type Item = McapResult<RawMessage<'a>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }

        let n = loop {
            // Let's start with a working record.
            let record = match self.records.next() {
                Some(Ok(rec)) => rec,
                Some(Err(e)) => break Some(Err(e)),
                None => break None,
            };

            match record {
                // Insert schemas into self so we know when subsequent channels reference them.
                Record::Schema { header, data } => {
                    let data = Cow::Owned(data.into_owned());
                    if let Err(e) = self.channeler.add_schema(header, data) {
                        break Some(Err(e));
                    }
                }

                // Insert channels into self so we know when subsequent messages reference them.
                Record::Channel(chan) => {
                    if let Err(e) = self.channeler.add_channel(chan) {
                        break Some(Err(e));
                    }
                }

                Record::Message { header, data } => {
                    break Some(Ok(RawMessage { header, data }));
                }

                // If it's EOD, do unholy things to calculate the CRC.
                // This would be much easier reading from a seekable Read instead of a buffer.
                // (But that would also force us to make copies of schema, message, and attachment
                // data! Should we have two APIs?)
                Record::DataEnd(end) => {
                    if end.data_section_crc != 0 {
                        //  op, length, CRC
                        const DATA_END_SIZE: usize =
                            size_of::<u8>() + size_of::<u64>() + size_of::<u32>();

                        let start_of_data_end = self.full_file.len()
                            - self.records.bytes_remaining() // sans MAGIC!
                            - MAGIC.len() // MORE MAGIC
                            - DATA_END_SIZE;
                        let data_section = &self.full_file[..start_of_data_end];

                        let calculated = crc32(data_section);
                        if end.data_section_crc != calculated {
                            break Some(Err(McapError::BadDataCrc {
                                saved: end.data_section_crc,
                                calculated,
                            }));
                        }
                    }
                    break None; // We're done at any rate.
                }
                _skip => {}
            };
        };

        if !matches!(n, Some(Ok(_))) {
            self.done = true;
        }
        n
    }
}

/// Like [`RawMessageStream`], but constructs a [`Message`]
/// (complete with its [`Channel`]) from the raw header and data.
///
/// This stops at the end of the data section and does not read the summary.
///
/// Because tying the lifetime of each message to the underlying MCAP memory map
/// makes it very difficult to send between threads or use in async land,
/// and because we assume _most_ MCAP files have _most_ messages in compressed chunks,
/// yielded [`Message`]s have unbounded lifetimes.
/// For messages we've decompressed into their own buffers, this is free!
/// For uncompressed messages, we take a copy of the message's data.
pub struct MessageStream<'a> {
    inner: RawMessageStream<'a>,
}

impl<'a> MessageStream<'a> {
    pub fn new(buf: &'a [u8]) -> McapResult<Self> {
        Self::new_with_options(buf, enum_set!())
    }

    pub fn new_with_options(buf: &'a [u8], options: EnumSet<Options>) -> McapResult<Self> {
        RawMessageStream::new_with_options(buf, options).map(|inner| Self { inner })
    }
}

impl<'a> Iterator for MessageStream<'a> {
    type Item = McapResult<Message<'static>>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.inner.next() {
            Some(Ok(RawMessage { header, data })) => {
                // Messages must have a previously-read channel.
                let channel = match self.inner.channeler.get(header.channel_id) {
                    Some(c) => c,
                    None => {
                        return Some(Err(McapError::UnknownChannel(
                            header.sequence,
                            header.channel_id,
                        )))
                    }
                };

                Some(Ok(Message {
                    channel,
                    sequence: header.sequence,
                    log_time: header.log_time,
                    publish_time: header.publish_time,
                    data: Cow::Owned(data.into_owned()),
                }))
            }
            // Coerce Option<McapResult<(header, data)>> into Option<McapResult<Message>>
            Some(Err(e)) => Some(Err(e)),
            None => None,
        }
    }
}

const FOOTER_LEN: usize = 20 + 8 + 1; // 20 bytes + 8 byte len + 1 byte opcode

/// Read the MCAP footer.
///
/// You'd probably prefer to use [`Summary::read`] to parse the whole summary,
/// then index into the rest of the file with
/// [`Summary::stream_chunk`], [`attachment`], [`metadata`], etc.
pub fn footer(mcap: &[u8]) -> McapResult<records::Footer> {
    if mcap.len() < MAGIC.len() * 2 + FOOTER_LEN {
        return Err(McapError::UnexpectedEof);
    }

    if !mcap.starts_with(MAGIC) || !mcap.ends_with(MAGIC) {
        return Err(McapError::BadMagic);
    }

    let footer_buf = &mcap[mcap.len() - MAGIC.len() - FOOTER_LEN..];

    match LinearReader::sans_magic(footer_buf).next() {
        Some(Ok(Record::Footer(f))) => Ok(f),
        _ => Err(McapError::BadFooter),
    }
}

/// Indexes of an MCAP file parsed from its (optional) summary section
#[derive(Default, Eq, PartialEq)]
pub struct Summary<'a> {
    pub stats: Option<records::Statistics>,
    /// Maps channel IDs to their channel
    pub channels: HashMap<u16, Arc<Channel<'a>>>,
    /// Maps schema IDs to their schema
    pub schemas: HashMap<u16, Arc<Schema<'a>>>,
    pub chunk_indexes: Vec<records::ChunkIndex>,
    pub attachment_indexes: Vec<records::AttachmentIndex>,
    pub metadata_indexes: Vec<records::MetadataIndex>,
}

impl fmt::Debug for Summary<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Keep the actual maps as HashMaps for constant-time lookups,
        // but order everything up before debug printing it here.
        let channels = self.channels.iter().collect::<BTreeMap<_, _>>();
        let schemas = self.schemas.iter().collect::<BTreeMap<_, _>>();

        f.debug_struct("Summary")
            .field("stats", &self.stats)
            .field("channels", &channels)
            .field("schemas", &schemas)
            .field("chunk_indexes", &self.chunk_indexes)
            .field("attachment_indexes", &self.attachment_indexes)
            .field("metadata_indexes", &self.metadata_indexes)
            .finish()
    }
}

impl<'a> Summary<'a> {
    /// Read the summary section of the given mapped MCAP file, if it has one.
    pub fn read(mcap: &'a [u8]) -> McapResult<Option<Self>> {
        let foot = footer(mcap)?;

        // A summary start offset of 0 means there's no summary.
        if foot.summary_start == 0 {
            return Ok(None);
        }

        if foot.summary_crc != 0 {
            // The checksum covers the entire summary _except_ itself, including other footer bytes.
            let calculated =
                crc32(&mcap[foot.summary_start as usize..mcap.len() - MAGIC.len() - 4]);
            if foot.summary_crc != calculated {
                return Err(McapError::BadSummaryCrc {
                    saved: foot.summary_crc,
                    calculated,
                });
            }
        }

        let mut summary = Summary::default();
        let mut channeler = ChannelAccumulator::default();

        let summary_end = match foot.summary_offset_start {
            0 => MAGIC.len() - FOOTER_LEN,
            sos => sos as usize,
        };
        let summary_buf = &mcap[foot.summary_start as usize..summary_end];

        for record in LinearReader::sans_magic(summary_buf) {
            match record? {
                Record::Statistics(s) => {
                    if summary.stats.is_some() {
                        warn!("Multiple statistics records found in summary");
                    }
                    summary.stats = Some(s);
                }
                Record::Schema { header, data } => channeler.add_schema(header, data)?,
                Record::Channel(c) => channeler.add_channel(c)?,
                Record::ChunkIndex(c) => summary.chunk_indexes.push(c),
                Record::AttachmentIndex(a) => summary.attachment_indexes.push(a),
                Record::MetadataIndex(i) => summary.metadata_indexes.push(i),
                _ => {}
            };
        }

        summary.schemas = channeler.schemas;
        summary.channels = channeler.channels;

        Ok(Some(summary))
    }

    /// Stream messages from the chunk with the given index.
    ///
    /// To avoid having to read all preceding chunks first,
    /// channels and their schemas are pulled from this summary.
    pub fn stream_chunk(
        &self,
        mcap: &'a [u8],
        index: &records::ChunkIndex,
    ) -> McapResult<impl Iterator<Item = McapResult<Message<'a>>> + '_> {
        let end = (index.chunk_start_offset + index.chunk_length) as usize;
        if mcap.len() < end {
            return Err(McapError::BadIndex);
        }

        // Get the chunk (as a header and its data) out of the file at the given offset.
        let mut reader = LinearReader::sans_magic(&mcap[index.chunk_start_offset as usize..end]);
        let (h, d) = match reader.next().ok_or(McapError::BadIndex)? {
            Ok(records::Record::Chunk { header, data }) => (header, data),
            Ok(_other_record) => return Err(McapError::BadIndex),
            Err(e) => return Err(e),
        };
        // Chunks from the LinearReader will always borrow from the file.
        // (Getting a normal reference to the underlying data back
        // frees us from returning things that reference this local Cow.)
        let d: &'a [u8] = match d {
            Cow::Borrowed(b) => b,
            Cow::Owned(_) => unreachable!(),
        };

        if reader.next().is_some() {
            // Wut - multiple records in the given slice?
            return Err(McapError::BadIndex);
        }

        // Now let's stream messages out of the chunk.
        let messages = ChunkReader::new(h, d)?.filter_map(|record| match record {
            Ok(records::Record::Message { header, data }) => {
                // Correlate the message to its channel from this summary.
                let channel = match self.channels.get(&header.channel_id) {
                    Some(c) => c.clone(),
                    None => {
                        return Some(Err(McapError::UnknownChannel(
                            header.sequence,
                            header.channel_id,
                        )));
                    }
                };

                let m = Message {
                    channel,
                    sequence: header.sequence,
                    log_time: header.log_time,
                    publish_time: header.publish_time,
                    data,
                };

                Some(Ok(m))
            }
            // We don't care about other chunk records (channels, schemas) -
            // we should have them from &self already.
            Ok(_other_record) => None,
            // We do care about errors, though.
            Err(e) => Some(Err(e)),
        });

        Ok(messages)
    }

    /// Read the mesage indexes for the given indexed chunk.
    ///
    /// Channels and their schemas are pulled from this summary.
    /// The offsets in each [`MessageIndexEntry`](records::MessageIndexEntry)
    /// is relative to the decompressed contents of the given chunk.
    pub fn read_message_indexes(
        &self,
        mcap: &[u8],
        index: &records::ChunkIndex,
    ) -> McapResult<HashMap<Arc<Channel>, Vec<records::MessageIndexEntry>>> {
        if index.message_index_offsets.is_empty() {
            // Message indexing is optional... should we be more descriptive here?
            return Err(McapError::BadIndex);
        }

        let mut indexes = HashMap::new();

        for (channel_id, offset) in &index.message_index_offsets {
            let offset = *offset as usize;

            // Message indexes are at least 15 bytes:
            // 1 byte opcode, 8 byte length, 2 byte channel ID, 4 byte array len
            if mcap.len() < offset + 15 {
                return Err(McapError::BadIndex);
            }

            // Get the MessageIndex out of the file at the given offset.
            let mut reader = LinearReader::sans_magic(&mcap[offset..]);
            let index = match reader.next().ok_or(McapError::BadIndex)? {
                Ok(records::Record::MessageIndex(i)) => i,
                Ok(_other_record) => return Err(McapError::BadIndex),
                Err(e) => return Err(e),
            };

            // The channel ID from the chunk index and the message index should match
            if *channel_id != index.channel_id {
                return Err(McapError::BadIndex);
            }

            let channel = match self.channels.get(&index.channel_id) {
                Some(c) => c,
                None => {
                    return Err(McapError::UnknownChannel(
                        0, // We don't have a message sequence num yet.
                        index.channel_id,
                    ));
                }
            };

            if indexes.insert(channel.clone(), index.records).is_some() {
                return Err(McapError::ConflictingChannels(channel.topic.clone()));
            }
        }

        Ok(indexes)
    }

    /// Seek to the given message in the given indexed chunk.
    ///
    /// If you're interested in more than a single message from the chunk,
    /// filtering [`Summary::stream_chunk`] is probably a better bet.
    /// Compressed chunks aren't random access -
    /// this decompresses everything in the chunk before
    /// [`message.offset`](records::MessageIndexEntry::offset) and throws it away.
    pub fn seek_message(
        &self,
        mcap: &'a [u8],
        index: &records::ChunkIndex,
        message: &records::MessageIndexEntry,
    ) -> McapResult<Message> {
        // Get the chunk (as a header and its data) out of the file at the given offset.
        let end = (index.chunk_start_offset + index.chunk_length) as usize;
        if mcap.len() < end {
            return Err(McapError::BadIndex);
        }

        let mut reader = LinearReader::sans_magic(&mcap[index.chunk_start_offset as usize..end]);
        let (h, d) = match reader.next().ok_or(McapError::BadIndex)? {
            Ok(records::Record::Chunk { header, data }) => (header, data),
            Ok(_other_record) => return Err(McapError::BadIndex),
            Err(e) => return Err(e),
        };
        // Chunks from the LinearReader will always borrow from the file.
        // (Getting a normal reference to the underlying data back
        // frees us from returning things that reference this local Cow.)
        let d: &'a [u8] = match d {
            Cow::Borrowed(b) => b,
            Cow::Owned(_) => unreachable!(),
        };

        if reader.next().is_some() {
            // Wut - multiple records in the given slice?
            return Err(McapError::BadIndex);
        }

        let mut chunk_reader = ChunkReader::new(h, d)?;

        // Do unspeakable things to seek to the message.
        match &mut chunk_reader.decompressor {
            ChunkDecompressor::Null(reader) => {
                // Skip messages until we're at the offset.
                while reader.bytes_remaining() as u64 > index.uncompressed_size - message.offset {
                    match reader.next() {
                        Some(Ok(_)) => {}
                        Some(Err(e)) => return Err(e),
                        None => return Err(McapError::BadIndex),
                    };
                }
                // Be exact!
                if reader.bytes_remaining() as u64 != index.uncompressed_size - message.offset {
                    return Err(McapError::BadIndex);
                }
            }
            ChunkDecompressor::Compressed(maybe_read) => {
                let reader = maybe_read.as_mut().unwrap();
                // Decompress offset bytes, which should put us at the message we want.
                io::copy(&mut reader.take(message.offset), &mut io::sink())?;
            }
        }

        // Now let's get our message.
        match chunk_reader.next() {
            Some(Ok(records::Record::Message { header, data })) => {
                // Correlate the message to its channel from this summary.
                let channel = match self.channels.get(&header.channel_id) {
                    Some(c) => c.clone(),
                    None => {
                        return Err(McapError::UnknownChannel(
                            header.sequence,
                            header.channel_id,
                        ));
                    }
                };

                let m = Message {
                    channel,
                    sequence: header.sequence,
                    log_time: header.log_time,
                    publish_time: header.publish_time,
                    data,
                };

                Ok(m)
            }
            // The index told us this was a message...
            Some(Ok(_other_record)) => Err(McapError::BadIndex),
            Some(Err(e)) => Err(e),
            None => Err(McapError::BadIndex),
        }
    }
}

/// Read the attachment with the given index.
pub fn attachment<'a>(
    mcap: &'a [u8],
    index: &records::AttachmentIndex,
) -> McapResult<Attachment<'a>> {
    let end = (index.offset + index.length) as usize;
    if mcap.len() < end {
        return Err(McapError::BadIndex);
    }

    let mut reader = LinearReader::sans_magic(&mcap[index.offset as usize..end]);
    let (h, d) = match reader.next().ok_or(McapError::BadIndex)? {
        Ok(records::Record::Attachment { header, data }) => (header, data),
        Ok(_other_record) => return Err(McapError::BadIndex),
        Err(e) => return Err(e),
    };

    if reader.next().is_some() {
        // Wut - multiple records in the given slice?
        return Err(McapError::BadIndex);
    }

    Ok(Attachment {
        log_time: h.log_time,
        create_time: h.create_time,
        name: h.name,
        media_type: h.media_type,
        data: d,
    })
}

/// Read the metadata with the given index.
pub fn metadata(mcap: &[u8], index: &records::MetadataIndex) -> McapResult<records::Metadata> {
    let end = (index.offset + index.length) as usize;
    if mcap.len() < end {
        return Err(McapError::BadIndex);
    }

    let mut reader = LinearReader::sans_magic(&mcap[index.offset as usize..end]);
    let m = match reader.next().ok_or(McapError::BadIndex)? {
        Ok(records::Record::Metadata(m)) => m,
        Ok(_other_record) => return Err(McapError::BadIndex),
        Err(e) => return Err(e),
    };

    if reader.next().is_some() {
        // Wut - multiple records in the given slice?
        return Err(McapError::BadIndex);
    }

    Ok(m)
}

// All of the following panic if they walk off the back of the data block;
// callers are assumed to have made sure they got enoug bytes back with
// `validate_response()`

/// Builds a `read_<type>(&mut buf)` function that reads a given type
/// off the buffer and advances it the appropriate number of bytes.
macro_rules! reader {
    ($type:ty) => {
        paste::paste! {
            #[inline]
            fn [<read_ $type>](block: &mut &[u8]) -> $type {
                const SIZE: usize = size_of::<$type>();
                let res = $type::from_le_bytes(
                    block[0..SIZE].try_into().unwrap()
                );
                *block = &block[SIZE..];
                res
            }
        }
    };
}

reader!(u8);
reader!(u64);

#[cfg(test)]
mod test {
    use super::*;

    // Can we read a file that's only magic?
    // (Probably considered malformed by the spec, but let's not panic on user input)

    #[test]
    fn only_two_magics() {
        let two_magics = MAGIC.repeat(2);
        let mut reader = LinearReader::new(&two_magics).unwrap();
        assert!(reader.next().is_none());
    }

    #[test]
    fn only_one_magic() {
        assert!(matches!(LinearReader::new(MAGIC), Err(McapError::BadMagic)));
    }

    #[test]
    fn only_two_magic_with_ignore_end_magic() {
        let two_magics = MAGIC.repeat(2);
        let mut reader =
            LinearReader::new_with_options(&two_magics, enum_set!(Options::IgnoreEndMagic))
                .unwrap();
        assert!(reader.next().is_none());
    }

    #[test]
    fn only_one_magic_with_ignore_end_magic() {
        let mut reader =
            LinearReader::new_with_options(MAGIC, enum_set!(Options::IgnoreEndMagic)).unwrap();
        assert!(reader.next().is_none());
    }

    #[test]
    fn test_read_record_from_slice_fails_on_too_short_chunks() {
        let res = read_record_from_slice(&mut [0_u8; 4].as_slice());
        assert!(matches!(res, Err(McapError::UnexpectedEof)));

        let res = read_record_from_slice(&mut [0_u8; 8].as_slice());
        assert!(matches!(res, Err(McapError::UnexpectedEof)));
    }

    #[test]
    fn test_read_record_from_slice_parses_for_big_enough_records() {
        let res = read_record_from_slice(&mut [0_u8; 9].as_slice());
        assert!(res.is_ok());
        // Not a very strong test, but we are only testing that it checks the buffer size correctly
        assert!(matches!(res, Ok(Record::Unknown { opcode: _, data: _ })));
    }
}
