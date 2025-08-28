//! Read MCAP data from a memory-mapped file.
//!
//! MCAPs are read from a byte slice instead of a [`std::io::Read`] trait object.
//! Consider [memory-mapping](https://docs.rs/memmap2/0.9.5/memmap2/struct.Mmap.html)
//! the file - the OS will load (and cache!) it on-demand, without any
//! further system calls.
use std::{
    borrow::Cow,
    collections::{hash_map::Entry, BTreeMap, HashMap},
    fmt,
    io::Cursor,
    sync::Arc,
};

use binrw::prelude::*;
use byteorder::{ReadBytesExt, LE};
use crc32fast::hash as crc32;
use enumset::{enum_set, EnumSet, EnumSetType};

use crate::{
    records::{self, op, Footer, Record},
    sans_io::{
        LinearReadEvent, LinearReader as SansIoReader, LinearReaderOptions, SummaryReadEvent,
        SummaryReader, SummaryReaderOptions,
    },
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
    inner: InnerReader<'a>,
}

impl<'a> LinearReader<'a> {
    /// Create a reader for the given file,
    /// checking [`MAGIC`] bytes on both ends.
    pub fn new(buf: &'a [u8]) -> McapResult<Self> {
        Self::new_with_options(buf, enum_set!())
    }

    /// Create a reader for the given file with special options.
    pub fn new_with_options(buf: &'a [u8], options: EnumSet<Options>) -> McapResult<Self> {
        Ok(Self {
            inner: InnerReader {
                buf,
                reader: SansIoReader::new_with_options(
                    LinearReaderOptions::default()
                        .with_record_length_limit(buf.len())
                        .with_skip_end_magic(options.contains(Options::IgnoreEndMagic))
                        .with_validate_chunk_crcs(true)
                        .with_emit_chunks(true),
                ),
            },
        })
    }

    /// Like [`new()`](Self::new), but assumes `buf` has the magic bytes sliced off.
    ///
    /// Useful for iterating through slices of an MCAP file instead of the whole thing.
    pub fn sans_magic(buf: &'a [u8]) -> Self {
        Self {
            inner: InnerReader {
                buf,
                reader: SansIoReader::new_with_options(
                    LinearReaderOptions::default()
                        .with_record_length_limit(buf.len())
                        .with_skip_end_magic(true)
                        .with_skip_start_magic(true),
                ),
            },
        }
    }
}

impl<'a> Iterator for LinearReader<'a> {
    type Item = McapResult<records::Record<'a>>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

/// Given a records' opcode and data, parse into a Record. The resulting Record will contain
/// borrowed slices from `body`.
pub fn parse_record(op: u8, body: &[u8]) -> McapResult<records::Record<'_>> {
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
            // We still provide the parsed CRC to the caller in case they want to re-serialize the
            // record in another MCAP, and so they know if the record had a non-zero CRC.
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
                crc,
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

/// Streams records out of a [Chunk](Record::Chunk), decompressing as needed.
pub struct ChunkReader<'a> {
    inner: InnerReader<'a>,
}

impl<'a> ChunkReader<'a> {
    pub fn new(header: records::ChunkHeader, buf: &'a [u8]) -> McapResult<Self> {
        Ok(Self {
            inner: InnerReader {
                reader: SansIoReader::for_chunk(header)?,
                buf,
            },
        })
    }
}

impl<'a> Iterator for ChunkReader<'a> {
    type Item = McapResult<records::Record<'a>>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

// common implementation for iterating over a range of owned records in a mapped buffer.
struct InnerReader<'a> {
    buf: &'a [u8],
    reader: SansIoReader,
}

impl<'a> Iterator for InnerReader<'a> {
    type Item = McapResult<records::Record<'a>>;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(event) = self.reader.next_event() {
            match event {
                Ok(LinearReadEvent::ReadRequest(need)) => {
                    let len = std::cmp::min(self.buf.len(), need);
                    self.reader.insert(len).copy_from_slice(&self.buf[..len]);
                    self.reader.notify_read(len);
                    self.buf = &self.buf[len..];
                }
                Ok(LinearReadEvent::Record { data, opcode }) => match parse_record(opcode, data) {
                    Ok(record) => return Some(Ok(record.into_owned())),
                    Err(err) => return Some(Err(err)),
                },
                Err(err) => return Some(Err(err)),
            }
        }
        None
    }
}

/// Like [`LinearReader`], but unpacks chunks' records into its stream
pub struct ChunkFlattener<'a> {
    inner: InnerReader<'a>,
}

impl<'a> ChunkFlattener<'a> {
    pub fn new(buf: &'a [u8]) -> McapResult<Self> {
        Self::new_with_options(buf, enum_set!())
    }

    pub fn new_with_options(buf: &'a [u8], options: EnumSet<Options>) -> McapResult<Self> {
        Ok(Self {
            inner: InnerReader {
                buf,
                reader: SansIoReader::new_with_options(
                    LinearReaderOptions::default()
                        .with_skip_end_magic(options.contains(Options::IgnoreEndMagic))
                        .with_validate_chunk_crcs(true),
                ),
            },
        })
    }
}

impl<'a> Iterator for ChunkFlattener<'a> {
    type Item = McapResult<records::Record<'a>>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

/// Parses schemas and channels and wires them together
#[derive(Debug, Default)]
pub(crate) struct ChannelAccumulator<'a> {
    pub(crate) schemas: HashMap<u16, Arc<Schema<'a>>>,
    pub(crate) channels: HashMap<u16, Arc<Channel<'a>>>,
}

impl<'a> ChannelAccumulator<'a> {
    pub(crate) fn add_schema(
        &mut self,
        header: records::SchemaHeader,
        data: Cow<'a, [u8]>,
    ) -> McapResult<()> {
        if header.id == 0 {
            return Err(McapError::InvalidSchemaId);
        }
        match self.schemas.entry(header.id) {
            Entry::Occupied(entry) => {
                // If we already have this schema, it must be identical.
                let schema = entry.get();
                if schema.name == header.name
                    && schema.encoding == header.encoding
                    && schema.data == data
                {
                    Ok(())
                } else {
                    Err(McapError::ConflictingSchemas(header.name))
                }
            }
            Entry::Vacant(entry) => {
                entry.insert(Arc::new(Schema {
                    id: header.id,
                    name: header.name.clone(),
                    encoding: header.encoding,
                    data,
                }));
                Ok(())
            }
        }
    }

    pub(crate) fn add_channel(&mut self, chan: records::Channel) -> McapResult<()> {
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
        match self.channels.entry(chan.id) {
            Entry::Occupied(entry) => {
                // If we already have this channel, it must be identical.
                let channel = entry.get();
                if channel.topic == chan.topic
                    && channel.schema.as_ref().map(|s| s.id).unwrap_or(0) == chan.schema_id
                    && channel.message_encoding == chan.message_encoding
                    && channel.metadata == chan.metadata
                {
                    Ok(())
                } else {
                    Err(McapError::ConflictingChannels(chan.topic))
                }
            }
            Entry::Vacant(entry) => {
                entry.insert(Arc::new(Channel {
                    id: chan.id,
                    topic: chan.topic.clone(),
                    schema,
                    message_encoding: chan.message_encoding,
                    metadata: chan.metadata,
                }));
                Ok(())
            }
        }
    }

    pub(crate) fn get(&self, chan_id: u16) -> Option<Arc<Channel<'a>>> {
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
    records: ChunkFlattener<'a>,
    done: bool,
    channeler: ChannelAccumulator<'static>,
}

impl<'a> RawMessageStream<'a> {
    pub fn new(buf: &'a [u8]) -> McapResult<Self> {
        Self::new_with_options(buf, enum_set!())
    }

    pub fn new_with_options(buf: &'a [u8], options: EnumSet<Options>) -> McapResult<Self> {
        let records = ChunkFlattener::new_with_options(buf, options)?;

        Ok(Self {
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

impl Iterator for MessageStream<'_> {
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

const FOOTER_LEN: usize = 8 // summary start
 + 8 // summary offset start
 + 4; // summary section CRC
const FOOTER_RECORD_LEN: usize = 1 // opcode
     + 8 // record length
     + FOOTER_LEN;

/// Read the MCAP footer.
///
/// You'd probably prefer to use [`Summary::read`] to parse the whole summary,
/// then index into the rest of the file with
/// [`Summary::stream_chunk`], [`attachment`], [`metadata`], etc.
pub fn footer(mcap: &[u8]) -> McapResult<records::Footer> {
    // an MCAP must be at least large enough to accomodate a header magic, a footer record and a
    // footer magic.
    if mcap.len() < (MAGIC.len() + FOOTER_RECORD_LEN + MAGIC.len()) {
        return Err(McapError::UnexpectedEof);
    }

    if !mcap.starts_with(MAGIC) || !mcap.ends_with(MAGIC) {
        return Err(McapError::BadMagic);
    }

    let footer_buf = &mcap[mcap.len() - MAGIC.len() - FOOTER_LEN..];
    let mut cursor = std::io::Cursor::new(footer_buf);

    Ok(Footer::read_le(&mut cursor)?)
}

/// Indexes of an MCAP file parsed from its (optional) summary section
#[derive(Default, Eq, PartialEq, Clone)]
pub struct Summary {
    pub stats: Option<records::Statistics>,
    /// Maps channel IDs to their channel
    pub channels: HashMap<u16, Arc<Channel<'static>>>,
    /// Maps schema IDs to their schema
    pub schemas: HashMap<u16, Arc<Schema<'static>>>,
    pub chunk_indexes: Vec<records::ChunkIndex>,
    pub attachment_indexes: Vec<records::AttachmentIndex>,
    pub metadata_indexes: Vec<records::MetadataIndex>,
}

impl fmt::Debug for Summary {
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

impl Summary {
    /// Read the summary section of the given mapped MCAP file, if it has one.
    pub fn read(mcap: &[u8]) -> McapResult<Option<Self>> {
        use std::io::{Read, Seek};
        let mut cursor = std::io::Cursor::new(mcap);
        let mut summary_reader = SummaryReader::new_with_options(
            SummaryReaderOptions::default().with_file_size(mcap.len() as u64),
        );
        while let Some(event) = summary_reader.next_event() {
            match event? {
                SummaryReadEvent::ReadRequest(n) => {
                    let read = cursor.read(summary_reader.insert(n))?;
                    summary_reader.notify_read(read);
                }
                SummaryReadEvent::SeekRequest(to) => {
                    let pos = cursor.seek(to)?;
                    summary_reader.notify_seeked(pos);
                }
            }
        }

        Ok(summary_reader.finish())
    }

    /// Stream messages from the chunk with the given index.
    ///
    /// To avoid having to read all preceding chunks first,
    /// channels and their schemas are pulled from this summary.
    pub fn stream_chunk<'a, 'b: 'a>(
        &'b self,
        mcap: &'a [u8],
        index: &records::ChunkIndex,
    ) -> McapResult<impl Iterator<Item = McapResult<Message<'a>>> + 'a> {
        let end = (index.chunk_start_offset + index.chunk_length) as usize;
        if mcap.len() < end {
            return Err(McapError::BadIndex);
        }

        // Get the chunk (as a header and its data) out of the file at the given offset.
        let chunk_record_buf = &mcap[(index.chunk_start_offset as usize) + 9..end];
        let chunk = parse_record(op::CHUNK, chunk_record_buf);

        let (h, d) = match chunk {
            Ok(records::Record::Chunk { header, data }) => (header, data),
            Ok(_other_record) => return Err(McapError::BadIndex),
            Err(e) => return Err(e),
        };
        // Chunks from the LinearReader will always borrow from the file.
        // (Getting a normal reference to the underlying data back
        // frees us from returning things that reference this local Cow.)
        let d: &[u8] = match d {
            Cow::Borrowed(b) => b,
            Cow::Owned(_) => unreachable!(),
        };

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
    pub fn seek_message<'a>(
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
        let chunk = parse_record(
            op::CHUNK,
            &mcap[(index.chunk_start_offset + 9) as usize..end],
        );
        let (h, d) = match chunk {
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

        let mut reader = SansIoReader::for_chunk(h)?;
        let mut remaining = d;
        let mut uncompressed_offset: usize = 0;
        while let Some(event) = reader.next_event() {
            match event {
                Ok(LinearReadEvent::ReadRequest(need)) => {
                    let len = std::cmp::min(remaining.len(), need);
                    reader.insert(len).copy_from_slice(&remaining[..len]);
                    reader.notify_read(len);
                    remaining = &remaining[len..];
                }
                Ok(LinearReadEvent::Record { data, opcode }) => {
                    if (uncompressed_offset as u64) < message.offset {
                        uncompressed_offset += 9 + data.len();
                    } else {
                        if uncompressed_offset as u64 != message.offset {
                            return Err(McapError::BadIndex);
                        }
                        match parse_record(opcode, data)? {
                            Record::Message { header, data } => {
                                let channel = match self.channels.get(&header.channel_id) {
                                    Some(c) => c.clone(),
                                    None => {
                                        return Err(McapError::UnknownChannel(
                                            header.sequence,
                                            header.channel_id,
                                        ));
                                    }
                                };
                                return Ok(Message {
                                    channel,
                                    sequence: header.sequence,
                                    log_time: header.log_time,
                                    publish_time: header.publish_time,
                                    data: Cow::Owned(data.into()),
                                });
                            }
                            _ => return Err(McapError::BadIndex),
                        }
                    }
                }
                Err(err) => return Err(err),
            }
        }
        Err(McapError::BadIndex)
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
        Ok(records::Record::Attachment { header, data, .. }) => (header, data),
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
