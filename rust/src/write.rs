//! Write MCAP files

use std::{
    borrow::Cow,
    collections::{BTreeMap, HashMap},
    io::{self, prelude::*, Cursor, SeekFrom},
    mem::size_of,
};

use binrw::prelude::*;
use byteorder::{WriteBytesExt, LE};

use crate::{
    io_utils::CountingCrcWriter,
    records::{self, op, MessageHeader, Record},
    Attachment, Channel, Compression, McapError, McapResult, Message, Schema, MAGIC,
};

pub use records::Metadata;

enum WriteMode<W: Write + Seek> {
    Raw(W),
    Chunk(ChunkWriter<W>),
}

fn op_and_len<W: Write>(w: &mut W, op: u8, len: usize) -> io::Result<()> {
    w.write_u8(op)?;
    w.write_u64::<LE>(len as u64)?;
    Ok(())
}

fn write_record<W: Write>(w: &mut W, r: &Record) -> io::Result<()> {
    // Annoying: our stream isn't Seek if we're writing to a compressed chunk stream,
    // so we need an intermediate buffer.
    macro_rules! record {
        ($op:expr, $b:ident) => {{
            let mut rec_buf = Vec::new();
            Cursor::new(&mut rec_buf).write_le($b).unwrap();

            op_and_len(w, $op, rec_buf.len())?;
            w.write_all(&rec_buf)?;
        }};
    }

    match r {
        Record::Header(h) => record!(op::HEADER, h),
        Record::Footer(_) => {
            unreachable!("Footer handles its own serialization because its CRC is self-referencing")
        }
        Record::Schema { header, data } => {
            let mut header_buf = Vec::new();
            Cursor::new(&mut header_buf).write_le(header).unwrap();

            op_and_len(
                w,
                op::SCHEMA,
                header_buf.len() + size_of::<u32>() + data.len(),
            )?;
            w.write_all(&header_buf)?;
            w.write_u32::<LE>(data.len() as u32)?;
            w.write_all(data)?;
        }
        Record::Channel(c) => record!(op::CHANNEL, c),
        Record::Message { header, data } => {
            let mut header_buf = Vec::new();
            Cursor::new(&mut header_buf).write_le(header).unwrap();

            op_and_len(w, op::MESSAGE, header_buf.len() + data.len())?;
            w.write_all(&header_buf)?;
            w.write_all(data)?;
        }
        Record::Chunk { .. } => {
            unreachable!("Chunks handle their own serialization due to seeking shenanigans")
        }
        Record::MessageIndex(_) => {
            unreachable!("MessageIndexes handle their own serialization to recycle the buffer between indexes")
        }
        Record::ChunkIndex(c) => record!(op::CHUNK_INDEX, c),
        Record::Attachment { header, data } => {
            let mut header_buf = Vec::new();
            Cursor::new(&mut header_buf).write_le(header).unwrap();
            op_and_len(
                w,
                op::ATTACHMENT,
                header_buf.len() + size_of::<u64>() + data.len() + size_of::<u32>(), /* crc */
            )?;

            let mut checksummer = CountingCrcWriter::new(w);
            checksummer.write_all(&header_buf)?;
            checksummer.write_u64::<LE>(data.len() as u64)?;
            checksummer.write_all(data)?;
            let (w, crc) = checksummer.finalize();
            w.write_u32::<LE>(crc)?;
        }
        Record::AttachmentIndex(ai) => record!(op::ATTACHMENT_INDEX, ai),
        Record::Statistics(s) => record!(op::STATISTICS, s),
        Record::Metadata(m) => record!(op::METADATA, m),
        Record::MetadataIndex(mi) => record!(op::METADATA_INDEX, mi),
        Record::SummaryOffset(so) => record!(op::SUMMARY_OFFSET, so),
        Record::DataEnd(eod) => record!(op::DATA_END, eod),
        _ => todo!(),
    };
    Ok(())
}

#[derive(Debug, Clone)]
pub struct WriteOptions {
    compression: Option<Compression>,
    profile: String,
    chunk_size: Option<u64>,
}

impl Default for WriteOptions {
    fn default() -> Self {
        Self {
            #[cfg(feature = "zstd")]
            compression: Some(Compression::Zstd),
            #[cfg(not(feature = "zstd"))]
            compression: None,
            profile: String::new(),
            chunk_size: Some(1024 * 768),
        }
    }
}

impl WriteOptions {
    pub fn new() -> Self {
        Self::default()
    }

    /// Specifies the compression that should be used on chunks.
    pub fn compression(self, compression: Option<Compression>) -> Self {
        Self {
            compression,
            ..self
        }
    }

    /// specifies the profile that should be written to the MCAP Header record.
    pub fn profile<S: Into<String>>(self, profile: S) -> Self {
        Self {
            profile: profile.into(),
            ..self
        }
    }

    /// specifies the target uncompressed size of each chunk.
    ///
    /// Messages will be written to chunks until the uncompressed chunk is larger than the
    /// target chunk size, at which point the chunk will be closed and a new one started.
    /// If `None`, chunks will not be automatically closed and the user must call `flush()` to
    /// begin a new chunk.
    pub fn chunk_size(self, chunk_size: Option<u64>) -> Self {
        Self {
            chunk_size: chunk_size,
            ..self
        }
    }

    /// Creates a [`Writer`] whch writes to `w` using the given options
    pub fn create<'a, W: Write + Seek>(self, w: W) -> McapResult<Writer<'a, W>> {
        Writer::with_options(w, self)
    }
}

/// Writes an MCAP file to the given [writer](Write).
///
/// Users should call [`finish()`](Self::finish) to flush the stream
/// and check for errors when done; otherwise the result will be unwrapped on drop.
pub struct Writer<'a, W: Write + Seek> {
    writer: Option<WriteMode<W>>,
    options: WriteOptions,
    schemas: HashMap<Schema<'a>, u16>,
    channels: HashMap<Channel<'a>, u16>,
    chunk_indexes: Vec<records::ChunkIndex>,
    attachment_indexes: Vec<records::AttachmentIndex>,
    metadata_indexes: Vec<records::MetadataIndex>,
    /// Message start and end time, or None if there are no messages yet.
    message_bounds: Option<(u64, u64)>,
    channel_message_counts: BTreeMap<u16, u64>,
}

impl<'a, W: Write + Seek> Writer<'a, W> {
    pub fn new(writer: W) -> McapResult<Self> {
        Self::with_options(writer, WriteOptions::default())
    }

    fn with_options(mut writer: W, opts: WriteOptions) -> McapResult<Self> {
        writer.write_all(MAGIC)?;

        write_record(
            &mut writer,
            &Record::Header(records::Header {
                profile: opts.profile.clone(),
                library: String::from("mcap-rs-") + env!("CARGO_PKG_VERSION"),
            }),
        )?;

        Ok(Self {
            writer: Some(WriteMode::Raw(writer)),
            options: opts,
            schemas: HashMap::new(),
            channels: HashMap::new(),
            chunk_indexes: Vec::new(),
            attachment_indexes: Vec::new(),
            metadata_indexes: Vec::new(),
            message_bounds: None,
            channel_message_counts: BTreeMap::new(),
        })
    }

    /// Adds a channel (and its provided schema, if any), returning its ID.
    ///
    /// Useful with subequent calls to [`write_to_known_channel()`](Self::write_to_known_channel)
    pub fn add_channel(&mut self, chan: &Channel<'a>) -> McapResult<u16> {
        let schema_id = match &chan.schema {
            Some(s) => self.add_schema(s)?,
            None => 0,
        };

        if let Some(id) = self.channels.get(chan) {
            return Ok(*id);
        }

        let next_channel_id = self.channels.len() as u16;
        assert!(self
            .channels
            .insert(chan.clone(), next_channel_id)
            .is_none());
        self.chunkin_time()?
            .write_channel(next_channel_id, schema_id, chan)?;
        Ok(next_channel_id)
    }

    fn add_schema(&mut self, schema: &Schema<'a>) -> McapResult<u16> {
        if let Some(id) = self.schemas.get(schema) {
            return Ok(*id);
        }

        // Schema IDs cannot be zero, that's the sentinel value in a channel
        // for "no schema"
        let next_schema_id = self.schemas.len() as u16 + 1;
        assert!(self
            .schemas
            .insert(schema.clone(), next_schema_id)
            .is_none());
        self.chunkin_time()?.write_schema(next_schema_id, schema)?;
        Ok(next_schema_id)
    }

    /// Write the given message (and its provided channel, if needed).
    pub fn write(&mut self, message: &Message<'a>) -> McapResult<()> {
        let channel_id = self.add_channel(&message.channel)?;
        let header = MessageHeader {
            channel_id,
            sequence: message.sequence,
            log_time: message.log_time,
            publish_time: message.publish_time,
        };
        let data: &[u8] = &message.data;
        self.write_to_known_channel(&header, data)
    }

    /// Write a message to an added channel, given its ID.
    ///
    /// This skips hash lookups of the channel and schema if you already added them.
    pub fn write_to_known_channel(
        &mut self,
        header: &MessageHeader,
        data: &[u8],
    ) -> McapResult<()> {
        // The number of channels should be relatively small,
        // do a quick linear search to make sure we're not being given a bogus ID
        if !self.channels.values().any(|id| *id == header.channel_id) {
            return Err(McapError::UnknownChannel(
                header.sequence,
                header.channel_id,
            ));
        }

        self.message_bounds = Some(match self.message_bounds {
            None => (header.log_time, header.log_time),
            Some((start, end)) => (start.min(header.log_time), end.max(header.log_time)),
        });
        *self
            .channel_message_counts
            .entry(header.channel_id)
            .or_insert(0) += 1;

        // if the current chunk is larger than our target chunk size, finish it
        // and start a new one.
        let current_chunk_size = match &self.writer {
            Some(WriteMode::Chunk(cw)) => Some(cw.compressor.position()),
            _ => None,
        };
        if let (Some(current_chunk_size), Some(target)) =
            (current_chunk_size, self.options.chunk_size)
        {
            if current_chunk_size > target {
                self.finish_chunk()?;
            }
        }

        self.chunkin_time()?.write_message(header, data)?;
        Ok(())
    }

    pub fn attach(&mut self, attachment: &Attachment) -> McapResult<()> {
        let header = records::AttachmentHeader {
            log_time: attachment.log_time,
            create_time: attachment.create_time,
            name: attachment.name.clone(),
            media_type: attachment.media_type.clone(),
        };

        // Attachments don't live in chunks.
        let w = self.finish_chunk()?;

        let offset = w.stream_position()?;

        write_record(
            w,
            &Record::Attachment {
                header,
                data: Cow::Borrowed(&attachment.data),
            },
        )?;

        let length = w.stream_position()? - offset;
        self.attachment_indexes.push(records::AttachmentIndex {
            offset,
            length,
            log_time: attachment.log_time,
            create_time: attachment.create_time,
            data_size: attachment.data.len() as u64,
            name: attachment.name.clone(),
            media_type: attachment.media_type.clone(),
        });

        Ok(())
    }

    pub fn write_metadata(&mut self, metadata: &Metadata) -> McapResult<()> {
        let w = self.finish_chunk()?;
        let offset = w.stream_position()?;

        // Should we specialize this to avoid taking a clone of the map?
        write_record(w, &Record::Metadata(metadata.clone()))?;

        let length = w.stream_position()? - offset;

        self.metadata_indexes.push(records::MetadataIndex {
            offset,
            length,
            name: metadata.name.clone(),
        });

        Ok(())
    }

    /// Finishes the current chunk, if we have one, and flushes the underlying
    /// [writer](Write).
    ///
    /// We finish the chunk to guarantee that the file can be streamed by future
    /// readers at least up to this point.
    /// (The alternative is to just flush the writer mid-chunk.
    /// But if we did that, and then writing was suddenly interrupted afterwards,
    /// readers would have to try to recover a half-written chunk,
    /// probably with an unfinished compresion stream.)
    ///
    /// Note that lossless compression schemes like LZ4 and Zstd improve
    /// as they go, so larger chunks will tend to have better compression.
    /// (Of course, this depends heavily on the entropy of what's being compressed!
    /// A stream of zeroes will compress great at any chunk size, and a stream
    /// of random data will compress terribly at any chunk size.)
    pub fn flush(&mut self) -> McapResult<()> {
        self.finish_chunk()?.flush()?;
        Ok(())
    }

    /// `.expect()` message when we go to write and self.writer is `None`,
    /// which should only happen when [`Writer::finish()`] was called.
    const WHERE_WRITER: &'static str = "Trying to write a record on a finished MCAP";

    /// Starts a new chunk if we haven't done so already.
    fn chunkin_time(&mut self) -> McapResult<&mut ChunkWriter<W>> {
        // Some Rust tricky: we can't move the writer out of self.writer,
        // leave that empty for a bit, and then replace it with a ChunkWriter.
        // (That would leave it in an unspecified state if we bailed here!)
        // Instead briefly swap it out for a null writer while we set up the chunker
        // The writer will only be None if finish() was called.
        let prev_writer = self.writer.take().expect(Self::WHERE_WRITER);

        self.writer = Some(match prev_writer {
            WriteMode::Raw(w) => {
                // It's chunkin time.
                WriteMode::Chunk(ChunkWriter::new(w, self.options.compression)?)
            }
            chunk => chunk,
        });

        match &mut self.writer {
            Some(WriteMode::Chunk(c)) => Ok(c),
            _ => unreachable!(),
        }
    }

    /// Finish the current chunk, if we have one.
    fn finish_chunk(&mut self) -> McapResult<&mut W> {
        // See above
        let prev_writer = self.writer.take().expect(Self::WHERE_WRITER);

        self.writer = Some(match prev_writer {
            WriteMode::Chunk(c) => {
                let (w, index) = c.finish()?;
                self.chunk_indexes.push(index);
                WriteMode::Raw(w)
            }
            raw => raw,
        });

        match &mut self.writer {
            Some(WriteMode::Raw(w)) => Ok(w),
            _ => unreachable!(),
        }
    }

    /// Finishes any current chunk and writes out the rest of the file.
    ///
    /// Subsequent calls to other methods will panic.
    pub fn finish(&mut self) -> McapResult<()> {
        if self.writer.is_none() {
            // We already called finish().
            // Maybe we're dropping after the user called it?
            return Ok(());
        }

        // Finish any chunk we were working on and update stats, indexes, etc.
        self.finish_chunk()?;

        // Grab the writer - self.writer becoming None makes subsequent writes fail.
        let mut writer = match self.writer.take() {
            // We called finish_chunk() above, so we're back to raw writes for
            // the summary section.
            Some(WriteMode::Raw(w)) => w,
            _ => unreachable!(),
        };
        let writer = &mut writer;

        // We're done with the data secton!
        write_record(writer, &Record::DataEnd(records::DataEnd::default()))?;

        // Take all the data we need, swapping in empty containers.
        // Without this, we get yelled at for moving things out of a mutable ref
        // (&mut self).
        // (We could get around all this noise by having finish() take self,
        // but then it wouldn't be droppable _and_ finish...able.)
        let mut channel_message_counts = BTreeMap::new();
        std::mem::swap(
            &mut channel_message_counts,
            &mut self.channel_message_counts,
        );

        // Grab stats before we munge all the self fields below.
        let message_bounds = self.message_bounds.unwrap_or((0, 0));
        let stats = records::Statistics {
            message_count: channel_message_counts.values().sum(),
            schema_count: self.schemas.len() as u16,
            channel_count: self.channels.len() as u32,
            attachment_count: self.attachment_indexes.len() as u32,
            metadata_count: self.metadata_indexes.len() as u32,
            chunk_count: self.chunk_indexes.len() as u32,
            message_start_time: message_bounds.0,
            message_end_time: message_bounds.1,
            channel_message_counts,
        };

        let mut chunk_indexes = Vec::new();
        std::mem::swap(&mut chunk_indexes, &mut self.chunk_indexes);

        let mut attachment_indexes = Vec::new();
        std::mem::swap(&mut attachment_indexes, &mut self.attachment_indexes);

        let mut metadata_indexes = Vec::new();
        std::mem::swap(&mut metadata_indexes, &mut self.metadata_indexes);

        // Make some Schema and Channel lists for the summary section.
        // Be sure to grab schema IDs for the channels from the schema hash map before we drain it!
        struct ChannelSummary<'a> {
            channel: Channel<'a>,
            channel_id: u16,
            schema_id: u16,
        }

        let mut all_channels: Vec<ChannelSummary<'_>> = self
            .channels
            .drain()
            .map(|(channel, channel_id)| {
                let schema_id = match &channel.schema {
                    Some(s) => *self.schemas.get(s).unwrap(),
                    None => 0,
                };

                ChannelSummary {
                    channel,
                    channel_id,
                    schema_id,
                }
            })
            .collect();
        all_channels.sort_unstable_by_key(|cs| cs.channel_id);

        let mut all_schemas: Vec<(Schema<'_>, u16)> = self.schemas.drain().collect();
        all_schemas.sort_unstable_by_key(|(_, v)| *v);

        let mut offsets = Vec::new();

        let summary_start = writer.stream_position()?;

        // Let's get a CRC of the summary section.
        let mut ccw = CountingCrcWriter::new(writer);

        fn posit<W: Write + Seek>(ccw: &mut CountingCrcWriter<W>) -> io::Result<u64> {
            ccw.get_mut().stream_position()
        }

        // Write all schemas.
        let schemas_start = summary_start;
        for (schema, id) in all_schemas {
            let header = records::SchemaHeader {
                id,
                name: schema.name,
                encoding: schema.encoding,
            };
            let data = schema.data;

            write_record(&mut ccw, &Record::Schema { header, data })?;
        }
        let schemas_end = posit(&mut ccw)?;
        if schemas_end - schemas_start > 0 {
            offsets.push(records::SummaryOffset {
                group_opcode: op::SCHEMA,
                group_start: schemas_start,
                group_length: schemas_end - schemas_start,
            });
        }

        // Write all channels.
        let channels_start = schemas_end;
        for cs in all_channels {
            let rec = records::Channel {
                id: cs.channel_id,
                schema_id: cs.schema_id,
                topic: cs.channel.topic,
                message_encoding: cs.channel.message_encoding,
                metadata: cs.channel.metadata,
            };
            write_record(&mut ccw, &Record::Channel(rec))?;
        }
        let channels_end = posit(&mut ccw)?;
        if channels_end - channels_start > 0 {
            offsets.push(records::SummaryOffset {
                group_opcode: op::CHANNEL,
                group_start: channels_start,
                group_length: channels_end - channels_start,
            });
        }

        // Write all chunk indexes.
        let chunk_indexes_start = channels_end;
        for index in chunk_indexes {
            write_record(&mut ccw, &Record::ChunkIndex(index))?;
        }
        let chunk_indexes_end = posit(&mut ccw)?;
        if chunk_indexes_end - chunk_indexes_start > 0 {
            offsets.push(records::SummaryOffset {
                group_opcode: op::CHUNK_INDEX,
                group_start: chunk_indexes_start,
                group_length: chunk_indexes_end - chunk_indexes_start,
            });
        }

        // ...and attachment indexes
        let attachment_indexes_start = chunk_indexes_end;
        for index in attachment_indexes {
            write_record(&mut ccw, &Record::AttachmentIndex(index))?;
        }
        let attachment_indexes_end = posit(&mut ccw)?;
        if attachment_indexes_end - attachment_indexes_start > 0 {
            offsets.push(records::SummaryOffset {
                group_opcode: op::ATTACHMENT_INDEX,
                group_start: attachment_indexes_start,
                group_length: attachment_indexes_end - attachment_indexes_start,
            });
        }

        // ...and metadata indexes
        let metadata_indexes_start = attachment_indexes_end;
        for index in metadata_indexes {
            write_record(&mut ccw, &Record::MetadataIndex(index))?;
        }
        let metadata_indexes_end = posit(&mut ccw)?;
        if metadata_indexes_end - metadata_indexes_start > 0 {
            offsets.push(records::SummaryOffset {
                group_opcode: op::METADATA_INDEX,
                group_start: metadata_indexes_start,
                group_length: metadata_indexes_end - metadata_indexes_start,
            });
        }

        let stats_start = metadata_indexes_end;
        write_record(&mut ccw, &Record::Statistics(stats))?;
        let stats_end = posit(&mut ccw)?;
        assert!(stats_end > stats_start);
        offsets.push(records::SummaryOffset {
            group_opcode: op::STATISTICS,
            group_start: stats_start,
            group_length: stats_end - stats_start,
        });

        // Write the summary offsets we've been accumulating
        let summary_offset_start = stats_end;
        for offset in offsets {
            write_record(&mut ccw, &Record::SummaryOffset(offset))?;
        }

        // Wat: the CRC in the footer _includes_ part of the footer.
        op_and_len(&mut ccw, op::FOOTER, 20)?;
        ccw.write_u64::<LE>(summary_start)?;
        ccw.write_u64::<LE>(summary_offset_start)?;

        let (writer, summary_crc) = ccw.finalize();

        writer.write_u32::<LE>(summary_crc)?;

        writer.write_all(MAGIC)?;
        writer.flush()?;
        Ok(())
    }
}

impl<'a, W: Write + Seek> Drop for Writer<'a, W> {
    fn drop(&mut self) {
        self.finish().unwrap()
    }
}

enum Compressor<W: Write> {
    Null(W),
    #[cfg(feature = "zstd")]
    Zstd(zstd::Encoder<'static, W>),
    #[cfg(feature = "lz4")]
    Lz4(lz4_flex::frame::FrameEncoder<W>),
}

impl<W: Write> Compressor<W> {
    fn finish(self) -> io::Result<W> {
        Ok(match self {
            Compressor::Null(w) => w,
            #[cfg(feature = "zstd")]
            Compressor::Zstd(w) => w.finish()?,
            #[cfg(feature = "lz4")]
            Compressor::Lz4(w) => w.finish()?,
        })
    }
}

impl<W: Write> Write for Compressor<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self {
            Compressor::Null(w) => w.write(buf),
            #[cfg(feature = "zstd")]
            Compressor::Zstd(w) => w.write(buf),
            #[cfg(feature = "lz4")]
            Compressor::Lz4(w) => w.write(buf),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            Compressor::Null(w) => w.flush(),
            #[cfg(feature = "zstd")]
            Compressor::Zstd(w) => w.flush(),
            #[cfg(feature = "lz4")]
            Compressor::Lz4(w) => w.flush(),
        }
    }
}

struct ChunkWriter<W: Write> {
    header_start: u64,
    stream_start: u64,
    /// Message start and end time, or None if there are no messages yet.
    message_bounds: Option<(u64, u64)>,
    compression_name: &'static str,
    compressor: CountingCrcWriter<Compressor<W>>,
    indexes: BTreeMap<u16, Vec<records::MessageIndexEntry>>,
}

impl<W: Write + Seek> ChunkWriter<W> {
    fn new(mut writer: W, compression: Option<Compression>) -> McapResult<Self> {
        let header_start = writer.stream_position()?;

        op_and_len(&mut writer, op::CHUNK, !0)?;

        let compression_name = match compression {
            #[cfg(feature = "zstd")]
            Some(Compression::Zstd) => "zstd",
            #[cfg(feature = "lz4")]
            Some(Compression::Lz4) => "lz4",
            #[cfg(not(any(feature = "zstd", feature = "lz4")))]
            Some(_) => unreachable!("`Compression` is an empty enum that cannot be instantiated"),
            None => "",
        };

        // Write a dummy header that we'll overwrite with the actual values later.
        // We just need its size (which only varies based on compression name).
        let header = records::ChunkHeader {
            message_start_time: 0,
            message_end_time: 0,
            uncompressed_size: !0,
            uncompressed_crc: !0,
            compression: String::from(compression_name),
            compressed_size: !0,
        };
        writer.write_le(&header)?;
        let stream_start = writer.stream_position()?;

        let compressor = match compression {
            #[cfg(feature = "zstd")]
            Some(Compression::Zstd) => {
                #[allow(unused_mut)]
                let mut enc = zstd::Encoder::new(writer, 0)?;
                #[cfg(not(target_arch = "wasm32"))]
                enc.multithread(num_cpus::get_physical() as u32)?;
                Compressor::Zstd(enc)
            }
            #[cfg(feature = "lz4")]
            Some(Compression::Lz4) => Compressor::Lz4(lz4_flex::frame::FrameEncoder::new(writer)),
            #[cfg(not(any(feature = "zstd", feature = "lz4")))]
            Some(_) => unreachable!("`Compression` is an empty enum that cannot be instantiated"),
            None => Compressor::Null(writer),
        };
        let compressor = CountingCrcWriter::new(compressor);
        Ok(Self {
            compressor,
            header_start,
            stream_start,
            compression_name,
            message_bounds: None,
            indexes: BTreeMap::new(),
        })
    }

    fn write_schema(&mut self, id: u16, schema: &Schema) -> McapResult<()> {
        let header = records::SchemaHeader {
            id,
            name: schema.name.clone(),
            encoding: schema.encoding.clone(),
        };
        write_record(
            &mut self.compressor,
            &Record::Schema {
                header,
                data: Cow::Borrowed(&schema.data),
            },
        )?;
        Ok(())
    }

    fn write_channel(&mut self, id: u16, schema_id: u16, chan: &Channel) -> McapResult<()> {
        assert_eq!(schema_id == 0, chan.schema.is_none());

        let rec = records::Channel {
            id,
            schema_id,
            topic: chan.topic.clone(),
            message_encoding: chan.message_encoding.clone(),
            metadata: chan.metadata.clone(),
        };

        write_record(&mut self.compressor, &Record::Channel(rec))?;
        Ok(())
    }

    fn write_message(&mut self, header: &MessageHeader, data: &[u8]) -> McapResult<()> {
        // Update min/max time for the chunk
        self.message_bounds = Some(match self.message_bounds {
            None => (header.log_time, header.log_time),
            Some((start, end)) => (start.min(header.log_time), end.max(header.log_time)),
        });

        // Add an index for this message
        self.indexes
            .entry(header.channel_id)
            .or_default()
            .push(records::MessageIndexEntry {
                log_time: header.log_time,
                offset: self.compressor.position(),
            });

        write_record(
            &mut self.compressor,
            &Record::Message {
                header: *header,
                data: Cow::Borrowed(data),
            },
        )?;
        Ok(())
    }

    fn finish(self) -> McapResult<(W, records::ChunkIndex)> {
        // Get the number of uncompressed bytes written and the CRC.

        let uncompressed_size = self.compressor.position();
        let (stream, crc) = self.compressor.finalize();
        let uncompressed_crc = crc;

        // Finalize the compression stream - it maintains an internal buffer.
        let mut writer = stream.finish()?;
        let end_of_stream = writer.stream_position()?;
        let compressed_size = end_of_stream - self.stream_start;
        let record_size = (end_of_stream - self.header_start) as usize - 9; // 1 byte op, 8 byte len

        // Back up, write our finished header, then continue at the end of the stream.
        writer.seek(SeekFrom::Start(self.header_start))?;
        op_and_len(&mut writer, op::CHUNK, record_size)?;
        let message_bounds = self.message_bounds.unwrap_or((0, 0));
        let header = records::ChunkHeader {
            message_start_time: message_bounds.0,
            message_end_time: message_bounds.1,
            uncompressed_size,
            uncompressed_crc,
            compression: String::from(self.compression_name),
            compressed_size,
        };
        writer.write_le(&header)?;
        assert_eq!(self.stream_start, writer.stream_position()?);
        assert_eq!(writer.seek(SeekFrom::End(0))?, end_of_stream);

        // Write our message indexes
        let mut message_index_offsets: BTreeMap<u16, u64> = BTreeMap::new();

        let mut index_buf = Vec::new();
        for (channel_id, records) in self.indexes {
            assert!(message_index_offsets
                .insert(channel_id, writer.stream_position()?)
                .is_none());
            index_buf.clear();
            let index = records::MessageIndex {
                channel_id,
                records,
            };

            Cursor::new(&mut index_buf).write_le(&index)?;
            op_and_len(&mut writer, op::MESSAGE_INDEX, index_buf.len())?;
            writer.write_all(&index_buf)?;
        }
        let end_of_indexes = writer.stream_position()?;

        let index = records::ChunkIndex {
            message_start_time: header.message_start_time,
            message_end_time: header.message_end_time,
            chunk_start_offset: self.header_start,
            chunk_length: end_of_stream - self.header_start,
            message_index_offsets,
            message_index_length: end_of_indexes - end_of_stream,
            compression: header.compression,
            compressed_size: header.compressed_size,
            uncompressed_size: header.uncompressed_size,
        };

        Ok((writer, index))
    }
}
