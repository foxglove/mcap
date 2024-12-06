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
    chunk_sink::{ChunkMode, ChunkSink},
    io_utils::CountingCrcWriter,
    records::{self, op, AttachmentHeader, AttachmentIndex, MessageHeader, Record},
    Attachment, Channel, Compression, McapError, McapResult, Message, Schema, MAGIC,
};

// re-export to help with linear writing
pub use binrw::io::NoSeek;

pub use records::Metadata;

enum WriteMode<W: Write + Seek> {
    Raw(W, ChunkMode),
    Chunk(ChunkWriter<W>),
    Attachment(AttachmentWriter<W>, ChunkMode),
}

fn op_and_len<W: Write>(w: &mut W, op: u8, len: u64) -> io::Result<()> {
    w.write_u8(op)?;
    w.write_u64::<LE>(len)?;
    Ok(())
}

fn write_record<W: Write>(w: &mut W, r: &Record) -> io::Result<()> {
    // Annoying: our stream isn't Seek if we're writing to a compressed chunk stream,
    // so we need an intermediate buffer.
    macro_rules! record {
        ($op:expr, $b:ident) => {{
            let mut rec_buf = Vec::new();
            Cursor::new(&mut rec_buf).write_le($b).unwrap();

            op_and_len(w, $op, rec_buf.len() as _)?;
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
                (header_buf.len() + size_of::<u32>() + data.len()) as _,
            )?;
            w.write_all(&header_buf)?;
            w.write_u32::<LE>(data.len() as u32)?;
            w.write_all(data)?;
        }
        Record::Channel(c) => record!(op::CHANNEL, c),
        Record::Message { header, data } => {
            let mut header_buf = Vec::new();
            Cursor::new(&mut header_buf).write_le(header).unwrap();

            op_and_len(w, op::MESSAGE, (header_buf.len() + data.len()) as _)?;
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
        Record::Attachment { .. } => {
            unreachable!("Attachments handle their own serialization to handle large files")
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
    use_chunks: bool,
    disable_seeking: bool,
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
            use_chunks: true,
            disable_seeking: false,
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
        Self { chunk_size, ..self }
    }

    /// specifies whether to use chunks for storing messages.
    ///
    /// If `false`, messages will be written directly to the data section of the file.
    /// This prevents using compression or indexing, but may be useful on small embedded systems
    /// that cannot afford the memory overhead of storing chunk metadata for the entire recording.
    ///
    /// Note that it's often useful to post-process a non-chunked file using `mcap recover` to add
    /// indexes for efficient processing.
    pub fn use_chunks(self, use_chunks: bool) -> Self {
        Self { use_chunks, ..self }
    }

    /// Specifies whether the writer should seek or not.
    ///
    /// Setting `true` will allow you to use [`NoSeek`] on the destination writer to support
    /// writing to a stream that does not support [`Seek`].
    ///
    /// By default the writer will seek the output to avoid buffering in memory. Seeking is an
    /// optimization and should only be disabled if the output is using [`NoSeek`].
    pub fn disable_seeking(mut self, disable_seeking: bool) -> Self {
        self.disable_seeking = disable_seeking;
        self
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

        // If both the `use_chunks` and `disable_seeking` options are enabled set the chunk
        // mode and pre-allocate the buffer. Checking both avoids allocating the temporary buffer
        // if seeking is disabled but chunking is not.
        let chunk_mode = if opts.use_chunks && opts.disable_seeking {
            let buffer_size = opts.chunk_size.unwrap_or_default();

            let buffer = Vec::with_capacity(
                buffer_size
                    .try_into()
                    .map_err(|_| McapError::ChunkBufferTooLarge(buffer_size))?,
            );

            ChunkMode::Buffered { buffer }
        } else {
            ChunkMode::Direct
        };

        Ok(Self {
            writer: Some(WriteMode::Raw(writer, chunk_mode)),
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
        if self.options.use_chunks {
            self.chunkin_time()?
                .write_channel(next_channel_id, schema_id, chan)?;
        } else {
            write_record(
                self.finish_chunk()?,
                &Record::Channel(records::Channel {
                    id: next_channel_id,
                    schema_id,
                    topic: chan.topic.clone(),
                    message_encoding: chan.message_encoding.clone(),
                    metadata: chan.metadata.clone(),
                }),
            )?;
        }
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
        if self.options.use_chunks {
            self.chunkin_time()?.write_schema(next_schema_id, schema)?;
        } else {
            write_record(
                self.finish_chunk()?,
                &Record::Schema {
                    header: records::SchemaHeader {
                        id: next_schema_id,
                        name: schema.name.clone(),
                        encoding: schema.encoding.clone(),
                    },
                    data: Cow::Borrowed(&schema.data),
                },
            )?;
        }
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

        if self.options.use_chunks {
            self.chunkin_time()?.write_message(header, data)?;
        } else {
            write_record(
                self.finish_chunk()?,
                &Record::Message {
                    header: *header,
                    data: Cow::Borrowed(data),
                },
            )?;
        }
        Ok(())
    }

    /// Start writing an attachment.
    ///
    /// This is a low level API. For small attachments, use [`Self::attach`].
    ///
    /// To start writing an attachment call this method with the [`AttachmentHeader`] as well as
    /// the length of the attachment in bytes. It is important this length is exact otherwise the
    /// writer will be left in an error state.
    ///
    /// This call should be followed by one or more calls to [`Self::put_attachment_bytes`].
    ///
    /// Once all attachment bytes have been written the attachment must be completed with a call to
    /// [`Self::finish_attachment`]. Failing to finish the attachment will leave the write in an
    /// error state.
    ///
    /// # Example
    /// ```rust
    /// # use mcap::write::Writer;
    /// # use mcap::records::AttachmentHeader;
    /// #
    /// # fn run() -> Result<(), Box<dyn std::error::Error>> {
    /// # let mut output = vec![];
    /// # let mut writer = Writer::new(std::io::Cursor::new(&mut output))?;
    /// let attachment_length = 6;
    ///
    /// // Start the attachment
    /// writer.start_attachment(attachment_length, AttachmentHeader {
    ///     log_time: 100,
    ///     create_time: 200,
    ///     name: "my-attachment".into(),
    ///     media_type: "application/octet-stream".into()
    /// })?;
    ///
    /// // Write all the bytes for the attachment. The amount of bytes written must
    /// // match the length specified when the attachment was started.
    /// writer.put_attachment_bytes(&[ 1, 2, 3, 4 ])?;
    /// writer.put_attachment_bytes(&[ 5, 6 ])?;
    ///
    /// // Finsh writing the attachment.
    /// writer.finish_attachment()?;
    /// #
    /// # Ok(())
    /// # }
    /// # run().expect("should succeed");
    /// ```
    pub fn start_attachment(
        &mut self,
        attachment_length: u64,
        header: AttachmentHeader,
    ) -> McapResult<()> {
        self.finish_chunk()?;

        let prev_writer = self.writer.take().expect(Self::WHERE_WRITER);

        let WriteMode::Raw(w, chunk_mode) = prev_writer else {
            panic!(
                "since finish_chunk was called, write mode is guaranteed to be raw at this point"
            );
        };

        self.writer = Some(WriteMode::Attachment(
            AttachmentWriter::new(w, attachment_length, header)?,
            chunk_mode,
        ));

        Ok(())
    }

    /// Write bytes to the current attachment.
    ///
    /// This is a low level API. For small attachments, use [`Self::attach`].
    ///
    /// Before calling this method call [`Self::start_attachment`].
    pub fn put_attachment_bytes(&mut self, bytes: &[u8]) -> McapResult<()> {
        let Some(WriteMode::Attachment(writer, _)) = &mut self.writer else {
            return Err(McapError::AttachmentNotInProgress);
        };

        writer.put_bytes(bytes)?;

        Ok(())
    }

    /// Finish the current attachment.
    ///
    /// This is a low level API. For small attachments, use [`Self::attach`].
    ///
    /// Before calling this method call [`Self::start_attachment`] and write bytes to the
    /// attachment using [`Self::put_attachment_bytes`].
    pub fn finish_attachment(&mut self) -> McapResult<()> {
        let Some(WriteMode::Attachment(..)) = &mut self.writer else {
            return Err(McapError::AttachmentNotInProgress);
        };

        let Some(WriteMode::Attachment(writer, chunk_mode)) = self.writer.take() else {
            panic!("WriteMode is guaranteed to be attachment by this point");
        };

        let (writer, attachment_index) = writer.finish()?;

        self.attachment_indexes.push(attachment_index);

        self.writer = Some(WriteMode::Raw(writer, chunk_mode));

        Ok(())
    }

    /// Write an attachment to the MCAP file
    pub fn attach(&mut self, attachment: &Attachment) -> McapResult<()> {
        let header = records::AttachmentHeader {
            log_time: attachment.log_time,
            create_time: attachment.create_time,
            name: attachment.name.clone(),
            media_type: attachment.media_type.clone(),
        };

        self.start_attachment(attachment.data.len() as _, header)?;
        self.put_attachment_bytes(&attachment.data[..])?;
        self.finish_attachment()?;

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
        // It is not possible to start writing a chunk if we're still writing an attachment. Return
        // an error instead.
        if let Some(WriteMode::Attachment(..)) = self.writer {
            return Err(McapError::AttachmentNotInProgress);
        }

        // Some Rust tricky: we can't move the writer out of self.writer,
        // leave that empty for a bit, and then replace it with a ChunkWriter.
        // (That would leave it in an unspecified state if we bailed here!)
        // Instead briefly swap it out for a null writer while we set up the chunker
        // The writer will only be None if finish() was called.
        assert!(
            self.options.use_chunks,
            "Trying to write to a chunk when chunking is disabled"
        );

        let prev_writer = self.writer.take().expect(Self::WHERE_WRITER);

        self.writer = Some(match prev_writer {
            WriteMode::Raw(w, chunk_mode) => {
                // It's chunkin time.
                WriteMode::Chunk(ChunkWriter::new(w, self.options.compression, chunk_mode)?)
            }
            chunk => chunk,
        });

        let Some(WriteMode::Chunk(c)) = &mut self.writer else {
            unreachable!("we're not in an attachment and write mode was set to chunk above")
        };

        Ok(c)
    }

    /// Finish the current chunk, if we have one.
    fn finish_chunk(&mut self) -> McapResult<&mut W> {
        // If we're currently writing an attachment then we're not writing a chunk. Return an
        // error instead.
        if let Some(WriteMode::Attachment(..)) = self.writer {
            return Err(McapError::AttachmentNotInProgress);
        }

        // See above
        let prev_writer = self.writer.take().expect(Self::WHERE_WRITER);

        self.writer = Some(match prev_writer {
            WriteMode::Chunk(c) => {
                let (w, mode, index) = c.finish()?;
                self.chunk_indexes.push(index);
                WriteMode::Raw(w, mode)
            }
            mode => mode,
        });

        let Some(WriteMode::Raw(w, _)) = &mut self.writer else {
            unreachable!("we're not in an attachment and write mode raw was set above")
        };

        Ok(w)
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
            Some(WriteMode::Raw(w, _)) => w,
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

        let chunk_indexes_end;
        if self.options.use_chunks {
            // Write all chunk indexes.
            let chunk_indexes_start = channels_end;
            for index in chunk_indexes {
                write_record(&mut ccw, &Record::ChunkIndex(index))?;
            }
            chunk_indexes_end = posit(&mut ccw)?;
            if chunk_indexes_end - chunk_indexes_start > 0 {
                offsets.push(records::SummaryOffset {
                    group_opcode: op::CHUNK_INDEX,
                    group_start: chunk_indexes_start,
                    group_length: chunk_indexes_end - chunk_indexes_start,
                });
            }
        } else {
            chunk_indexes_end = channels_end;
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
    Lz4(lz4::Encoder<W>),
}

impl<W: Write> Compressor<W> {
    fn finish(self) -> io::Result<W> {
        Ok(match self {
            Compressor::Null(w) => w,
            #[cfg(feature = "zstd")]
            Compressor::Zstd(w) => w.finish()?,
            #[cfg(feature = "lz4")]
            Compressor::Lz4(w) => {
                let (output, result) = w.finish();
                result?;
                output
            }
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
    chunk_offset: u64,
    header_start: u64,
    data_start: u64,

    /// Message start and end time, or None if there are no messages yet.
    message_bounds: Option<(u64, u64)>,
    compression_name: &'static str,
    compressor: CountingCrcWriter<Compressor<ChunkSink<W>>>,
    indexes: BTreeMap<u16, Vec<records::MessageIndexEntry>>,
}

impl<W: Write + Seek> ChunkWriter<W> {
    fn new(mut writer: W, compression: Option<Compression>, mode: ChunkMode) -> McapResult<Self> {
        let chunk_offset = writer.stream_position()?;

        let mut sink = match mode {
            ChunkMode::Buffered { mut buffer } => {
                // ensure the buffer is empty before using it for the chunk
                buffer.truncate(0);
                ChunkSink::Buffered(writer, Cursor::new(buffer))
            }
            ChunkMode::Direct => ChunkSink::Direct(writer),
        };

        let header_start = sink.stream_position()?;

        op_and_len(&mut sink, op::CHUNK, !0)?;

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
        sink.write_le(&header)?;
        let data_start = sink.stream_position()?;

        let compressor = match compression {
            #[cfg(feature = "zstd")]
            Some(Compression::Zstd) => {
                #[allow(unused_mut)]
                let mut enc = zstd::Encoder::new(sink, 0)?;
                #[cfg(not(target_arch = "wasm32"))]
                enc.multithread(num_cpus::get_physical() as u32)?;
                Compressor::Zstd(enc)
            }
            #[cfg(feature = "lz4")]
            Some(Compression::Lz4) => Compressor::Lz4(lz4::EncoderBuilder::new().build(sink)?),
            #[cfg(not(any(feature = "zstd", feature = "lz4")))]
            Some(_) => unreachable!("`Compression` is an empty enum that cannot be instantiated"),
            None => Compressor::Null(sink),
        };
        let compressor = CountingCrcWriter::new(compressor);
        Ok(Self {
            chunk_offset,
            data_start,
            header_start,

            compressor,
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

    fn finish(self) -> McapResult<(W, ChunkMode, records::ChunkIndex)> {
        // Get the number of uncompressed bytes written and the CRC.

        let uncompressed_size = self.compressor.position();
        let (stream, crc) = self.compressor.finalize();
        let uncompressed_crc = crc;

        // Finalize the compression stream - it maintains an internal buffer.
        let mut sink = stream.finish()?;
        let data_end = sink.stream_position()?;
        let compressed_size = data_end - self.data_start;
        let record_size = (data_end - self.header_start) - 9; // 1 byte op, 8 byte len

        // Back up, write our finished header, then continue at the end of the stream.
        sink.seek(SeekFrom::Start(self.header_start))?;
        op_and_len(&mut sink, op::CHUNK, record_size)?;
        let message_bounds = self.message_bounds.unwrap_or((0, 0));
        let header = records::ChunkHeader {
            message_start_time: message_bounds.0,
            message_end_time: message_bounds.1,
            uncompressed_size,
            uncompressed_crc,
            compression: String::from(self.compression_name),
            compressed_size,
        };
        sink.write_le(&header)?;
        assert_eq!(self.data_start, sink.stream_position()?);
        assert_eq!(sink.seek(SeekFrom::End(0))?, data_end);

        // Write our message indexes
        let mut message_index_offsets: BTreeMap<u16, u64> = BTreeMap::new();

        let mut index_buf = Vec::new();
        for (channel_id, records) in self.indexes {
            // since the chunk sink position might be relative to a buffer, calculate the absolute
            // position based on the `chunk_offset` and `header_start`.
            //
            // When the chunk is buffered and stream position is "relative" then `header_start`
            // will be zero so the position will be relative to the chunk offset. When the stream
            // position is "absolute" then `header_start` and `chunk_offset` will be equal and the
            // position will be absolute.
            let absolute_position =
                (self.chunk_offset - self.header_start) + sink.stream_position()?;

            let existing_offset = message_index_offsets.insert(channel_id, absolute_position);

            assert!(existing_offset.is_none());

            index_buf.clear();
            let index = records::MessageIndex {
                channel_id,
                records,
            };

            Cursor::new(&mut index_buf).write_le(&index)?;
            op_and_len(&mut sink, op::MESSAGE_INDEX, index_buf.len() as _)?;
            sink.write_all(&index_buf)?;
        }
        let end_of_indexes = sink.stream_position()?;

        let index = records::ChunkIndex {
            message_start_time: header.message_start_time,
            message_end_time: header.message_end_time,
            chunk_start_offset: self.chunk_offset,
            chunk_length: data_end - self.header_start,
            message_index_offsets,
            message_index_length: end_of_indexes - data_end,
            compression: header.compression,
            compressed_size: header.compressed_size,
            uncompressed_size: header.uncompressed_size,
        };

        let (writer, mode) = match sink {
            ChunkSink::Direct(w) => (w, ChunkMode::Direct),
            ChunkSink::Buffered(mut w, data) => {
                let buffer = data.into_inner();
                w.write_all(&buffer)?;
                (w, ChunkMode::Buffered { buffer })
            }
        };

        Ok((writer, mode, index))
    }
}

struct AttachmentWriter<W> {
    record_offset: u64,
    attachment_offset: u64,
    attachment_length: u64,
    header: AttachmentHeader,
    writer: CountingCrcWriter<W>,
}

impl<W: Write + Seek> AttachmentWriter<W> {
    /// Create a new [`AttachmentWriter`] and write the attachment header to the output.
    fn new(mut writer: W, attachment_length: u64, header: AttachmentHeader) -> McapResult<Self> {
        let record_offset = writer.stream_position()?;

        // We have to write to a temporary buffer here as the CountingCrcWriter doesn't support
        // seeking.
        let mut header_buf = vec![];
        Cursor::new(&mut header_buf).write_le(&header)?;

        op_and_len(
            &mut writer,
            op::ATTACHMENT,
            header_buf.len() as u64
                // attachment_length
                + size_of::<u64>() as u64
                // attachment
                + attachment_length
                // crc
                + size_of::<u32>() as u64,
        )?;

        let mut writer = CountingCrcWriter::new(writer);
        writer.write_all(&header_buf)?;
        writer.write_u64::<LE>(attachment_length)?;

        let attachment_offset = writer.position();

        Ok(Self {
            record_offset,
            attachment_offset,
            attachment_length,
            header,
            writer,
        })
    }

    /// Write bytes to the attachment.
    ///
    /// This method will return an error if the provided bytes exceed the space remaining in the
    /// attachment.
    fn put_bytes(&mut self, bytes: &[u8]) -> McapResult<()> {
        let attachment_position = self.writer.position() - self.attachment_offset;

        let space = self.attachment_length - attachment_position;
        let byte_length = bytes.len() as u64;

        if byte_length > space {
            return Err(McapError::AttachmentTooLarge {
                excess: byte_length - space,
                attachment_length: self.attachment_length,
            });
        }

        self.writer.write_all(bytes)?;
        Ok(())
    }

    /// Finish the attachment and write the CRC to the output, returning the [`AttachmentIndex`]
    /// for the written attachment.
    fn finish(self) -> McapResult<(W, AttachmentIndex)> {
        let expected = self.attachment_length;
        let current = self.writer.position() - self.attachment_offset;

        if expected != current {
            return Err(McapError::AttachmentIncomplete { expected, current });
        }

        let (mut writer, crc) = self.writer.finalize();
        writer.write_u32::<LE>(crc)?;

        let offset = self.record_offset;
        let length = writer.stream_position()? - offset;

        Ok((
            writer,
            AttachmentIndex {
                offset,
                length,
                log_time: self.header.log_time,
                media_type: self.header.media_type,
                name: self.header.name,
                create_time: self.header.create_time,
                data_size: self.attachment_length,
            },
        ))
    }
}
