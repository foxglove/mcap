//! Write MCAP files

use std::{
    borrow::Cow,
    collections::BTreeMap,
    io::{self, prelude::*, Cursor, SeekFrom},
    mem::size_of,
};

use bimap::BiHashMap;
use binrw::prelude::*;
use byteorder::{WriteBytesExt, LE};
#[cfg(feature = "zstd")]
use zstd::stream::{raw as zraw, zio};

use crate::{
    chunk_sink::{ChunkMode, ChunkSink},
    io_utils::CountingCrcWriter,
    records::{self, op, AttachmentHeader, AttachmentIndex, MessageHeader, Record},
    Attachment, Compression, McapError, McapResult, Message, Schema, MAGIC,
};

// re-export to help with linear writing
pub use binrw::io::NoSeek;

pub use records::Metadata;

enum WriteMode<W: Write + Seek> {
    Raw(CountingCrcWriter<W>),
    Chunk(ChunkWriter<W>),
    Attachment(AttachmentWriter<CountingCrcWriter<W>>),
}

fn op_and_len<W: Write>(w: &mut W, op: u8, len: u64) -> io::Result<()> {
    w.write_u8(op)?;
    w.write_u64::<LE>(len)?;
    Ok(())
}

fn write_record<W: Write>(mut w: &mut W, r: &Record) -> io::Result<()> {
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
            let header_len = header.serialized_len();
            op_and_len(w, op::MESSAGE, header_len + data.len() as u64)?;
            NoSeek::new(&mut w)
                .write_le(header)
                .map_err(io::Error::other)?;
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
    library: String,
    chunk_size: Option<u64>,
    use_chunks: bool,
    disable_seeking: bool,
    emit_statistics: bool,
    emit_summary_offsets: bool,
    emit_message_indexes: bool,
    emit_chunk_indexes: bool,
    emit_attachment_indexes: bool,
    emit_metadata_indexes: bool,
    repeat_channels: bool,
    repeat_schemas: bool,
    calculate_chunk_crcs: bool,
    calculate_data_section_crc: bool,
    calculate_summary_section_crc: bool,
    calculate_attachment_crcs: bool,
}

impl Default for WriteOptions {
    fn default() -> Self {
        Self {
            #[cfg(feature = "zstd")]
            compression: Some(Compression::Zstd),
            #[cfg(not(feature = "zstd"))]
            compression: None,
            profile: String::new(),
            library: String::from("mcap-rs-") + env!("CARGO_PKG_VERSION"),
            chunk_size: Some(1024 * 768),
            use_chunks: true,
            disable_seeking: false,
            emit_statistics: true,
            emit_summary_offsets: true,
            emit_message_indexes: true,
            emit_chunk_indexes: true,
            emit_attachment_indexes: true,
            emit_metadata_indexes: true,
            repeat_channels: true,
            repeat_schemas: true,
            calculate_chunk_crcs: true,
            calculate_data_section_crc: true,
            calculate_summary_section_crc: true,
            calculate_attachment_crcs: true,
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

    /// specifies the library that should be written to the MCAP Header record.
    /// This is a free-form string that can be used to identify the library that wrote the file.
    /// It is not used for any other purpose.
    pub fn library<S: Into<String>>(self, library: S) -> Self {
        Self {
            library: library.into(),
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

    /// Specifies in whether to write any records to the [summary
    /// section](https://mcap.dev/spec#summary-section).
    ///
    /// If you want only want to include specific record types in the summary section, call this
    /// method with `false` and then enable the records you want. This ensures that no unwanted
    /// summary records will be written if the format changes in the future.
    ///
    /// Note that this does *not* control whether [summary offset
    /// records](https://mcap.dev/spec#summary-offset-op0x0e) are written, because they
    /// are not part of the [summary section](https://mcap.dev/spec#summary-section).
    pub fn emit_summary_records(mut self, value: bool) -> Self {
        self.emit_statistics = value;
        self.emit_chunk_indexes = value;
        self.emit_attachment_indexes = value;
        self.emit_metadata_indexes = value;
        self.repeat_channels = value;
        self.repeat_schemas = value;
        self
    }

    /// Specifies whether to write [summary offset
    /// records](https://mcap.dev/spec#summary-offset-op0x0e). This is on by default.
    pub fn emit_summary_offsets(mut self, emit_summary_offsets: bool) -> Self {
        self.emit_summary_offsets = emit_summary_offsets;
        self
    }

    /// Specifies whether to write a [statistics record](https://mcap.dev/spec#statistics-op0x0b) in
    /// the [summary section](https://mcap.dev/spec#summary-section). This is on by default.
    pub fn emit_statistics(mut self, emit_statistics: bool) -> Self {
        self.emit_statistics = emit_statistics;
        self
    }

    /// Specifies whether to write [message index
    /// records](https://mcap.dev/spec#message-index-op0x07) after each chunk. This is on by
    /// default.
    pub fn emit_message_indexes(mut self, emit_message_indexes: bool) -> Self {
        self.emit_message_indexes = emit_message_indexes;
        self
    }

    /// Specifies whether to write [chunk index records](https://mcap.dev/spec#chunk-index-op0x08)
    /// in the [summary section](https://mcap.dev/spec#summary-section). This is on by default.
    pub fn emit_chunk_indexes(mut self, emit_chunk_indexes: bool) -> Self {
        self.emit_chunk_indexes = emit_chunk_indexes;
        self
    }

    /// Specifies whether to write [attachment index
    /// records](https://mcap.dev/spec#attachment-index-op0x0a) in the [summary
    /// section](https://mcap.dev/spec#summary-section). This is on by default.
    pub fn emit_attachment_indexes(mut self, emit_attachment_indexes: bool) -> Self {
        self.emit_attachment_indexes = emit_attachment_indexes;
        self
    }

    /// Specifies whether to write [metadata index
    /// records](https://mcap.dev/spec#metadata-index-op0x0d) in the [summary
    /// section](https://mcap.dev/spec#summary-section). This is on by default.
    pub fn emit_metadata_indexes(mut self, emit_metadata_indexes: bool) -> Self {
        self.emit_metadata_indexes = emit_metadata_indexes;
        self
    }

    /// Specifies whether to repeat each [channel record](https://mcap.dev/spec#channel-op0x04) from
    /// the [data section](https://mcap.dev/spec#data-section) in the [summary
    /// section](https://mcap.dev/spec#summary-section). This is on by default.
    pub fn repeat_channels(mut self, repeat_channels: bool) -> Self {
        self.repeat_channels = repeat_channels;
        self
    }

    /// Specifies whether to repeat each [schema record](https://mcap.dev/spec#schema-op0x03) from
    /// the [data section](https://mcap.dev/spec#data-section) in the [summary
    /// section](https://mcap.dev/spec#summary-section). This is on by default.
    pub fn repeat_schemas(mut self, repeat_schemas: bool) -> Self {
        self.repeat_schemas = repeat_schemas;
        self
    }

    /// Creates a [`Writer`] whch writes to `w` using the given options
    pub fn create<W: Write + Seek>(self, w: W) -> McapResult<Writer<W>> {
        Writer::with_options(w, self)
    }

    /// Specifies whether to calculate and write CRCs for chunk records. This is on by default.
    pub fn calculate_chunk_crcs(mut self, calculate_chunk_crcs: bool) -> Self {
        self.calculate_chunk_crcs = calculate_chunk_crcs;
        self
    }

    /// Specifies whether to calculate and write a data section CRC into the DataEnd record. This is on by default.
    pub fn calculate_data_section_crc(mut self, calculate_data_section_crc: bool) -> Self {
        self.calculate_data_section_crc = calculate_data_section_crc;
        self
    }

    /// Specifies whether to calculate and write a summary section CRC into the Footer record. This is on by default.
    pub fn calculate_summary_section_crc(mut self, calculate_summary_section_crc: bool) -> Self {
        self.calculate_summary_section_crc = calculate_summary_section_crc;
        self
    }

    /// Specifies whether to calculate and write a CRC for attachments. This is on by default.
    pub fn calculate_attachment_crcs(mut self, calculate_attachment_crcs: bool) -> Self {
        self.calculate_attachment_crcs = calculate_attachment_crcs;
        self
    }
}

#[derive(Hash, PartialEq, Eq)]
struct ChannelContent<'a> {
    topic: Cow<'a, str>,
    schema_id: u16,
    message_encoding: Cow<'a, str>,
    metadata: Cow<'a, BTreeMap<String, String>>,
}

#[derive(Hash, PartialEq, Eq)]
struct SchemaContent<'a> {
    name: Cow<'a, str>,
    encoding: Cow<'a, str>,
    data: Cow<'a, [u8]>,
}

/// Writes an MCAP file to the given [writer](Write).
///
/// Users should call [`finish()`](Self::finish) to flush the stream
/// and check for errors when done; otherwise the result will be unwrapped on drop.
pub struct Writer<W: Write + Seek> {
    writer: Option<WriteMode<W>>,
    is_finished: bool,
    chunk_mode: ChunkMode,
    options: WriteOptions,
    canonical_schemas: BiHashMap<SchemaContent<'static>, u16>,
    all_channel_ids: BTreeMap<u16, u16>,
    canonical_channels: BiHashMap<ChannelContent<'static>, u16>,
    all_schema_ids: BTreeMap<u16, u16>,
    next_schema_id: u16,
    next_channel_id: u16,
    chunk_indexes: Vec<records::ChunkIndex>,
    attachment_count: u32,
    attachment_indexes: Vec<records::AttachmentIndex>,
    metadata_count: u32,
    metadata_indexes: Vec<records::MetadataIndex>,
    /// Message start and end time, or None if there are no messages yet.
    message_bounds: Option<(u64, u64)>,
    channel_message_counts: BTreeMap<u16, u64>,
}

impl<W: Write + Seek> Writer<W> {
    pub fn new(writer: W) -> McapResult<Self> {
        Self::with_options(writer, WriteOptions::default())
    }

    fn with_options(writer: W, opts: WriteOptions) -> McapResult<Self> {
        let mut writer = CountingCrcWriter::new(writer, opts.calculate_data_section_crc);
        writer.write_all(MAGIC)?;

        write_record(
            &mut writer,
            &Record::Header(records::Header {
                profile: opts.profile.clone(),
                library: opts.library.clone(),
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
            writer: Some(WriteMode::Raw(writer)),
            is_finished: false,
            options: opts,
            chunk_mode,
            canonical_schemas: Default::default(),
            canonical_channels: Default::default(),
            all_channel_ids: Default::default(),
            all_schema_ids: Default::default(),
            next_channel_id: 1,
            next_schema_id: 1,
            chunk_indexes: Default::default(),
            attachment_count: 0,
            attachment_indexes: Default::default(),
            metadata_count: 0,
            metadata_indexes: Default::default(),
            message_bounds: None,
            channel_message_counts: BTreeMap::new(),
        })
    }

    /// Adds a schema, returning its ID. If a schema with the same content has been added already,
    /// its ID is returned.
    ///
    /// * `name`: an identifier for the schema.
    /// * `encoding`: Describes the schema format.  The [well-known schema
    ///   encodings](https://mcap.dev/spec/registry#well-known-schema-encodings) are preferred. An
    ///   empty string indicates no schema is available.
    /// * `data`: The serialized schema content. If `encoding` is an empty string, `data` should
    ///   have zero length.
    pub fn add_schema(&mut self, name: &str, encoding: &str, data: &[u8]) -> McapResult<u16> {
        if let Some(&id) = self.canonical_schemas.get_by_left(&SchemaContent {
            name: name.into(),
            encoding: encoding.into(),
            data: data.into(),
        }) {
            return Ok(id);
        }
        while self.all_schema_ids.contains_key(&self.next_schema_id) {
            if self.next_schema_id == u16::MAX {
                return Err(McapError::TooManySchemas);
            }
            self.next_schema_id += 1;
        }
        let id = self.next_schema_id;
        self.next_schema_id += 1;
        self.write_schema(Schema {
            id,
            name: name.into(),
            encoding: encoding.into(),
            data: Cow::Owned(data.into()),
        })?;
        Ok(id)
    }

    fn write_schema(&mut self, schema: Schema) -> McapResult<()> {
        let content = SchemaContent {
            name: Cow::Owned(schema.name.clone()),
            encoding: Cow::Owned(schema.encoding.clone()),
            data: Cow::Owned(schema.data.clone().into_owned()),
        };
        if let Some(canonical_id) = self.canonical_schemas.get_by_left(&content) {
            assert!(self
                .all_schema_ids
                .insert(schema.id, *canonical_id)
                .is_none());
        } else {
            assert!(!self
                .canonical_schemas
                .insert(content, schema.id)
                .did_overwrite());
            assert!(self.all_schema_ids.insert(schema.id, schema.id).is_none());
        }

        if self.options.use_chunks {
            self.start_chunk()?.write_schema(schema)
        } else {
            let header = records::SchemaHeader {
                id: schema.id,
                name: schema.name,
                encoding: schema.encoding,
            };
            Ok(write_record(
                &mut self.finish_chunk()?,
                &Record::Schema {
                    header,
                    data: schema.data,
                },
            )?)
        }
    }

    /// Adds a channel, returning its ID. If a channel with equivalent content was added previously,
    /// its ID is returned.
    ///
    /// Useful with subequent calls to [`write_to_known_channel()`](Self::write_to_known_channel).
    ///
    /// * `schema_id`: a schema_id returned from [`Self::add_schema`], or 0 if the channel has no
    ///    schema.
    /// * `topic`: The topic name.
    /// * `message_encoding`: Encoding for messages on this channel. The [well-known message
    ///    encodings](https://mcap.dev/spec/registry#well-known-message-encodings) are preferred.
    ///  * `metadata`: Metadata about this channel.
    pub fn add_channel(
        &mut self,
        schema_id: u16,
        topic: &str,
        message_encoding: &str,
        metadata: &BTreeMap<String, String>,
    ) -> McapResult<u16> {
        if let Some(&id) = self.canonical_channels.get_by_left(&ChannelContent {
            topic: Cow::Borrowed(topic),
            schema_id,
            message_encoding: Cow::Borrowed(message_encoding),
            metadata: Cow::Borrowed(metadata),
        }) {
            return Ok(id);
        }
        if schema_id != 0 && !self.all_schema_ids.contains_key(&schema_id) {
            return Err(McapError::UnknownSchema(topic.into(), schema_id));
        }

        while self.all_channel_ids.contains_key(&self.next_channel_id) {
            if self.next_channel_id == u16::MAX {
                return Err(McapError::TooManyChannels);
            }
            self.next_channel_id += 1;
        }
        let id = self.next_channel_id;
        self.next_channel_id += 1;

        self.write_channel(records::Channel {
            id,
            schema_id,
            topic: topic.into(),
            message_encoding: message_encoding.into(),
            metadata: metadata.clone(),
        })?;
        Ok(id)
    }

    fn write_channel(&mut self, channel: records::Channel) -> McapResult<()> {
        let content = ChannelContent {
            topic: Cow::Owned(channel.topic.clone()),
            schema_id: channel.schema_id,
            message_encoding: Cow::Owned(channel.message_encoding.clone()),
            metadata: Cow::Owned(channel.metadata.clone()),
        };
        if let Some(canonical_id) = self.canonical_channels.get_by_left(&content) {
            assert!(self
                .all_channel_ids
                .insert(channel.id, *canonical_id)
                .is_none());
        } else {
            assert!(!self
                .canonical_channels
                .insert(content, channel.id)
                .did_overwrite());
            assert!(self
                .all_channel_ids
                .insert(channel.id, channel.id)
                .is_none());
        }
        if self.options.use_chunks {
            self.start_chunk()?.write_channel(channel)
        } else {
            Ok(write_record(
                self.finish_chunk()?,
                &Record::Channel(channel),
            )?)
        }
    }

    /// Write the given message (and its provided channel, if not already added).
    /// The provided channel ID and schema ID will be used as IDs in the resulting MCAP.
    pub fn write(&mut self, message: &Message) -> McapResult<()> {
        if let Some(schema) = message.channel.schema.as_ref() {
            match self.all_schema_ids.get(&schema.id) {
                Some(previous) => {
                    // ensure that this message schema does not conflict with the existing one's content
                    let current = SchemaContent {
                        name: Cow::Borrowed(&schema.name),
                        encoding: Cow::Borrowed(&schema.encoding),
                        data: Cow::Borrowed(&schema.data),
                    };
                    if *self.canonical_schemas.get_by_right(previous).expect(
                        "id should exist in canonical_schemas for every value in all_schema_ids",
                    ) != current
                    {
                        return Err(McapError::ConflictingSchemas(schema.name.clone()));
                    }
                }
                None => {
                    self.write_schema(schema.as_ref().clone())?;
                }
            }
        }
        let schema_id = match message.channel.schema.as_ref() {
            None => 0,
            Some(schema) => schema.id,
        };
        match self.all_channel_ids.get(&message.channel.id) {
            Some(canonical) => {
                let current = ChannelContent {
                    topic: Cow::Borrowed(&message.channel.topic),
                    schema_id,
                    message_encoding: Cow::Borrowed(&message.channel.message_encoding),
                    metadata: Cow::Borrowed(&message.channel.metadata),
                };
                if *self.canonical_channels.get_by_right(canonical).expect(
                    "key should exist in canonical_channels for every value in all_channel_ids",
                ) != current
                {
                    return Err(McapError::ConflictingChannels(
                        message.channel.topic.clone(),
                    ));
                }
            }
            None => {
                self.write_channel(records::Channel {
                    id: message.channel.id,
                    schema_id: message.channel.schema.as_ref().map(|s| s.id).unwrap_or(0),
                    topic: message.channel.topic.clone(),
                    message_encoding: message.channel.message_encoding.clone(),
                    metadata: message.channel.metadata.clone(),
                })?;
            }
        }
        let header = MessageHeader {
            channel_id: message.channel.id,
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
        if !self.all_channel_ids.contains_key(&header.channel_id) {
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
        if let (Some(WriteMode::Chunk(cw)), Some(target)) = (&self.writer, self.options.chunk_size)
        {
            let current_chunk_size = cw.compressor.position();
            if current_chunk_size > target {
                self.finish_chunk()?;
            }
        }

        if self.options.use_chunks {
            self.start_chunk()?.write_message(header, data)?;
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

        let WriteMode::Raw(w) = self.writer.take().expect(Self::WRITER_IS_NONE) else {
            unreachable!(
                "since finish_chunk was called, write mode is guaranteed to be raw at this point"
            );
        };

        self.writer = Some(WriteMode::Attachment(AttachmentWriter::new(
            w,
            attachment_length,
            header,
            self.options.calculate_attachment_crcs,
        )?));

        Ok(())
    }

    /// Write bytes to the current attachment.
    ///
    /// This is a low level API. For small attachments, use [`Self::attach`].
    ///
    /// Before calling this method call [`Self::start_attachment`].
    pub fn put_attachment_bytes(&mut self, bytes: &[u8]) -> McapResult<()> {
        let Some(WriteMode::Attachment(writer)) = &mut self.writer else {
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

        let Some(WriteMode::Attachment(writer)) = self.writer.take() else {
            panic!("WriteMode is guaranteed to be attachment by this point");
        };

        let (writer, attachment_index) = writer.finish()?;
        self.attachment_count += 1;

        if self.options.emit_attachment_indexes {
            self.attachment_indexes.push(attachment_index);
        }

        self.writer = Some(WriteMode::Raw(writer));

        Ok(())
    }

    /// Write an attachment to the MCAP file. This finishes any current chunk before writing the
    /// attachment.
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

    /// Write a [Metadata](https://mcap.dev/spec#metadata-op0x0c) record to the MCAP file. This
    /// finishes any current chunk before writing the metadata.
    pub fn write_metadata(&mut self, metadata: &Metadata) -> McapResult<()> {
        let w = self.finish_chunk()?;
        let offset = w.stream_position()?;

        // Should we specialize this to avoid taking a clone of the map?
        write_record(w, &Record::Metadata(metadata.clone()))?;

        let length = w.stream_position()? - offset;

        self.metadata_count += 1;
        if self.options.emit_metadata_indexes {
            self.metadata_indexes.push(records::MetadataIndex {
                offset,
                length,
                name: metadata.name.clone(),
            });
        }

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

    const WRITER_IS_NONE: &'static str = "unreachable: self.writer should never be None";

    fn assert_not_finished(&self) {
        assert!(
            !self.is_finished,
            "{}",
            "Trying to write a record on a finished MCAP"
        );
    }

    /// Starts a new chunk if we haven't done so already.
    fn start_chunk(&mut self) -> McapResult<&mut ChunkWriter<W>> {
        self.assert_not_finished();

        // It is not possible to start writing a chunk if we're still writing an attachment. Return
        // an error instead.
        if let Some(WriteMode::Attachment(..)) = self.writer {
            return Err(McapError::AttachmentNotInProgress);
        }

        assert!(
            self.options.use_chunks,
            "Trying to write to a chunk when chunking is disabled"
        );

        // Rust forbids moving values out of a &mut reference. We made self.writer an Option so we
        // can work around this by using take() to temporarily replace it with None while we
        // construct the ChunkWriter.
        self.writer = Some(match self.writer.take().expect(Self::WRITER_IS_NONE) {
            WriteMode::Raw(w) => {
                // It's chunkin time.
                WriteMode::Chunk(ChunkWriter::new(
                    w,
                    self.options.compression,
                    std::mem::take(&mut self.chunk_mode),
                    self.options.emit_message_indexes,
                    self.options.calculate_chunk_crcs,
                )?)
            }
            chunk => chunk,
        });

        let Some(WriteMode::Chunk(c)) = &mut self.writer else {
            unreachable!("we're not in an attachment and write mode was set to chunk above")
        };

        Ok(c)
    }

    /// Finish the current chunk, if we have one.
    fn finish_chunk(&mut self) -> McapResult<&mut CountingCrcWriter<W>> {
        self.assert_not_finished();
        // If we're currently writing an attachment then we're not writing a chunk. Return an
        // error instead.
        if let Some(WriteMode::Attachment(..)) = self.writer {
            return Err(McapError::AttachmentNotInProgress);
        }

        // See start_chunk() for why we use take() here.
        self.writer = Some(match self.writer.take().expect(Self::WRITER_IS_NONE) {
            WriteMode::Chunk(c) => {
                let (w, mode, index) = c.finish()?;
                self.chunk_indexes.push(index);
                self.chunk_mode = mode;
                WriteMode::Raw(w)
            }
            mode => mode,
        });

        let Some(WriteMode::Raw(w)) = &mut self.writer else {
            unreachable!("we're not in an attachment and write mode raw was set above")
        };

        Ok(w)
    }

    /// Finishes any current chunk and writes out the rest of the file.
    ///
    /// Subsequent calls to other methods will panic.
    pub fn finish(&mut self) -> McapResult<()> {
        if self.is_finished {
            // We already called finish().
            // Maybe we're dropping after the user called it?
            return Ok(());
        }

        // Finish any chunk we were working on and update stats, indexes, etc.
        self.finish_chunk()?;
        self.is_finished = true;

        // Grab the writer - self.writer becoming None makes subsequent writes fail.
        let writer = match &mut self.writer {
            // We called finish_chunk() above, so we're back to raw writes for
            // the summary section.
            Some(WriteMode::Raw(w)) => w,
            _ => unreachable!(),
        };
        let data_section_crc = writer.current_checksum();
        let writer = writer.get_mut();

        // We're done with the data secton!
        write_record(
            writer,
            &Record::DataEnd(records::DataEnd { data_section_crc }),
        )?;

        // Take all the data we need, swapping in empty containers.
        // Without this, we get yelled at for moving things out of a mutable ref
        // (&mut self).
        // (We could get around all this noise by having finish() take self,
        // but then it wouldn't be droppable _and_ finish...able.)
        let channel_message_counts = std::mem::take(&mut self.channel_message_counts);

        // Grab stats before we munge all the self fields below.
        let message_bounds = self.message_bounds.unwrap_or((0, 0));
        let stats = records::Statistics {
            message_count: channel_message_counts.values().sum(),
            schema_count: self.all_schema_ids.len() as u16,
            channel_count: self.all_channel_ids.len() as u32,
            attachment_count: self.attachment_count,
            metadata_count: self.metadata_count,
            chunk_count: self.chunk_indexes.len() as u32,
            message_start_time: message_bounds.0,
            message_end_time: message_bounds.1,
            channel_message_counts,
        };

        let chunk_indexes = std::mem::take(&mut self.chunk_indexes);
        let attachment_indexes = std::mem::take(&mut self.attachment_indexes);
        let metadata_indexes = std::mem::take(&mut self.metadata_indexes);

        let all_channels: Vec<_> = self
            .all_channel_ids
            .iter()
            .map(|(&id, canonical_id)| {
                let content = self.canonical_channels.get_by_right(canonical_id).expect(
                    "ID should exist in canonical_channels for every value in all_channel_ids",
                );
                records::Channel {
                    id,
                    schema_id: content.schema_id,
                    topic: content.topic.clone().into(),
                    message_encoding: content.message_encoding.clone().into(),
                    metadata: content.metadata.clone().into_owned(),
                }
            })
            .collect();
        let all_schemas: Vec<_> = self
            .all_schema_ids
            .iter()
            .map(|(&id, canonical_id)| {
                let content = self.canonical_schemas.get_by_right(canonical_id).expect(
                    "id should exist in canonical_schemas for every value in all_schema_ids",
                );
                Record::Schema {
                    header: records::SchemaHeader {
                        id,
                        name: content.name.clone().into(),
                        encoding: content.encoding.clone().into(),
                    },
                    data: content.data.clone(),
                }
            })
            .collect();

        let summary_start = writer.stream_position()?;
        let summary_offset_start;
        // Let's get a CRC of the summary section.
        let mut ccw;

        let mut offsets = Vec::new();

        let mut summary_end = summary_start;
        ccw = CountingCrcWriter::new(writer, self.options.calculate_summary_section_crc);

        fn posit<W: Write + Seek>(ccw: &mut CountingCrcWriter<W>) -> io::Result<u64> {
            ccw.get_mut().stream_position()
        }

        // Write all schemas.
        if self.options.repeat_schemas && !all_schemas.is_empty() {
            let schemas_start: u64 = summary_start;
            for schema in all_schemas.iter() {
                write_record(&mut ccw, schema)?;
            }
            summary_end = posit(&mut ccw)?;
            offsets.push(records::SummaryOffset {
                group_opcode: op::SCHEMA,
                group_start: schemas_start,
                group_length: summary_end - schemas_start,
            });
        }

        // Write all channels.
        if self.options.repeat_channels && !all_channels.is_empty() {
            let channels_start = summary_end;
            for channel in all_channels {
                write_record(&mut ccw, &Record::Channel(channel))?;
            }
            summary_end = posit(&mut ccw)?;
            offsets.push(records::SummaryOffset {
                group_opcode: op::CHANNEL,
                group_start: channels_start,
                group_length: summary_end - channels_start,
            });
        }

        if self.options.emit_statistics {
            let statistics_start = summary_end;
            write_record(&mut ccw, &Record::Statistics(stats))?;
            summary_end = posit(&mut ccw)?;
            offsets.push(records::SummaryOffset {
                group_opcode: op::STATISTICS,
                group_start: statistics_start,
                group_length: summary_end - statistics_start,
            });
        }

        if self.options.emit_chunk_indexes && !chunk_indexes.is_empty() {
            // Write all chunk indexes.
            let chunk_indexes_start = summary_end;
            for index in chunk_indexes {
                write_record(&mut ccw, &Record::ChunkIndex(index))?;
            }
            summary_end = posit(&mut ccw)?;
            offsets.push(records::SummaryOffset {
                group_opcode: op::CHUNK_INDEX,
                group_start: chunk_indexes_start,
                group_length: summary_end - chunk_indexes_start,
            });
        }

        // ...and attachment indexes
        if self.options.emit_attachment_indexes && !attachment_indexes.is_empty() {
            let attachment_indexes_start = summary_end;
            for index in attachment_indexes {
                write_record(&mut ccw, &Record::AttachmentIndex(index))?;
            }
            summary_end = posit(&mut ccw)?;
            offsets.push(records::SummaryOffset {
                group_opcode: op::ATTACHMENT_INDEX,
                group_start: attachment_indexes_start,
                group_length: summary_end - attachment_indexes_start,
            });
        }

        // ...and metadata indexes
        if self.options.emit_metadata_indexes && !metadata_indexes.is_empty() {
            let metadata_indexes_start = summary_end;
            for index in metadata_indexes {
                write_record(&mut ccw, &Record::MetadataIndex(index))?;
            }
            summary_end = posit(&mut ccw)?;
            offsets.push(records::SummaryOffset {
                group_opcode: op::METADATA_INDEX,
                group_start: metadata_indexes_start,
                group_length: summary_end - metadata_indexes_start,
            });
        }

        // Write the summary offsets we've been accumulating
        if self.options.emit_summary_offsets {
            summary_offset_start = summary_end;
            for offset in offsets {
                write_record(&mut ccw, &Record::SummaryOffset(offset))?;
            }
        } else {
            summary_offset_start = 0;
        }

        let summary_start = if summary_end > summary_start {
            summary_start
        } else {
            0 // We didn't write anything to the summary section.
        };

        // Wat: the CRC in the footer _includes_ part of the footer.
        op_and_len(&mut ccw, op::FOOTER, 20)?;
        ccw.write_u64::<LE>(summary_start)?;
        ccw.write_u64::<LE>(summary_offset_start)?;

        let (writer, summary_hasher) = ccw.finalize();
        let summary_crc = summary_hasher.map(|hasher| hasher.finalize()).unwrap_or(0);

        writer.write_u32::<LE>(summary_crc)?;

        writer.write_all(MAGIC)?;
        writer.flush()?;
        Ok(())
    }

    /// Consumes this writer, returning the underlying stream. Unless [`Self::finish()`] was called
    /// first, the underlying stream __will not contain a complete MCAP.__
    ///
    /// Use this if you wish to handle any errors returned when the underlying stream is closed. In
    /// particular, if using [`std::fs::File`], you may wish to call [`std::fs::File::sync_all()`]
    /// to ensure all data was sent to the filesystem.
    pub fn into_inner(mut self) -> W {
        self.is_finished = true;
        // Peel away all the layers of the writer to get the underlying stream.
        match self.writer.take().expect(Self::WRITER_IS_NONE) {
            WriteMode::Raw(w) => w.finalize().0,
            WriteMode::Attachment(w) => w.writer.finalize().0.finalize().0,
            WriteMode::Chunk(w) => w.compressor.finalize().0.into_inner().finalize().0.inner,
        }
    }
}

impl<W: Write + Seek> Drop for Writer<W> {
    fn drop(&mut self) {
        self.finish().unwrap()
    }
}

enum Compressor<W: Write> {
    Null(W),
    // zstd's Encoder wrapper doesn't let us get the inner writer without calling finish(), so use
    // zio::Writer directly instead.
    #[cfg(feature = "zstd")]
    Zstd(zio::Writer<W, zraw::Encoder<'static>>),
    #[cfg(feature = "lz4")]
    Lz4(lz4::Encoder<W>),
}

impl<W: Write> Compressor<W> {
    fn finish(self) -> io::Result<W> {
        Ok(match self {
            Compressor::Null(w) => w,
            #[cfg(feature = "zstd")]
            Compressor::Zstd(mut w) => {
                w.finish()?;
                w.into_inner().0
            }
            #[cfg(feature = "lz4")]
            Compressor::Lz4(w) => {
                let (output, result) = w.finish();
                result?;
                output
            }
        })
    }

    fn into_inner(self) -> W {
        match self {
            Compressor::Null(w) => w,
            #[cfg(feature = "zstd")]
            Compressor::Zstd(w) => w.into_inner().0,
            #[cfg(feature = "lz4")]
            Compressor::Lz4(w) => w.finish().0,
        }
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
    compressor: CountingCrcWriter<Compressor<CountingCrcWriter<ChunkSink<W>>>>,
    indexes: BTreeMap<u16, Vec<records::MessageIndexEntry>>,

    // Hasher from data before the chunk.
    pre_chunk_crc: Option<crc32fast::Hasher>,

    emit_message_indexes: bool,
}

impl<W: Write + Seek> ChunkWriter<W> {
    fn new(
        mut writer: CountingCrcWriter<W>,
        compression: Option<Compression>,
        mode: ChunkMode,
        emit_message_indexes: bool,
        calculate_chunk_crcs: bool,
    ) -> McapResult<Self> {
        // Relative to start of original stream.
        let chunk_offset = writer.stream_position()?;

        let (writer, pre_chunk_crc) = writer.finalize();
        let mut sink = ChunkSink::new(writer, mode);

        // Relative to start of chunk sink stream.
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
        let sink = CountingCrcWriter::new(sink, calculate_chunk_crcs);

        let compressor = match compression {
            #[cfg(feature = "zstd")]
            Some(Compression::Zstd) => {
                #[allow(unused_mut)]
                let mut enc = zraw::Encoder::with_dictionary(0, &[])?;
                // Enable multithreaded encoding on non-WASM targets.
                #[cfg(not(target_arch = "wasm32"))]
                enc.set_parameter(zraw::CParameter::NbWorkers(num_cpus::get_physical() as u32))?;
                Compressor::Zstd(zio::Writer::new(sink, enc))
            }
            #[cfg(feature = "lz4")]
            Some(Compression::Lz4) => Compressor::Lz4(
                lz4::EncoderBuilder::new()
                    // Disable the block checksum for wider compatibility with MCAP tooling that
                    // includes a fault block checksum calculation. Since the MCAP spec includes a
                    // CRC for the compressed chunk this would be a superfluous check anyway.
                    .block_checksum(lz4::liblz4::BlockChecksum::NoBlockChecksum)
                    .build(sink)?,
            ),
            #[cfg(not(any(feature = "zstd", feature = "lz4")))]
            Some(_) => unreachable!("`Compression` is an empty enum that cannot be instantiated"),
            None => Compressor::Null(sink),
        };
        let compressor = CountingCrcWriter::new(compressor, calculate_chunk_crcs);
        Ok(Self {
            chunk_offset,
            data_start,
            header_start,
            compressor,
            compression_name,
            message_bounds: None,
            indexes: BTreeMap::new(),
            pre_chunk_crc,
            emit_message_indexes,
        })
    }

    fn write_schema(&mut self, schema: Schema) -> McapResult<()> {
        let header = records::SchemaHeader {
            id: schema.id,
            name: schema.name,
            encoding: schema.encoding,
        };
        write_record(
            &mut self.compressor,
            &Record::Schema {
                header,
                data: schema.data,
            },
        )?;
        Ok(())
    }

    fn write_channel(&mut self, chan: records::Channel) -> McapResult<()> {
        write_record(&mut self.compressor, &Record::Channel(chan))?;
        Ok(())
    }

    fn write_message(&mut self, header: &MessageHeader, data: &[u8]) -> McapResult<()> {
        // Update min/max time for the chunk
        self.message_bounds = Some(match self.message_bounds {
            None => (header.log_time, header.log_time),
            Some((start, end)) => (start.min(header.log_time), end.max(header.log_time)),
        });

        if self.emit_message_indexes {
            // Add an index for this message
            self.indexes
                .entry(header.channel_id)
                .or_default()
                .push(records::MessageIndexEntry {
                    log_time: header.log_time,
                    offset: self.compressor.position(),
                });
        }

        write_record(
            &mut self.compressor,
            &Record::Message {
                header: *header,
                data: Cow::Borrowed(data),
            },
        )?;
        Ok(())
    }

    fn finish(self) -> McapResult<(CountingCrcWriter<W>, ChunkMode, records::ChunkIndex)> {
        // Get the number of uncompressed bytes written and the CRC.

        let uncompressed_size = self.compressor.position();
        let (stream, uncompressed_crc) = self.compressor.finalize();

        // Finalize the compression stream - it maintains an internal buffer.
        let writer = stream.finish()?;
        let compressed_size = writer.position();
        let (mut sink, compressed_crc) = writer.finalize();

        let data_end = sink.stream_position()?;
        // let compressed_size =  data_end - self.data_start;
        let record_size = (data_end - self.header_start) - 9; // 1 byte op, 8 byte len

        // Now that we know the size of the chunk data and the CRC of the uncompressed data, we
        // rewind the stream and overwrite the dummy chunk header with the true header.
        sink.seek(SeekFrom::Start(self.header_start))?;
        // Compute the CRC of the pre-chunk data concatenated with the chunk header.
        let mut writer = CountingCrcWriter::with_hasher(sink, self.pre_chunk_crc);
        op_and_len(&mut writer, op::CHUNK, record_size)?;
        let message_bounds = self.message_bounds.unwrap_or((0, 0));
        let header = records::ChunkHeader {
            message_start_time: message_bounds.0,
            message_end_time: message_bounds.1,
            uncompressed_size,
            uncompressed_crc: uncompressed_crc
                .map(|hasher| hasher.finalize())
                .unwrap_or(0),
            compression: String::from(self.compression_name),
            compressed_size,
        };
        writer.write_le(&header)?;
        let (mut sink, mut post_chunk_header_crc) = writer.finalize();
        assert_eq!(self.data_start, sink.stream_position()?);
        // We're done with all the chunk data. Move the cursor past the end and go back to just
        // appending records.
        assert_eq!(sink.seek(SeekFrom::End(0))?, data_end);
        let chunk_length = data_end - self.header_start;
        let (writer, mode) = sink.finish()?;

        // Compute the CRC of the pre-chunk data + chunk header + compressed chunk data. That is,
        // the CRC of the entire MCAP file up to the end of this chunk. This is necessary because
        // we ultimately have to produce a correct CRC of the MCAP file until the DataEnd record.
        if let (Some(hasher), Some(compressed_crc)) = (&mut post_chunk_header_crc, &compressed_crc)
        {
            hasher.combine(compressed_crc);
        }
        let mut writer = CountingCrcWriter::with_hasher(writer, post_chunk_header_crc);

        // Write our message indexes
        let data_end = writer.stream_position()?;
        let mut message_index_offsets: BTreeMap<u16, u64> = BTreeMap::new();
        let mut index_buf = Vec::new();
        for (channel_id, records) in self.indexes {
            let existing_offset =
                message_index_offsets.insert(channel_id, writer.stream_position()?);
            assert!(existing_offset.is_none());

            index_buf.clear();
            let index = records::MessageIndex {
                channel_id,
                records,
            };
            Cursor::new(&mut index_buf).write_le(&index)?;
            op_and_len(&mut writer, op::MESSAGE_INDEX, index_buf.len() as _)?;
            writer.write_all(&index_buf)?;
        }
        let message_index_length = writer.stream_position()? - data_end;

        let index = records::ChunkIndex {
            message_start_time: header.message_start_time,
            message_end_time: header.message_end_time,
            chunk_start_offset: self.chunk_offset,
            chunk_length,
            message_index_offsets,
            message_index_length,
            compression: header.compression,
            compressed_size: header.compressed_size,
            uncompressed_size: header.uncompressed_size,
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
    fn new(
        mut writer: W,
        attachment_length: u64,
        header: AttachmentHeader,
        calculate_crc: bool,
    ) -> McapResult<Self> {
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

        let mut writer = CountingCrcWriter::new(writer, calculate_crc);
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

        let (mut writer, hasher) = self.writer.finalize();
        let crc = hasher.map(|hasher| hasher.finalize()).unwrap_or(0);
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    #[test]
    fn writes_all_channel_ids() {
        let file = std::io::Cursor::new(Vec::new());
        let mut writer = Writer::new(file).expect("failed to construct writer");
        let custom_channel = Arc::new(crate::Channel {
            id: u16::MAX,
            topic: "chat".into(),
            message_encoding: "json".into(),
            metadata: BTreeMap::new(),
            schema: None,
        });
        writer
            .write(&crate::Message {
                channel: custom_channel.clone(),
                sequence: 0,
                log_time: 0,
                publish_time: 0,
                data: Cow::Owned(Vec::new()),
            })
            .expect("could not write initial channel");
        for i in 1..65535u16 {
            let id = writer
                .add_channel(0, &format!("{i}"), "json", &BTreeMap::new())
                .expect("could not add channel");
            assert_eq!(i, id);
        }
        let Err(too_many) = writer.add_channel(0, "last", "json", &BTreeMap::new()) else {
            panic!("should not be able to add another channel");
        };
        assert!(matches!(too_many, McapError::TooManyChannels));
    }
    #[test]
    fn writes_all_schema_ids() {
        let file = std::io::Cursor::new(Vec::new());
        let mut writer = Writer::new(file).expect("failed to construct writer");
        let custom_channel = Arc::new(crate::Channel {
            id: 0,
            topic: "chat".into(),
            message_encoding: "json".into(),
            metadata: BTreeMap::new(),
            schema: Some(Arc::new(crate::Schema {
                id: u16::MAX,
                name: "int".into(),
                encoding: "jsonschema".into(),
                data: Cow::Owned(Vec::new()),
            })),
        });
        writer
            .write(&crate::Message {
                channel: custom_channel.clone(),
                sequence: 0,
                log_time: 0,
                publish_time: 0,
                data: Cow::Owned(Vec::new()),
            })
            .expect("could not write initial channel");
        for i in 0..65534u16 {
            let id = writer
                .add_schema(&format!("{i}"), "jsonschema", &[])
                .expect("could not add schema");
            assert_eq!(id, i + 1);
        }
        let Err(too_many) = writer.add_schema("last", "jsonschema", &[]) else {
            panic!("should not be able to add another channel");
        };
        assert!(matches!(too_many, McapError::TooManySchemas));
    }

    #[test]
    #[should_panic(expected = "Trying to write a record on a finished MCAP")]
    fn panics_if_write_called_after_finish() {
        let file = std::io::Cursor::new(Vec::new());
        let mut writer = Writer::new(file).expect("failed to construct writer");
        writer.finish().expect("failed to finish writer");

        let custom_channel = Arc::new(crate::Channel {
            id: 1,
            topic: "chat".into(),
            message_encoding: "json".into(),
            metadata: BTreeMap::new(),
            schema: None,
        });

        writer
            .write(&crate::Message {
                channel: custom_channel.clone(),
                sequence: 0,
                log_time: 0,
                publish_time: 0,
                data: Cow::Owned(Vec::new()),
            })
            .expect("could not write message");
    }

    #[test]
    fn writes_message_and_checks_stream_length() {
        let file = std::io::Cursor::new(Vec::new());
        let mut writer = Writer::new(file).expect("failed to construct writer");

        let custom_channel = Arc::new(crate::Channel {
            id: 1,
            topic: "chat".into(),
            message_encoding: "json".into(),
            metadata: BTreeMap::new(),
            schema: None,
        });

        writer
            .write(&crate::Message {
                channel: custom_channel.clone(),
                sequence: 0,
                log_time: 0,
                publish_time: 0,
                data: Cow::Owned(Vec::new()),
            })
            .expect("could not write message");

        writer.finish().expect("failed to finish writer");

        let output_len = writer
            .into_inner()
            .stream_position()
            .expect("failed to get stream position");
        assert_eq!(output_len, 487);
    }

    #[test]
    fn preserves_written_channel_and_schema_ids() {
        let file = std::io::Cursor::new(Vec::new());
        let mut writer = Writer::new(file).expect("failed to construct writer");
        let schema = Arc::new(crate::Schema {
            id: 1,
            name: "schema".into(),
            encoding: "ros1msg".into(),
            data: Vec::new().into(),
        });
        let first_channel = crate::Channel {
            id: 1,
            topic: "chat".into(),
            schema: Some(schema.clone()),
            message_encoding: "ros1".into(),
            metadata: Default::default(),
        };
        let second_channel = crate::Channel {
            id: 2,
            schema: Some(schema.clone()),
            ..first_channel.clone()
        };
        let third_channel = crate::Channel {
            id: 3,
            schema: Some(schema.clone()),
            ..first_channel.clone()
        };
        writer
            .write(&crate::Message {
                channel: Arc::new(first_channel),
                sequence: 0,
                log_time: 0,
                publish_time: 0,
                data: Vec::new().into(),
            })
            .expect("failed to write first message");
        writer
            .write(&crate::Message {
                channel: Arc::new(second_channel),
                sequence: 0,
                log_time: 0,
                publish_time: 0,
                data: Vec::new().into(),
            })
            .expect("failed to write first message");
        writer
            .write(&crate::Message {
                channel: Arc::new(third_channel),
                sequence: 0,
                log_time: 0,
                publish_time: 0,
                data: Vec::new().into(),
            })
            .expect("failed to write first message");

        writer.finish().expect("failed in finish");
        let buf = writer.into_inner().into_inner();
        let summary = crate::Summary::read(&buf)
            .expect("failed to parse summary")
            .expect("expected a summary");
        assert_eq!(summary.channels.len(), 3);
        assert_eq!(summary.schemas.len(), 1);
    }
}
