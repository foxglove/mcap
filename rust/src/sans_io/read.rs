//! Contains a [sans-io](https://sans-io.readthedocs.io/) MCAP reader struct, [`LinearReader`].
//! This can be used to read MCAP data from any source of bytes.
use std::collections::HashMap;

use super::decompressor::Decompressor;
use crate::{
    records::{op, ChunkHeader},
    McapError, McapResult, MAGIC,
};
use binrw::BinReaderExt;

#[cfg(feature = "lz4")]
use super::lz4;

#[cfg(feature = "zstd")]
use super::zstd;

#[derive(Clone)]
enum CurrentlyReading {
    StartMagic,
    FileRecord,
    ChunkHeader { len: u64 },
    ValidatingChunkCrc,
    ChunkRecord,
    PaddingAfterChunk,
    EndMagic,
}
use CurrentlyReading::*;

struct ChunkState {
    decompressor: Option<Box<dyn Decompressor>>,
    uncompressed_data_hasher: Option<crc32fast::Hasher>,
    compressed_remaining: u64,
    uncompressed_len: u64,
    padding_after_compressed_data: usize,
    crc: u32,
}

#[derive(Default)]
struct RWBuf {
    data: Vec<u8>,
    start: usize,
    end: usize,
    hasher: Option<crc32fast::Hasher>,
}

impl RWBuf {
    fn new(instantiate_hasher: bool) -> Self {
        Self {
            hasher: if instantiate_hasher {
                Some(crc32fast::Hasher::new())
            } else {
                None
            },
            ..Default::default()
        }
    }

    // returns a mutable view of the un-written part of the buffer.
    fn tail(&mut self) -> &mut [u8] {
        &mut self.data[self.end..]
    }

    // Marks some bytes of the un-written part as written.
    fn mark_written(&mut self, written: usize) {
        self.end += written;
    }

    // Marks some bytes of the un-read part as read.
    fn mark_read(&mut self, read: usize) {
        if let Some(hasher) = self.hasher.as_mut() {
            hasher.update(&self.data[self.start..self.start + read]);
        }
        self.start += read;
    }

    // returns the length of the unread section.
    fn len(&self) -> usize {
        self.end - self.start
    }

    // returns an immutable view of the entire unread section.
    fn unread(&self) -> &[u8] {
        &self.data[self.start..self.end]
    }

    // returns a span of un-read data if enough is available, otherwise returning the remainder
    // needed.
    fn span(&self, want: usize) -> SpanOrRemainder {
        let desired_end = self.start + want;
        if desired_end <= self.end {
            Span((self.start, desired_end))
        } else {
            Remainder(desired_end - self.end)
        }
    }

    // returns an immutable view into the buffer for the given span.
    fn view(&self, span: (usize, usize)) -> &[u8] {
        let (start, end) = span;
        &self.data[start..end]
    }

    // returns a mutable view of the un-written part of the buffer, resizing as needed to ensure
    // N bytes are available to write into.
    fn tail_with_size(&mut self, n: usize) -> &mut [u8] {
        let desired_end = self.end + n;
        self.data.resize(desired_end, 0);
        self.tail()
    }

    // clears the RWBuf. Does not affect hasher state.
    fn clear(&mut self) {
        self.data.clear();
        self.start = 0;
        self.end = 0;
    }

    fn consume(&mut self, n: usize) -> SpanOrRemainder {
        let res = self.span(n);
        if let Span(_) = &res {
            self.mark_read(n);
        }
        res
    }
}

enum SpanOrRemainder {
    Span((usize, usize)),
    Remainder(usize),
}
use SpanOrRemainder::*;

/// Options for initializing [`LinearReader`].
#[derive(Debug, Default, Clone)]
pub struct LinearReaderOptions {
    /// If true, the reader will not expect the MCAP magic at the start of the stream.
    pub skip_start_magic: bool,
    /// If true, the reader will not expect the MCAP magic at the end of the stream.
    pub skip_end_magic: bool,
    /// If true, the reader will yield entire chunk records. Otherwise, the reader will decompress
    /// and read into chunks, yielding the records inside.
    pub emit_chunks: bool,
    // Enables chunk CRC validation. Ignored if `prevalidate_chunk_crcs: true`.
    pub validate_chunk_crcs: bool,
    // Enables chunk CRC validation before yielding any messages from the chunk. Implies
    // `validate_chunk_crcs: true`.
    pub prevalidate_chunk_crcs: bool,
    // Enables data section CRC validation.
    pub validate_data_section_crc: bool,
    // Enables summary section CRC validation.
    pub validate_summary_section_crc: bool,
}

impl LinearReaderOptions {
    pub fn with_skip_start_magic(self, skip_start_magic: bool) -> Self {
        Self {
            skip_start_magic,
            ..self
        }
    }
    pub fn with_skip_end_magic(self, skip_end_magic: bool) -> Self {
        Self {
            skip_end_magic,
            ..self
        }
    }
    pub fn with_emit_chunks(self, emit_chunks: bool) -> Self {
        Self {
            emit_chunks,
            ..self
        }
    }
    pub fn with_validate_chunk_crcs(self, validate_chunk_crcs: bool) -> Self {
        Self {
            validate_chunk_crcs,
            ..self
        }
    }
    pub fn with_prevalidate_chunk_crcs(self, prevalidate_chunk_crcs: bool) -> Self {
        Self {
            prevalidate_chunk_crcs,
            ..self
        }
    }
    pub fn with_validate_data_section_crc(self, validate_data_section_crc: bool) -> Self {
        Self {
            validate_data_section_crc,
            ..self
        }
    }
    pub fn with_validate_summary_section_crc(self, validate_summary_section_crc: bool) -> Self {
        Self {
            validate_summary_section_crc,
            ..self
        }
    }
}

/// Reads an MCAP file from start to end, yielding raw records by opcode and data buffer.
///
/// This struct does not perform any I/O on its own, instead it yields slices to the caller and
/// allows them to use their own I/O primitives.
/// ```no_run
/// use std::fs;
///
/// use tokio::fs::File as AsyncFile;
/// use tokio::io::AsyncReadExt;
/// use std::io::Read;
///
/// use mcap::sans_io::read::ReadAction;
/// use mcap::McapResult;
///
/// // Asynchronously...
/// async fn read_async() -> McapResult<()> {
///     let mut file = AsyncFile::open("in.mcap").await.expect("couldn't open file");
///     let mut reader = mcap::sans_io::read::LinearReader::new();
///     while let Some(action) = reader.next_action() {
///         match action? {
///             ReadAction::NeedMore(n) => {
///                 let written = file.read(reader.insert(n)).await?;
///                 reader.set_filled(written);
///             },
///             ReadAction::GetRecord{ opcode, data } => {
///                 let raw_record = mcap::parse_record(opcode, data)?;
///                 // do something with the record...
///             }
///         }
///     }
///     Ok(())
/// }
///
/// // Or synchronously.
/// fn read_sync() -> McapResult<()> {
///     let mut file = fs::File::open("in.mcap")?;
///     let mut reader = mcap::sans_io::read::LinearReader::new();
///     while let Some(action) = reader.next_action() {
///         match action? {
///             ReadAction::NeedMore(n) => {
///                 let written = file.read(reader.insert(n))?;
///                 reader.set_filled(written);
///             },
///             ReadAction::GetRecord{ opcode, data } => {
///                 let raw_record = mcap::parse_record(opcode, data)?;
///                 // do something with the record...
///             }
///         }
///     }
///     Ok(())
/// }
/// ```
pub struct LinearReader {
    currently_reading: CurrentlyReading,
    chunk_state: Option<ChunkState>,
    file_data: RWBuf,
    uncompressed_content: RWBuf,
    decompressors: HashMap<String, Box<dyn Decompressor>>,
    options: LinearReaderOptions,
    last_write: Option<usize>,
}

impl LinearReader {
    pub fn new() -> Self {
        Self::new_with_options(LinearReaderOptions::default())
    }

    pub fn new_with_options(options: LinearReaderOptions) -> Self {
        LinearReader {
            currently_reading: if options.skip_start_magic {
                FileRecord
            } else {
                StartMagic
            },
            file_data: RWBuf::new(options.validate_data_section_crc),
            uncompressed_content: RWBuf::new(
                options.validate_chunk_crcs && !options.prevalidate_chunk_crcs,
            ),
            options,
            chunk_state: None,
            decompressors: Default::default(),
            last_write: None,
        }
    }

    /// Constructs a linear reader that will iterate through all records in a chunk.
    pub(crate) fn for_chunk(header: ChunkHeader) -> McapResult<Self> {
        let mut result = Self::new_with_options(
            LinearReaderOptions::default()
                .with_skip_end_magic(true)
                .with_skip_start_magic(true)
                .with_validate_chunk_crcs(true),
        );
        result.currently_reading = FileRecord;
        result.chunk_state = Some(ChunkState {
            decompressor: get_decompressor(&mut HashMap::new(), &header.compression)?,
            crc: header.uncompressed_crc,
            uncompressed_data_hasher: Some(crc32fast::Hasher::new()),
            uncompressed_len: header.uncompressed_size,
            compressed_remaining: header.compressed_size,
            padding_after_compressed_data: 0,
        });
        Ok(result)
    }

    /// Read new data into this reader.
    pub fn insert(&mut self, n: usize) -> &mut [u8] {
        self.file_data.tail_with_size(n)
    }

    pub fn set_filled(&mut self, n: usize) {
        self.last_write = Some(n);
    }

    /// Yields the next action the caller should take to progress through the file.
    pub fn next_action(&mut self) -> Option<McapResult<ReadAction>> {
        self.next_action_inner().transpose()
    }

    fn next_action_inner(&mut self) -> McapResult<Option<ReadAction>> {
        if let Some(n) = self.last_write.take() {
            if n == 0 {
                // at EOF.
                if self.options.skip_end_magic {
                    if matches!(self.currently_reading, FileRecord) && self.file_data.len() == 0 {
                        return Ok(None);
                    }
                }
                if matches!(self.chunk_state, Some(_)) {
                    return Err(McapError::UnexpectedEoc);
                }
                return Err(McapError::UnexpectedEof);
            }
            self.file_data.mark_written(n);
        }

        if self.file_data.len() == 0 {
            self.file_data.clear();
        }

        loop {
            match self.currently_reading.clone() {
                StartMagic => {
                    if self.options.skip_start_magic {
                        self.currently_reading = CurrentlyReading::FileRecord;
                        continue;
                    }
                    let data = match self.file_data.consume(MAGIC.len()) {
                        Span(span) => self.file_data.view(span),
                        Remainder(n) => return Ok(Some(ReadAction::NeedMore(n))),
                    };
                    if *data != *MAGIC {
                        return Err(McapError::BadMagic);
                    }
                    self.currently_reading = CurrentlyReading::FileRecord;
                }
                EndMagic => {
                    if self.options.skip_end_magic {
                        return Ok(None);
                    }
                    let data = match self.file_data.consume(MAGIC.len()) {
                        Span(span) => self.file_data.view(span),
                        Remainder(n) => return Ok(Some(ReadAction::NeedMore(n))),
                    };
                    if *data != *MAGIC {
                        return Err(McapError::BadMagic);
                    }
                    return Ok(None);
                }
                FileRecord => {
                    let opcode_length_buf = match self.file_data.span(9) {
                        Span(span) => self.file_data.view(span),
                        Remainder(n) => return Ok(Some(ReadAction::NeedMore(n))),
                    };
                    let opcode = opcode_length_buf[0];
                    let len = u64::from_le_bytes(opcode_length_buf[1..].try_into().unwrap());
                    if opcode == op::CHUNK && !self.options.emit_chunks {
                        self.file_data.mark_read(9);
                        self.currently_reading = CurrentlyReading::ChunkHeader { len };
                        continue;
                    } else if opcode == op::DATA_END {
                        // treat this opcode specially as we need to check the data section CRC
                        let calculated =
                            self.file_data.hasher.take().map(|hasher| hasher.finalize());

                        if len < 4 {
                            return Err(McapError::RecordTooShort {
                                opcode: op::DATA_END,
                                len,
                                expected: 4,
                            });
                        }
                        let span = match self.file_data.consume(len_as_usize(len)? + 9) {
                            Span(span) => span,
                            Remainder(n) => return Ok(Some(ReadAction::NeedMore(n))),
                        };
                        if self.options.validate_summary_section_crc {
                            self.file_data.hasher = Some(crc32fast::Hasher::new());
                        }
                        let data = &self.file_data.view(span)[9..];
                        let saved = u32::from_le_bytes(data[..4].try_into().unwrap());
                        if let Some(calculated) = calculated {
                            if saved != 0 && calculated != saved {
                                return Err(McapError::BadDataCrc { saved, calculated });
                            }
                        }
                        return Ok(Some(ReadAction::GetRecord { data, opcode }));
                    } else if opcode == op::FOOTER {
                        if len < 20 {
                            return Err(McapError::RecordTooShort {
                                opcode: op::DATA_END,
                                len,
                                expected: 20,
                            });
                        }
                        let (start, end) = match self.file_data.span(len_as_usize(len)? + 9) {
                            Span(span) => span,
                            Remainder(n) => return Ok(Some(ReadAction::NeedMore(n))),
                        };
                        if let Some(mut hasher) = self.file_data.hasher.take() {
                            hasher.update(&self.file_data.data[start..9 + 16]);
                            let calculated = hasher.finalize();
                            let saved = u32::from_le_bytes(
                                self.file_data.data[start + 9 + 16..start + 9 + 20]
                                    .try_into()
                                    .unwrap(),
                            );
                            if saved != 0 && saved != calculated {
                                return Err(McapError::BadSummaryCrc { saved, calculated });
                            }
                        }
                        self.file_data.mark_read(len_as_usize(len)? + 9);
                        let data = &self.file_data.data[start + 9..end];
                        self.currently_reading = EndMagic;
                        return Ok(Some(ReadAction::GetRecord { data, opcode }));
                    }
                    let data = match self.file_data.consume(len_as_usize(len)? + 9) {
                        Span(span) => self.file_data.view(span),
                        Remainder(n) => return Ok(Some(ReadAction::NeedMore(n))),
                    };
                    let data = &data[9..];
                    return Ok(Some(ReadAction::GetRecord { data, opcode }));
                }
                CurrentlyReading::ChunkHeader { len } => {
                    const MIN_CHUNK_HEADER_SIZE: usize = 8 + 8 + 8 + 4 + 4 + 8;
                    let min_header_buf = match self.file_data.span(MIN_CHUNK_HEADER_SIZE) {
                        Span(span) => self.file_data.view(span),
                        Remainder(n) => return Ok(Some(ReadAction::NeedMore(n))),
                    };
                    let compression_len =
                        u32::from_le_bytes(min_header_buf[28..32].try_into().unwrap());
                    let header_len = MIN_CHUNK_HEADER_SIZE + compression_len as usize;
                    let header_buf = match self.file_data.consume(header_len) {
                        Span(span) => self.file_data.view(span),
                        Remainder(n) => return Ok(Some(ReadAction::NeedMore(n))),
                    };
                    let header: ChunkHeader = std::io::Cursor::new(header_buf).read_le()?;
                    let state = ChunkState {
                        decompressor: get_decompressor(
                            &mut self.decompressors,
                            &header.compression,
                        )?,
                        uncompressed_data_hasher: if self.options.validate_chunk_crcs
                            && !self.options.prevalidate_chunk_crcs
                            && header.uncompressed_crc != 0
                        {
                            Some(crc32fast::Hasher::new())
                        } else {
                            None
                        },
                        compressed_remaining: header.compressed_size,
                        uncompressed_len: header.uncompressed_size,
                        padding_after_compressed_data: len_as_usize(
                            len - header_len as u64 - header.compressed_size,
                        )?,
                        crc: header.uncompressed_crc,
                    };
                    self.uncompressed_content.clear();
                    if self.options.prevalidate_chunk_crcs && state.crc != 0 {
                        self.currently_reading = ValidatingChunkCrc;
                    } else {
                        self.currently_reading = ChunkRecord;
                    }
                    self.chunk_state = Some(state);
                }
                ValidatingChunkCrc => {
                    let state = self
                        .chunk_state
                        .as_mut()
                        .expect("chunk state should be set");
                    match &mut state.decompressor {
                        None => {
                            let records = match self
                                .file_data
                                .span(len_as_usize(state.compressed_remaining)?)
                            {
                                Span(span) => self.file_data.view(span),
                                Remainder(n) => return Ok(Some(ReadAction::NeedMore(n))),
                            };
                            let calculated = crc32fast::hash(records);
                            let saved = state.crc;
                            if calculated != saved {
                                return Err(McapError::BadChunkCrc { saved, calculated });
                            }
                            self.currently_reading = ChunkRecord;
                        }
                        Some(decompressor) => {
                            if state.compressed_remaining == 0 {
                                let calculated =
                                    crc32fast::hash(self.uncompressed_content.unread());
                                let saved = state.crc;
                                if calculated != saved {
                                    return Err(McapError::BadChunkCrc { saved, calculated });
                                }
                                self.currently_reading = ChunkRecord;
                                continue;
                            }
                            match decompress_n(
                                decompressor,
                                len_as_usize(state.uncompressed_len)?,
                                &mut self.file_data,
                                &mut self.uncompressed_content,
                                &mut state.compressed_remaining,
                            )? {
                                Span(_) => {}
                                Remainder(n) => return Ok(Some(ReadAction::NeedMore(n))),
                            };
                        }
                    }
                }
                ChunkRecord => {
                    let state = self
                        .chunk_state
                        .as_mut()
                        .expect("chunk state should be set");
                    match &mut state.decompressor {
                        None => {
                            if state.compressed_remaining == 0 {
                                if let Some(hasher) = state.uncompressed_data_hasher.take() {
                                    let calculated = hasher.finalize();
                                    if state.crc != 0 && state.crc != calculated {
                                        return Err(McapError::BadChunkCrc {
                                            saved: state.crc,
                                            calculated,
                                        });
                                    }
                                }
                                self.currently_reading = PaddingAfterChunk;
                                continue;
                            }
                            let opcode_len_buf = match self.file_data.span(9) {
                                Span(span) => self.file_data.view(span),
                                Remainder(n) => return Ok(Some(ReadAction::NeedMore(n))),
                            };
                            let opcode = opcode_len_buf[0];
                            let len = len_as_usize(u64::from_le_bytes(
                                opcode_len_buf[1..9].try_into().unwrap(),
                            ))?;
                            let (start, end) = match self.file_data.span(9 + len) {
                                Span(span) => span,
                                Remainder(n) => return Ok(Some(ReadAction::NeedMore(n))),
                            };
                            state
                                .uncompressed_data_hasher
                                .as_mut()
                                .map(|hasher| hasher.update(&self.file_data.data[start..end]));
                            self.file_data.mark_read(9 + len);
                            let data_span = (start + 9, end);
                            state.compressed_remaining -= (9 + len) as u64;
                            return Ok(Some(ReadAction::GetRecord {
                                data: self.file_data.view(data_span),
                                opcode,
                            }));
                        }
                        Some(decompressor) => {
                            if state.compressed_remaining == 0
                                && self.uncompressed_content.len() == 0
                            {
                                if let Some(hasher) = self.uncompressed_content.hasher.take() {
                                    let calculated = hasher.finalize();
                                    let saved = state.crc;
                                    if saved != 0 && saved != calculated {
                                        return Err(McapError::BadChunkCrc { saved, calculated });
                                    }
                                    self.uncompressed_content.hasher =
                                        Some(crc32fast::Hasher::new());
                                }
                                self.uncompressed_content.clear();
                                self.currently_reading = PaddingAfterChunk;
                                continue;
                            }
                            let opcode_len_buf = match decompress_n(
                                decompressor,
                                9,
                                &mut self.file_data,
                                &mut self.uncompressed_content,
                                &mut state.compressed_remaining,
                            )? {
                                Span(span) => self.uncompressed_content.view(span),
                                Remainder(n) => return Ok(Some(ReadAction::NeedMore(n))),
                            };
                            let opcode = opcode_len_buf[0];
                            let len = len_as_usize(u64::from_le_bytes(
                                opcode_len_buf[1..9].try_into().unwrap(),
                            ))?;
                            let (start, end) = match decompress_n(
                                decompressor,
                                9 + len,
                                &mut self.file_data,
                                &mut self.uncompressed_content,
                                &mut state.compressed_remaining,
                            )? {
                                Span(span) => span,
                                Remainder(n) => return Ok(Some(ReadAction::NeedMore(n))),
                            };
                            self.uncompressed_content.mark_read(9 + len);
                            let data = &self.uncompressed_content.data[start + 9..end];
                            return Ok(Some(ReadAction::GetRecord { data, opcode }));
                        }
                    }
                }
                PaddingAfterChunk => {
                    let mut state = self.chunk_state.take().expect("chunk state should be set");
                    match self.file_data.consume(state.padding_after_compressed_data) {
                        Span(_) => {}
                        Remainder(n) => return Ok(Some(ReadAction::NeedMore(n))),
                    }
                    if let Some(decompressor) = state.decompressor.take() {
                        release_decompressor(&mut self.decompressors, decompressor)?;
                    }
                    self.currently_reading = FileRecord;
                }
            }
        }
    }
}

// impl Default for LinearReader {
//     fn default() -> Self {
//         Self::new()
//     }
// }

/// Encapsulates the action the user should take next when reading an MCAP file.
///
/// ```no_run
/// use mcap::sans_io::read::ReadAction;
/// use mcap::McapResult;
/// use mcap::read::parse_record;
/// fn use_action(file: &mut Box<dyn std::io::Read>, action: ReadAction) -> McapResult<()> {
///   match action {
///     ReadAction::Fill(mut into) => {
///       let n = file.read(into.buf)?;
///       into.set_filled(n);
///     },
///     ReadAction::GetRecord{ opcode, data } => {
///       let record = mcap::parse_record(opcode, data)?;
///       // do something with the record...
///     }
///   }
///   Ok(())
/// }
/// ```
pub enum ReadAction<'a> {
    /// The reader needs more data to continue - call [`LinearReader::insert`] to load more data.
    /// the value provided here is a hint for how much data to insert.
    NeedMore(usize),
    /// Read a record out of the MCAP file. Use [`parse_record`] to parse the record.
    GetRecord { data: &'a [u8], opcode: u8 },
}

fn len_as_usize(len: u64) -> McapResult<usize> {
    len.try_into().map_err(|_| McapError::TooLong(len))
}

fn clamp_to_usize(len: u64) -> usize {
    len.try_into().unwrap_or(usize::MAX)
}

fn get_decompressor(
    decompressors: &mut HashMap<String, Box<dyn Decompressor>>,
    name: &str,
) -> McapResult<Option<Box<dyn Decompressor>>> {
    if let Some(decompressor) = decompressors.remove(name) {
        return Ok(Some(decompressor));
    }
    match name {
        #[cfg(feature = "zstd")]
        "zstd" => Ok(Some(Box::new(zstd::ZstdDecoder::new()))),
        #[cfg(feature = "lz4")]
        "lz4" => Ok(Some(Box::new(lz4::Lz4Decoder::new()?))),
        "" => Ok(None),
        _ => Err(McapError::UnsupportedCompression(name.into())),
    }
}

fn release_decompressor(
    decompressors: &mut HashMap<String, Box<dyn Decompressor>>,
    mut decompressor: Box<dyn Decompressor>,
) -> McapResult<()> {
    decompressor.reset()?;
    decompressors.insert(decompressor.name().into(), decompressor);
    Ok(())
}

fn decompress_n(
    decompressor: &mut Box<dyn Decompressor>,
    n: usize,
    from: &mut RWBuf,
    to: &mut RWBuf,
    compressed_remaining: &mut u64,
) -> McapResult<SpanOrRemainder> {
    if to.len() > n {
        return Ok(to.span(n));
    }
    to.data.resize(to.start + n, 0);
    loop {
        let need = decompressor.next_read_size();
        let have = from.len();
        if need > have {
            return Ok(Remainder(need - have));
        }
        let dst = to.tail();
        if dst.len() == 0 {
            return Ok(Span((to.start, to.end)));
        }
        let src_len = std::cmp::min(have, clamp_to_usize(*compressed_remaining));
        let src = &from.data[from.start..from.start + src_len];
        let res = decompressor.decompress(src, dst)?;
        from.mark_read(res.consumed);
        to.mark_written(res.wrote);
        *compressed_remaining -= res.consumed as u64;
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::{parse_record, Compression};
    use std::collections::BTreeMap;
    use std::io::Read;

    fn basic_chunked_file(compression: Option<Compression>) -> McapResult<Vec<u8>> {
        let mut buf = std::io::Cursor::new(Vec::new());
        {
            let mut writer = crate::WriteOptions::new()
                .compression(compression)
                .create(&mut buf)?;
            let channel = std::sync::Arc::new(crate::Channel {
                topic: "chat".to_owned(),
                schema: None,
                message_encoding: "json".to_owned(),
                metadata: BTreeMap::new(),
            });
            writer.add_channel(&channel)?;
            writer.write(&crate::Message {
                channel,
                sequence: 0,
                log_time: 0,
                publish_time: 0,
                data: (&[0, 1, 2]).into(),
            })?;
            writer.finish()?;
        }
        Ok(buf.into_inner())
    }

    #[test]
    fn test_un_chunked() -> McapResult<()> {
        let mut buf = std::io::Cursor::new(Vec::new());
        {
            let mut writer = crate::WriteOptions::new()
                .use_chunks(false)
                .create(&mut buf)?;
            let channel = std::sync::Arc::new(crate::Channel {
                topic: "chat".to_owned(),
                schema: None,
                message_encoding: "json".to_owned(),
                metadata: BTreeMap::new(),
            });
            writer.add_channel(&channel)?;
            writer.write(&crate::Message {
                channel,
                sequence: 0,
                log_time: 0,
                publish_time: 0,
                data: (&[0, 1, 2]).into(),
            })?;
            writer.finish()?;
        }
        let mut reader = LinearReader::new();
        let mut cursor = std::io::Cursor::new(buf.into_inner());
        let mut opcodes: Vec<u8> = Vec::new();
        let mut iter_count = 0;
        while let Some(action) = reader.next_action() {
            match action? {
                ReadAction::NeedMore(n) => {
                    let written = cursor.read(reader.insert(n))?;
                    reader.set_filled(written);
                }
                ReadAction::GetRecord { data, opcode } => {
                    opcodes.push(opcode);
                    parse_record(opcode, data)?;
                }
            }
            assert!(iter_count < 10000);
            iter_count += 1;
        }
        assert_eq!(
            opcodes,
            vec![
                op::HEADER,
                op::CHANNEL,
                op::MESSAGE,
                op::DATA_END,
                op::CHANNEL,
                op::STATISTICS,
                op::SUMMARY_OFFSET,
                op::SUMMARY_OFFSET,
                op::FOOTER
            ]
        );

        Ok(())
    }

    #[test]
    fn test_all_validations() -> McapResult<()> {
        let mut reader = LinearReader::new_with_options(
            LinearReaderOptions::default()
                .with_validate_data_section_crc(true)
                .with_validate_summary_section_crc(true),
        );
        let mut cursor = std::io::Cursor::new(basic_chunked_file(None)?);
        let mut opcodes: Vec<u8> = Vec::new();
        let mut iter_count = 0;
        while let Some(action) = reader.next_action() {
            match action? {
                ReadAction::NeedMore(n) => {
                    let written = cursor.read(reader.insert(n))?;
                    reader.set_filled(written);
                }
                ReadAction::GetRecord { data, opcode } => {
                    opcodes.push(opcode);
                    parse_record(opcode, data)?;
                }
            }
            iter_count += 1;
            // guard against infinite loop
            assert!(iter_count < 10000);
        }
        Ok(())
    }

    fn test_chunked(
        compression: Option<Compression>,
        options: LinearReaderOptions,
    ) -> McapResult<()> {
        let mut reader = LinearReader::new_with_options(options);
        let mut cursor = std::io::Cursor::new(basic_chunked_file(compression)?);
        let mut opcodes: Vec<u8> = Vec::new();
        let mut iter_count = 0;
        while let Some(action) = reader.next_action() {
            match action? {
                ReadAction::NeedMore(n) => {
                    let written = cursor.read(reader.insert(n))?;
                    reader.set_filled(written);
                }
                ReadAction::GetRecord { data, opcode } => {
                    opcodes.push(opcode);
                    parse_record(opcode, data)?;
                }
            }
            iter_count += 1;
            // guard against infinite loop
            assert!(iter_count < 10000);
        }
        assert_eq!(
            opcodes,
            vec![
                op::HEADER,
                op::CHANNEL,
                op::MESSAGE,
                op::MESSAGE_INDEX,
                op::DATA_END,
                op::CHANNEL,
                op::CHUNK_INDEX,
                op::STATISTICS,
                op::SUMMARY_OFFSET,
                op::SUMMARY_OFFSET,
                op::SUMMARY_OFFSET,
                op::FOOTER
            ]
        );
        Ok(())
    }
    use paste::paste;

    macro_rules! test_chunked_parametrized {
        ($($name:ident, $compression:expr, $options:expr),*) => {
            $(
                paste! {
                    #[test]
                    fn [ <test_chunked_ $name> ]() -> McapResult<()> {
                        test_chunked($compression, $options)
                    }
                }
            )*

        };
    }

    test_chunked_parametrized! {
        none_none, None, LinearReaderOptions::default(),
        none_after, None, LinearReaderOptions::default().with_validate_chunk_crcs(true),
        none_before, None, LinearReaderOptions::default().with_prevalidate_chunk_crcs(true),
        zstd_none, Some(Compression::Zstd), LinearReaderOptions::default(),
        zstd_after, Some(Compression::Zstd), LinearReaderOptions::default().with_validate_chunk_crcs(true),
        zstd_before, Some(Compression::Zstd), LinearReaderOptions::default().with_prevalidate_chunk_crcs(true),
        lz4_none, Some(Compression::Lz4), LinearReaderOptions::default(),
        lz4_after, Some(Compression::Lz4), LinearReaderOptions::default().with_validate_chunk_crcs(true),
        lz4_before, Some(Compression::Lz4), LinearReaderOptions::default().with_prevalidate_chunk_crcs(true)
    }

    #[test]
    fn test_no_magic() -> McapResult<()> {
        for options in [
            LinearReaderOptions::default().with_skip_start_magic(true),
            LinearReaderOptions::default().with_skip_end_magic(true),
        ] {
            let mcap = basic_chunked_file(None)?;
            let input = if options.skip_start_magic {
                &mcap[8..]
            } else if options.skip_end_magic {
                &mcap[..mcap.len() - 8]
            } else {
                panic!("options should either skip start or end magic")
            };
            let mut reader = LinearReader::new_with_options(options);
            let mut cursor = std::io::Cursor::new(input);
            let mut opcodes: Vec<u8> = Vec::new();
            let mut iter_count = 0;
            while let Some(action) = reader.next_action() {
                match action? {
                    ReadAction::NeedMore(n) => {
                        let written = cursor.read(reader.insert(n))?;
                        reader.set_filled(written);
                    }
                    ReadAction::GetRecord { data, opcode } => {
                        opcodes.push(opcode);
                        parse_record(opcode, data)?;
                    }
                }
                iter_count += 1;
                // guard against infinite loop
                assert!(iter_count < 10000);
            }
            assert_eq!(
                opcodes,
                vec![
                    op::HEADER,
                    op::CHANNEL,
                    op::MESSAGE,
                    op::MESSAGE_INDEX,
                    op::DATA_END,
                    op::CHANNEL,
                    op::CHUNK_INDEX,
                    op::STATISTICS,
                    op::SUMMARY_OFFSET,
                    op::SUMMARY_OFFSET,
                    op::SUMMARY_OFFSET,
                    op::FOOTER
                ]
            );
        }
        Ok(())
    }

    #[test]
    fn test_emit_chunks() -> McapResult<()> {
        let mcap = basic_chunked_file(None)?;
        let mut reader =
            LinearReader::new_with_options(LinearReaderOptions::default().with_emit_chunks(true));
        let mut cursor = std::io::Cursor::new(mcap);
        let mut opcodes: Vec<u8> = Vec::new();
        let mut iter_count = 0;
        while let Some(action) = reader.next_action() {
            match action? {
                ReadAction::NeedMore(n) => {
                    let written = cursor.read(reader.insert(n))?;
                    reader.set_filled(written);
                }
                ReadAction::GetRecord { data, opcode } => {
                    opcodes.push(opcode);
                    parse_record(opcode, data)?;
                }
            }
            iter_count += 1;
            // guard against infinite loop
            assert!(iter_count < 10000);
        }
        assert_eq!(
            opcodes,
            vec![
                op::HEADER,
                op::CHUNK,
                op::MESSAGE_INDEX,
                op::DATA_END,
                op::CHANNEL,
                op::CHUNK_INDEX,
                op::STATISTICS,
                op::SUMMARY_OFFSET,
                op::SUMMARY_OFFSET,
                op::SUMMARY_OFFSET,
                op::FOOTER
            ]
        );
        Ok(())
    }
}
