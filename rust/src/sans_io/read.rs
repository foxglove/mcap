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
    ChunkHeader {
        len: u64,
    },
    Footer {
        len: u64,
        hasher: Option<crc32fast::Hasher>,
    },
    DataEnd {
        len: u64,
        calculated: Option<u32>,
    },
    ValidatingChunkCrc,
    ChunkRecord,
    PaddingAfterChunk,
    EndMagic,
}
use CurrentlyReading::*;

struct ChunkState {
    // The decompressor to use for loading records for this chunk. None if not compressed.
    decompressor: Option<Box<dyn Decompressor>>,
    // For uncompressed chunks, records are sliced directly out of `file_data`.  Therefore we can't
    // use `decompressed_content.hasher` to calculate a CRC, so we maintain a separate hasher for
    // this purpose.
    uncompressed_data_hasher: Option<crc32fast::Hasher>,
    // The number of compressed bytes left in the chunk that have not been read out of `file_data`.
    compressed_remaining: u64,
    // The number of uncompressed bytes left in the chunk that have not been decompressed yet.
    uncompressed_remaining: u64,
    // The total uncompressed length of the chunk records field.
    uncompressed_len: u64,
    // The number of bytes in the chunk record after the `records` field ends.
    padding_after_compressed_data: usize,
    // The CRC value that was read at the start of the chunk record.
    crc: u32,
}

/// A private struct that encapsulates a buffer with start and end cursors.
#[derive(Default)]
struct RwBuf {
    data: Vec<u8>,
    start: usize,
    end: usize,
    hasher: Option<crc32fast::Hasher>,
}

impl RwBuf {
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

    // returns a mutable view of the un-written part of the buffer, resizing as needed to ensure
    // N bytes are available to write into.
    fn tail_with_size(&mut self, n: usize) -> &mut [u8] {
        let unread_len = self.end - self.start;
        // Compact the output buffer if there is sufficient free space and there is more free
        // than used.
        if self.start > 4096 && self.start > unread_len {
            self.data.copy_within(self.start..self.end, 0);
            self.start = 0;
            self.end = unread_len;
        }
        let desired_end = self.end + n;
        self.data.resize(desired_end, 0);
        &mut self.data[self.end..]
    }

    // clears the RWBuf. Does not affect hasher state.
    fn clear(&mut self) {
        self.data.clear();
        self.start = 0;
        self.end = 0;
    }
}

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
    pub fn with_skip_start_magic(mut self, skip_start_magic: bool) -> Self {
        self.skip_start_magic = skip_start_magic;
        self
    }
    pub fn with_skip_end_magic(mut self, skip_end_magic: bool) -> Self {
        self.skip_end_magic = skip_end_magic;
        self
    }
    pub fn with_emit_chunks(mut self, emit_chunks: bool) -> Self {
        self.emit_chunks = emit_chunks;
        self
    }
    pub fn with_validate_chunk_crcs(mut self, validate_chunk_crcs: bool) -> Self {
        self.validate_chunk_crcs = validate_chunk_crcs;
        self
    }
    pub fn with_prevalidate_chunk_crcs(mut self, prevalidate_chunk_crcs: bool) -> Self {
        self.prevalidate_chunk_crcs = prevalidate_chunk_crcs;
        self
    }
    pub fn with_validate_data_section_crc(mut self, validate_data_section_crc: bool) -> Self {
        self.validate_data_section_crc = validate_data_section_crc;
        self
    }
    pub fn with_validate_summary_section_crc(mut self, validate_summary_section_crc: bool) -> Self {
        self.validate_summary_section_crc = validate_summary_section_crc;
        self
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
///             ReadAction::NeedMore(need) => {
///                 let written = file.read(reader.insert(need)).await?;
///                 reader.set_written(written);
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
///             ReadAction::NeedMore(need) => {
///                 let written = file.read(reader.insert(need))?;
///                 reader.set_written(written);
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
    // The core state of the LinearReader state machine. Describes the part of the MCAP
    // file currently being read.
    currently_reading: CurrentlyReading,
    // Auxilliary state specific to reading records out of chunks. This is stored outside of
    // CurrentlyReading to avoid needing to clone() it on every iteration.
    chunk_state: Option<ChunkState>,
    // MCAP data loaded from the file
    file_data: RwBuf,
    // data decompressed from compressed chunks
    decompressed_content: RwBuf,
    // decompressor that can be re-used between chunks.
    decompressors: HashMap<String, Box<dyn Decompressor>>,
    // Stores the number of bytes written into this reader since the last `next_action()` call.
    last_write: Option<usize>,
    options: LinearReaderOptions,
}

impl Default for LinearReader {
    fn default() -> Self {
        Self::new_with_options(Default::default())
    }
}

impl LinearReader {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn new_with_options(options: LinearReaderOptions) -> Self {
        LinearReader {
            currently_reading: if options.skip_start_magic {
                FileRecord
            } else {
                StartMagic
            },
            file_data: RwBuf::new(options.validate_data_section_crc),
            decompressed_content: RwBuf::new(
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
        result.currently_reading = ChunkRecord;
        result.chunk_state = Some(ChunkState {
            decompressor: get_decompressor(&mut HashMap::new(), &header.compression)?,
            crc: header.uncompressed_crc,
            uncompressed_data_hasher: Some(crc32fast::Hasher::new()),
            uncompressed_len: header.uncompressed_size,
            compressed_remaining: header.compressed_size,
            uncompressed_remaining: header.uncompressed_size,
            padding_after_compressed_data: 0,
        });
        Ok(result)
    }

    /// Get a mutable slice to write new MCAP data into. Call [`Self::set_written`] afterwards with
    /// the number of bytes successfully written.
    pub fn insert(&mut self, to_write: usize) -> &mut [u8] {
        self.file_data.tail_with_size(to_write)
    }

    /// Set the number of bytes successfully written into the buffer returned from [`Self::insert`]
    /// since the last [`Self::next_action`] call. Providing 0 indicates EOF to the reader.
    ///
    /// Panics if `written` is greater than the last `to_write` provided to [`Self::insert`].
    pub fn set_written(&mut self, written: usize) {
        if (self.file_data.data.len() - self.file_data.end) < written {
            panic!("set_written called with written > last inserted length");
        }
        self.last_write = Some(written);
    }

    /// Yields the next action the caller should take to progress through the file.
    pub fn next_action(&mut self) -> Option<McapResult<ReadAction>> {
        if let Some(written) = self.last_write.take() {
            if written == 0 {
                // at EOF. If the reader is not expecting end magic, and it isn't in the middle of a
                // record or chunk, this is OK.
                if self.options.skip_end_magic
                    && self.file_data.len() == 0
                    && self.decompressed_content.len() == 0
                    && self.chunk_state.is_none()
                {
                    return None;
                }
                if self.chunk_state.is_some() {
                    return Some(Err(McapError::UnexpectedEoc));
                }
                return Some(Err(McapError::UnexpectedEof));
            }
            self.file_data.end += written;
        }

        /// Macros for loading data into the reader. These return early with NeedMore(n) if
        /// more data is needed.
        ///
        /// load ensures that $n bytes are available unread in self.file_data.
        macro_rules! load {
            ($n:expr) => {{
                if self.file_data.len() < $n {
                    return Some(Ok(ReadAction::NeedMore($n - self.file_data.len())));
                }
                &self.file_data.data[self.file_data.start..self.file_data.start + $n]
            }};
        }

        // consume ensures that $n bytes are available in file_data, and marks them as read before
        // returning a slice containing those bytes.
        macro_rules! consume {
            ($n:expr) => {{
                if self.file_data.len() < $n {
                    return Some(Ok(ReadAction::NeedMore($n - self.file_data.len())));
                }
                let start = self.file_data.start;
                self.file_data.mark_read($n);
                &self.file_data.data[start..start + $n]
            }};
        }

        // decompress ensures that $n bytes are available in the uncompressed_content buffer.
        macro_rules! decompress {
            ($n: expr, $chunk_state: expr, $decompressor:expr) => {{
                match decompress_inner(
                    $decompressor,
                    $n,
                    &mut self.file_data,
                    &mut self.decompressed_content,
                    &mut $chunk_state.compressed_remaining,
                    &mut $chunk_state.uncompressed_remaining,
                ) {
                    Ok(None) => {
                        &self.decompressed_content.data
                            [self.decompressed_content.start..self.decompressed_content.start + $n]
                    }
                    Ok(Some(n)) => return Some(Ok(ReadAction::NeedMore(n))),
                    Err(err) => return Some(Err(err)),
                }
            }};
        }

        // returns early on error. This is similar to the ? operator, but it returns an
        // Option<Result> instead of a Result.
        macro_rules! check {
            ($t:expr) => {
                match $t {
                    Ok(t) => t,
                    Err(err) => return Some(Err(err.into())),
                }
            };
        }

        loop {
            match self.currently_reading.clone() {
                StartMagic => {
                    if !self.options.skip_start_magic {
                        let data = consume!(MAGIC.len());
                        if *data != *MAGIC {
                            return Some(Err(McapError::BadMagic));
                        }
                    }
                    self.currently_reading = CurrentlyReading::FileRecord;
                }
                FileRecord => {
                    let opcode_length_buf = load!(9);
                    let opcode = opcode_length_buf[0];
                    let len = u64::from_le_bytes(opcode_length_buf[1..].try_into().unwrap());
                    // Some record types are handled specially.
                    if opcode == op::CHUNK && !self.options.emit_chunks {
                        self.file_data.mark_read(9);
                        self.currently_reading = CurrentlyReading::ChunkHeader { len };
                        continue;
                    } else if opcode == op::DATA_END {
                        // The data end CRC needs to be checked against the CRC of the entire file
                        // up to the end of the previous record. We `take()` the data section hasher
                        // here before calling `mark_read()`, which would otherwise include too
                        // much data in the CRC.
                        let calculated =
                            self.file_data.hasher.take().map(|hasher| hasher.finalize());
                        self.file_data.mark_read(9);
                        self.currently_reading = DataEnd { len, calculated };
                        continue;
                    } else if opcode == op::FOOTER {
                        // The summary section CRC needs to be checked against the CRC of the entire
                        // summary section _including_ the first bytes of the footer record.
                        self.file_data.mark_read(9);
                        self.currently_reading = Footer {
                            len,
                            hasher: self.file_data.hasher.take(),
                        };
                        continue;
                    }
                    // For all other records, load the entire record into memory and yield to the
                    // caller.
                    let len = check!(len_as_usize(len));
                    let data = &consume!(9 + len)[9..];
                    return Some(Ok(ReadAction::GetRecord { data, opcode }));
                }
                CurrentlyReading::DataEnd { len, calculated } => {
                    let len = check!(len_as_usize(len));
                    let data = consume!(len);
                    let rec: crate::records::DataEnd = check!(std::io::Cursor::new(data).read_le());
                    let saved = rec.data_section_crc;
                    if let Some(calculated) = calculated {
                        if saved != 0 && calculated != saved {
                            return Some(Err(McapError::BadDataCrc { saved, calculated }));
                        }
                    }
                    self.currently_reading = FileRecord;
                    if self.options.validate_summary_section_crc {
                        self.file_data.hasher = Some(crc32fast::Hasher::new());
                    }
                    return Some(Ok(ReadAction::GetRecord {
                        data,
                        opcode: op::DATA_END,
                    }));
                }
                CurrentlyReading::Footer { len, hasher } => {
                    let len = check!(len_as_usize(len));
                    let data = consume!(len);
                    let footer: crate::records::Footer =
                        check!(std::io::Cursor::new(data).read_le());
                    if let Some(mut hasher) = hasher {
                        // Check the CRC of all bytes up to the CRC bytes in the footer
                        // record.
                        hasher.update(&data[..16]);
                        let calculated = hasher.finalize();
                        let saved = footer.summary_crc;
                        if saved != 0 && saved != calculated {
                            return Some(Err(McapError::BadSummaryCrc { saved, calculated }));
                        }
                    }
                    self.currently_reading = EndMagic;
                    return Some(Ok(ReadAction::GetRecord {
                        data,
                        opcode: op::FOOTER,
                    }));
                }
                CurrentlyReading::ChunkHeader { len } => {
                    // Load the chunk header from the file. The chunk header is of variable length,
                    // depending on the length of the compression string field, so we load
                    // enough bytes to read that length, then load more if neccessary.
                    const MIN_CHUNK_HEADER_SIZE: usize = 8 + 8 + 8 + 4 + 4 + 8;
                    let min_header_buf = load!(MIN_CHUNK_HEADER_SIZE);
                    let compression_len =
                        u32::from_le_bytes(min_header_buf[28..32].try_into().unwrap());
                    let header_len = MIN_CHUNK_HEADER_SIZE + compression_len as usize;
                    let header_buf = consume!(header_len);
                    let header: ChunkHeader = check!(std::io::Cursor::new(header_buf).read_le());
                    // Re-use or construct a compressor
                    let decompressor = check!(get_decompressor(
                        &mut self.decompressors,
                        &header.compression
                    ));
                    let padding_after_compressed_data = check!(len_as_usize(
                        len - (header_len as u64) - header.compressed_size
                    ));

                    let state = ChunkState {
                        decompressor,
                        uncompressed_data_hasher: if self.options.validate_chunk_crcs
                            && !self.options.prevalidate_chunk_crcs
                            && header.uncompressed_crc != 0
                            && header.compression.is_empty()
                        {
                            Some(crc32fast::Hasher::new())
                        } else {
                            None
                        },
                        compressed_remaining: header.compressed_size,
                        uncompressed_len: header.uncompressed_size,
                        uncompressed_remaining: header.uncompressed_size,
                        padding_after_compressed_data,
                        crc: header.uncompressed_crc,
                    };
                    self.decompressed_content.clear();
                    self.decompressed_content.hasher = if self.options.validate_chunk_crcs
                        && !self.options.prevalidate_chunk_crcs
                        && state.crc != 0
                        && !header.compression.is_empty()
                    {
                        Some(crc32fast::Hasher::new())
                    } else {
                        None
                    };
                    if self.options.prevalidate_chunk_crcs && state.crc != 0 {
                        self.currently_reading = ValidatingChunkCrc;
                    } else {
                        self.currently_reading = ChunkRecord;
                    }
                    self.chunk_state = Some(state);
                }
                ValidatingChunkCrc => {
                    // decompress all chunk records into memory and check their CRC before yielding
                    // records.
                    let state = self
                        .chunk_state
                        .as_mut()
                        .expect("chunk state should be set");
                    match &mut state.decompressor {
                        None => {
                            let to_load = check!(len_as_usize(state.compressed_remaining));
                            let records = load!(to_load);
                            let calculated = crc32fast::hash(records);
                            let saved = state.crc;
                            if calculated != saved {
                                return Some(Err(McapError::BadChunkCrc { saved, calculated }));
                            }
                            self.currently_reading = ChunkRecord;
                        }
                        Some(decompressor) => {
                            // decompress all available compressed data until there are no compressed
                            // bytes remaining. If not all of the compressed bytes are available in
                            // file_data, decompress! will return a NeedMore action for more data.
                            let uncompressed_len = check!(len_as_usize(state.uncompressed_len));
                            if self.decompressed_content.len() >= uncompressed_len {
                                let calculated =
                                    crc32fast::hash(self.decompressed_content.unread());
                                let saved = state.crc;
                                if calculated != saved {
                                    return Some(Err(McapError::BadChunkCrc { saved, calculated }));
                                }
                                self.currently_reading = ChunkRecord;
                                continue;
                            }
                            let _ = decompress!(uncompressed_len, state, decompressor);
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
                                self.currently_reading = PaddingAfterChunk;
                                continue;
                            }
                            let opcode_len_buf = load!(9);
                            let opcode = opcode_len_buf[0];
                            let len = check!(len_as_usize(u64::from_le_bytes(
                                opcode_len_buf[1..].try_into().unwrap(),
                            )));
                            let opcode_len_data = consume!(9 + len);
                            let data = &opcode_len_data[9..];
                            if let Some(hasher) = state.uncompressed_data_hasher.as_mut() {
                                hasher.update(opcode_len_data);
                            }
                            state.compressed_remaining -= (9 + len) as u64;
                            return Some(Ok(ReadAction::GetRecord { data, opcode }));
                        }
                        Some(decompressor) => {
                            if self.decompressed_content.len() == 0 {
                                self.decompressed_content.clear();
                            }
                            if state.uncompressed_remaining == 0
                                && self.decompressed_content.len() == 0
                            {
                                // We've consumed all compressed data. It's possible for there to
                                // still be data left in the chunk that has not yet been read into
                                // `self.file_data`. This can happen when a compressor adds extra
                                // bytes after its last frame. We need to treat this as "padding
                                // after the chunk" and skip over it before reading the next record.
                                state.padding_after_compressed_data +=
                                    check!(len_as_usize(state.compressed_remaining));
                                state.compressed_remaining = 0;
                                self.currently_reading = PaddingAfterChunk;
                                continue;
                            }
                            let opcode_len_buf = decompress!(9, state, decompressor);
                            let opcode = opcode_len_buf[0];
                            let len = check!(len_as_usize(u64::from_le_bytes(
                                opcode_len_buf[1..9].try_into().unwrap(),
                            )));
                            let _ = decompress!(9 + len, state, decompressor);
                            self.decompressed_content.mark_read(9);
                            let (start, end) = (
                                self.decompressed_content.start,
                                self.decompressed_content.start + len,
                            );
                            self.decompressed_content.mark_read(len);
                            let data = &self.decompressed_content.data[start..end];
                            return Some(Ok(ReadAction::GetRecord { data, opcode }));
                        }
                    }
                }
                PaddingAfterChunk => {
                    // discard any padding bytes after the chunk records and validate CRCs if
                    // neccessary
                    let state = self
                        .chunk_state
                        .as_mut()
                        .expect("chunk state should be set");
                    let _ = consume!(state.padding_after_compressed_data);
                    if let Some(mut decompressor) = state.decompressor.take() {
                        check!(decompressor.reset());
                        self.decompressors
                            .insert(decompressor.name().into(), decompressor);
                        if let Some(hasher) = self.decompressed_content.hasher.take() {
                            let calculated = hasher.finalize();
                            let saved = state.crc;
                            if saved != 0 && saved != calculated {
                                return Some(Err(McapError::BadChunkCrc { saved, calculated }));
                            }
                        }
                    } else if let Some(hasher) = state.uncompressed_data_hasher.take() {
                        let calculated = hasher.finalize();
                        let saved = state.crc;
                        if saved != 0 && saved != calculated {
                            return Some(Err(McapError::BadChunkCrc { saved, calculated }));
                        }
                    }
                    self.chunk_state = None;
                    self.currently_reading = FileRecord;
                }
                EndMagic => {
                    if !self.options.skip_end_magic {
                        let data = consume!(MAGIC.len());
                        if *data != *MAGIC {
                            return Some(Err(McapError::BadMagic));
                        }
                        self.file_data.mark_read(MAGIC.len());
                    }
                    return None;
                }
            }
        }
    }
}

/// Encapsulates the action the user should take next when reading an MCAP file.
///
pub enum ReadAction<'a> {
    /// The reader needs more data to provide the next record. Call [`LinearReader::insert`] then
    /// [`LinearReader::set_written`] to load more data. The value provided here is a hint for how
    /// much data to insert.
    NeedMore(usize),
    /// Read a record out of the MCAP file. Use [`crate::parse_record`] to parse the record.
    GetRecord { data: &'a [u8], opcode: u8 },
}

/// casts a 64-bit length value from an MCAP into a [`usize`].
fn len_as_usize(len: u64) -> McapResult<usize> {
    len.try_into().map_err(|_| McapError::TooLong(len))
}

/// casts a 64-bit length from an MCAP into a [`usize`], saturating to [`usize::MAX`] if too large.
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

// decompresses up to `n` bytes from `from` into `to`. Repeatedly calls `decompress` until
// either the input is exhausted or enough data has been written. Returns None if all required
// data has been decompressed, or Some(need) if more bytes need to be read from the input.
fn decompress_inner(
    decompressor: &mut Box<dyn Decompressor>,
    n: usize,
    src_buf: &mut RwBuf,
    dest_buf: &mut RwBuf,
    compressed_remaining: &mut u64,
    uncompressed_remaining: &mut u64,
) -> McapResult<Option<usize>> {
    if dest_buf.len() >= n {
        return Ok(None);
    }
    dest_buf.data.resize(dest_buf.start + n, 0);
    loop {
        let need = decompressor.next_read_size();
        let have = src_buf.len();
        if need > have {
            return Ok(Some(need - have));
        }
        let dst = &mut dest_buf.data[dest_buf.end..];
        if dst.is_empty() {
            return Ok(None);
        }
        if *uncompressed_remaining == 0 {
            return Err(McapError::UnexpectedEoc);
        }
        let src_len = std::cmp::min(have, clamp_to_usize(*compressed_remaining));
        let src = &src_buf.data[src_buf.start..src_buf.start + src_len];
        let res = decompressor.decompress(src, dst)?;
        src_buf.mark_read(res.consumed);
        dest_buf.end += res.wrote;
        *compressed_remaining -= res.consumed as u64;
        *uncompressed_remaining -= res.wrote as u64;
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
                .chunk_size(None)
                .create(&mut buf)?;
            let channel = std::sync::Arc::new(crate::Channel {
                id: 0,
                topic: "chat".to_owned(),
                schema: None,
                message_encoding: "json".to_owned(),
                metadata: BTreeMap::new(),
            });
            for n in 0..3 {
                writer.write(&crate::Message {
                    channel: channel.clone(),
                    sequence: n,
                    log_time: n as u64,
                    publish_time: n as u64,
                    data: (&[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]).into(),
                })?;
                if n == 1 {
                    writer.flush()?;
                }
            }
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
                id: 0,
                topic: "chat".to_owned(),
                schema: None,
                message_encoding: "json".to_owned(),
                metadata: BTreeMap::new(),
            });
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
                    reader.set_written(written);
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
    fn test_file_data_validation() -> McapResult<()> {
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
                    reader.set_written(written);
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
                    reader.set_written(written);
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
                op::MESSAGE,
                op::MESSAGE_INDEX,
                op::MESSAGE,
                op::MESSAGE_INDEX,
                op::DATA_END,
                op::CHANNEL,
                op::CHUNK_INDEX,
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
                        reader.set_written(written);
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
                    op::MESSAGE,
                    op::MESSAGE_INDEX,
                    op::MESSAGE,
                    op::MESSAGE_INDEX,
                    op::DATA_END,
                    op::CHANNEL,
                    op::CHUNK_INDEX,
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
                    reader.set_written(written);
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
                op::CHUNK,
                op::MESSAGE_INDEX,
                op::DATA_END,
                op::CHANNEL,
                op::CHUNK_INDEX,
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

    // Ensures that the internal buffer for the linear reader gets compacted regularly and does not
    // expand unbounded.
    #[test]
    fn test_buffer_compaction() -> McapResult<()> {
        let mut buf = Vec::new();
        {
            let mut cursor = std::io::Cursor::new(buf);
            let data = Vec::from_iter(std::iter::repeat(0x20u8).take(1024 * 1024 * 4));
            let mut writer = crate::WriteOptions::new()
                .compression(None)
                .chunk_size(None)
                .create(&mut cursor)?;
            let channel = std::sync::Arc::new(crate::Channel {
                topic: "chat".to_owned(),
                schema: None,
                message_encoding: "json".to_owned(),
                metadata: BTreeMap::new(),
            });
            writer.add_channel(&channel)?;
            for n in 0..3 {
                writer.write(&crate::Message {
                    channel: channel.clone(),
                    sequence: n,
                    log_time: n as u64,
                    publish_time: n as u64,
                    data: std::borrow::Cow::Borrowed(&data[..]),
                })?;
                if n == 1 {
                    writer.flush()?;
                }
            }
            writer.finish()?;
            drop(writer);
            buf = cursor.into_inner();
        }
        let mut reader = LinearReader::new();
        let mut cursor = std::io::Cursor::new(buf);
        let mut opcodes: Vec<u8> = Vec::new();
        let mut iter_count = 0;
        let mut max_needed: usize = 0;
        while let Some(action) = reader.next_action() {
            match action? {
                ReadAction::NeedMore(n) => {
                    max_needed = std::cmp::max(max_needed, n);
                    // read slightly more than requested, such that the data in the buffer does not
                    // hit zero after the next action.
                    let written = cursor.read(reader.insert(n + 1))?;
                    reader.set_written(written);
                    let buffer_size = reader.file_data.data.len();
                    assert!(
                        buffer_size < std::cmp::max(max_needed * 2, 4096),
                        "max needed: {0}, buffer size: {1}",
                        max_needed,
                        buffer_size
                    );
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

    #[test]
    fn test_decompression_does_not_fail() {
        let mut f = std::fs::File::open("tests/data/zstd_chunk_with_padding.mcap")
            .expect("failed to open file");
        let blocksize: usize = 1024;
        let mut reader = LinearReader::new();
        let mut message_count = 0;
        while let Some(action) = reader.next_action() {
            match action.expect("failed to get next action") {
                ReadAction::GetRecord { opcode, .. } => {
                    if opcode == op::MESSAGE {
                        message_count += 1;
                    }
                }
                ReadAction::NeedMore(_) => {
                    let read = f
                        .read(reader.insert(blocksize))
                        .expect("failed to read from file");
                    reader.set_written(read);
                }
            }
        }
        assert_eq!(message_count, 12);
    }
}
