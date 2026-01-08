//! Contains a [sans-io](https://sans-io.readthedocs.io/) MCAP reader struct, [`LinearReader`].
//! This can be used to read MCAP data from any source of bytes.
use std::collections::HashMap;

use super::decompressor::Decompressor;
use crate::{
    records::{op, sizes, ChunkHeader},
    sans_io::check_len,
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
    AfterEndMagic,
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

// MCAP records start with an opcode (1 byte) and a 64-bit length (8 bytes).
const OPCODE_LEN_SIZE: usize = 1 + 8;

mod rw_buf {
    /// A private struct that encapsulates a buffer with start and end cursors.
    #[derive(Default)]
    pub struct RwBuf {
        data: Vec<u8>,
        start: usize,
        end: usize,
        hasher: Option<crc32fast::Hasher>,
    }

    impl RwBuf {
        pub fn new(instantiate_hasher: bool) -> Self {
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
        pub fn mark_read(&mut self, read: usize) {
            assert!(
                read <= self.end - self.start,
                "attempted to read past end of buffer"
            );
            if let Some(hasher) = self.hasher.as_mut() {
                hasher.update(&self.data[self.start..self.start + read]);
            }
            self.start += read;
        }

        pub fn mark_written(&mut self, written: usize) {
            assert!(
                written <= self.unwritten().len(),
                "attempted to write past end of buffer"
            );
            self.end += written;
        }

        // returns the length of the unread section.
        pub fn len(&self) -> usize {
            self.end - self.start
        }

        // returns an immutable view of the entire unread section.
        pub fn unread(&self) -> &[u8] {
            &self.data[self.start..self.end]
        }

        pub fn unwritten(&self) -> &[u8] {
            &self.data[self.end..]
        }

        pub fn unwritten_mut(&mut self) -> &mut [u8] {
            &mut self.data[self.end..]
        }

        pub fn reserve_exact(&mut self, additional: usize) {
            self.data
                .resize(self.end.checked_add(additional).unwrap(), 0);
        }

        // returns a mutable view of the un-written part of the buffer, resizing as needed to ensure
        // N bytes are available to write into.
        pub fn tail_with_size(&mut self, n: usize) -> &mut [u8] {
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
        pub fn clear(&mut self) {
            self.data.clear();
            self.start = 0;
            self.end = 0;
        }

        pub fn consume(&mut self, n: usize) -> &[u8] {
            let start = self.start;
            self.mark_read(n);
            &self.data[start..start + n]
        }

        pub fn consume_without_hashing(&mut self, n: usize) -> &[u8] {
            assert!(
                self.end - self.start >= n,
                "attempted to read past end of buffer"
            );
            let start = self.start;
            self.start += n;
            &self.data[start..start + n]
        }

        pub fn hasher_mut(&mut self) -> &mut Option<crc32fast::Hasher> {
            &mut self.hasher
        }

        #[cfg(test)]
        pub fn buffer(&self) -> &[u8] {
            &self.data
        }
    }
}

use rw_buf::RwBuf;

/// Options for initializing [`LinearReader`].
#[derive(Debug, Default, Clone)]
pub struct LinearReaderOptions {
    /// If true, the reader will not expect the MCAP magic at the start of the stream.
    pub skip_start_magic: bool,
    /// If true, the reader will not expect the MCAP magic after the footer record.
    pub skip_end_magic: bool,
    /// If `skip_end_magic` is false and this is true, the reader will check that there are no
    /// bytes after the end magic.
    pub check_finishes_after_end_magic: bool,
    /// If true, the reader will yield entire chunk records. Otherwise, the reader will decompress
    /// and read into chunks, yielding the records inside.
    pub emit_chunks: bool,
    /// Enables chunk CRC validation. Ignored if `prevalidate_chunk_crcs: true`.
    pub validate_chunk_crcs: bool,
    /// Enables chunk CRC validation before yielding any messages from the chunk. Implies
    /// `validate_chunk_crcs: true`.
    pub prevalidate_chunk_crcs: bool,
    /// Enables data section CRC validation.
    pub validate_data_section_crc: bool,
    /// Enables summary section CRC validation.
    pub validate_summary_section_crc: bool,
    /// If Some(limit), the reader will return an error on any non-chunk record with length > `limit`.
    /// If used in conjunction with `prevalidate_chunk_crcs`, the reader will return an error on any
    /// chunk record where the compressed OR decompressed length are > `limit`.
    pub record_length_limit: Option<usize>,
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
    pub fn with_check_finishes_after_end_magic(
        mut self,
        check_finishes_after_end_magic: bool,
    ) -> Self {
        self.check_finishes_after_end_magic = check_finishes_after_end_magic;
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

    pub fn with_record_length_limit(mut self, record_length_limit: usize) -> Self {
        self.record_length_limit = Some(record_length_limit);
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
/// use mcap::sans_io::linear_reader::LinearReadEvent;
/// use mcap::McapResult;
///
/// // Asynchronously...
/// async fn read_async() -> McapResult<()> {
///     let mut file = AsyncFile::open("in.mcap").await.expect("couldn't open file");
///     let mut reader = mcap::sans_io::linear_reader::LinearReader::new();
///     while let Some(event) = reader.next_event() {
///         match event? {
///             LinearReadEvent::ReadRequest(need) => {
///                 let written = file.read(reader.insert(need)).await?;
///                 reader.notify_read(written);
///             },
///             LinearReadEvent::Record{ opcode, data } => {
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
///     let mut reader = mcap::sans_io::linear_reader::LinearReader::new();
///     while let Some(event) = reader.next_event() {
///         match event? {
///             LinearReadEvent::ReadRequest(need) => {
///                 let written = file.read(reader.insert(need))?;
///                 reader.notify_read(written);
///             },
///             LinearReadEvent::Record { opcode, data } => {
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
    // Auxiliary state specific to reading records out of chunks. This is stored outside of
    // CurrentlyReading to avoid needing to clone() it on every iteration.
    chunk_state: Option<ChunkState>,
    // MCAP data loaded from the file
    file_data: RwBuf,
    // data decompressed from compressed chunks
    decompressed_content: RwBuf,
    // decompressor that can be re-used between chunks.
    decompressors: HashMap<String, Box<dyn Decompressor>>,
    // Stores the number of bytes written into this reader since the last `next_event()` call.
    options: LinearReaderOptions,
    at_eof: bool,
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
            at_eof: false,
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

    /// Get a mutable slice to write new MCAP data into. Call [`Self::notify_read`] afterwards with
    /// the number of bytes successfully written.
    pub fn insert(&mut self, to_write: usize) -> &mut [u8] {
        self.file_data.tail_with_size(to_write)
    }

    /// Notify the number of bytes read into the linear reader
    /// since the last [`Self::next_event`] call. Providing 0 indicates EOF to the reader.
    ///
    /// Panics if `read` is greater than the last `to_write` provided to [`Self::insert`].
    pub fn notify_read(&mut self, written: usize) {
        if written == 0 {
            self.at_eof = true;
        }
        if self.file_data.unwritten().len() < written {
            panic!("notify_read called with n > last inserted length");
        }
        self.file_data.mark_written(written);
    }

    /// Yields the next event the caller should take to progress through the file.
    pub fn next_event(&mut self) -> Option<McapResult<LinearReadEvent<'_>>> {
        if self.at_eof {
            // At EOF. If the reader is not expecting end magic or has already seen it, and it isn't
            // in the middle of a record or chunk, this is OK.
            if (self.options.skip_end_magic || matches!(self.currently_reading, AfterEndMagic))
                && self.file_data.len() == 0
                && self.decompressed_content.len() == 0
                && self.chunk_state.is_none()
            {
                return None;
            }
            return Some(Err(McapError::UnexpectedEof));
        }
        /// Macros for loading data into the reader. These return early with Read(n) if
        /// more data is needed.
        ///
        /// load ensures that $n bytes are available unread in self.file_data.
        macro_rules! load {
            ($n:expr) => {{
                if self.file_data.len() < $n {
                    return Some(Ok(LinearReadEvent::ReadRequest($n - self.file_data.len())));
                }
                &self.file_data.unread()[..$n]
            }};
        }

        // consume ensures that $n bytes are available in file_data, and marks them as read before
        // returning a slice containing those bytes.
        macro_rules! consume {
            ($n:expr) => {{
                if self.file_data.len() < $n {
                    return Some(Ok(LinearReadEvent::ReadRequest($n - self.file_data.len())));
                }
                self.file_data.consume($n)
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
                    Ok(None) => &self.decompressed_content.unread()[..$n],
                    Ok(Some(n)) => return Some(Ok(LinearReadEvent::ReadRequest(n))),
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
                    let opcode_length_buf = load!(OPCODE_LEN_SIZE);
                    let opcode = opcode_length_buf[0];
                    let len = u64::from_le_bytes(opcode_length_buf[1..].try_into().unwrap());
                    // Some record types are handled specially.
                    if opcode == op::CHUNK && !self.options.emit_chunks {
                        self.file_data.mark_read(OPCODE_LEN_SIZE);
                        self.currently_reading = CurrentlyReading::ChunkHeader { len };
                        continue;
                    } else if opcode == op::DATA_END {
                        // The data end CRC needs to be checked against the CRC of the entire file
                        // up to the end of the previous record. We `take()` the data section hasher
                        // here before calling `mark_read()`, which would otherwise include too
                        // much data in the CRC.
                        let calculated = self
                            .file_data
                            .hasher_mut()
                            .take()
                            .map(|hasher| hasher.finalize());
                        self.file_data.mark_read(OPCODE_LEN_SIZE);
                        self.currently_reading = DataEnd { len, calculated };
                        continue;
                    } else if opcode == op::FOOTER {
                        // The summary section CRC needs to be checked against the CRC of the entire
                        // summary section _including_ the first bytes of the footer record.
                        self.file_data.mark_read(OPCODE_LEN_SIZE);
                        self.currently_reading = Footer {
                            len,
                            hasher: self.file_data.hasher_mut().take(),
                        };
                        continue;
                    }
                    // For all other records, load the entire record into memory and yield to the
                    // caller.
                    let len = check!(check_len(len, self.options.record_length_limit)
                        .ok_or(McapError::RecordTooLarge { opcode, len }));
                    let data = &consume!(OPCODE_LEN_SIZE + len)[OPCODE_LEN_SIZE..];
                    return Some(Ok(LinearReadEvent::Record { data, opcode }));
                }
                CurrentlyReading::DataEnd { len, calculated } => {
                    let len = check!(check_len(len, self.options.record_length_limit).ok_or(
                        McapError::RecordTooLarge {
                            opcode: op::DATA_END,
                            len
                        }
                    ));
                    let rec: crate::records::DataEnd =
                        check!(std::io::Cursor::new(load!(len)).read_le());
                    let saved = rec.data_section_crc;
                    if let Some(calculated) = calculated {
                        if saved != 0 && calculated != saved {
                            return Some(Err(McapError::BadDataCrc { saved, calculated }));
                        }
                    }
                    if self.options.validate_summary_section_crc {
                        *self.file_data.hasher_mut() = Some(crc32fast::Hasher::new());
                    }
                    let data = self.file_data.consume_without_hashing(len);
                    self.currently_reading = FileRecord;
                    return Some(Ok(LinearReadEvent::Record {
                        data,
                        opcode: op::DATA_END,
                    }));
                }
                CurrentlyReading::Footer { len, hasher } => {
                    let len = check!(check_len(len, self.options.record_length_limit).ok_or(
                        McapError::RecordTooLarge {
                            opcode: op::FOOTER,
                            len
                        }
                    ));
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
                    return Some(Ok(LinearReadEvent::Record {
                        data,
                        opcode: op::FOOTER,
                    }));
                }
                CurrentlyReading::ChunkHeader { len } => {
                    // Load the chunk header from the file. The chunk header is of variable length,
                    // depending on the length of the compression string field, so we load
                    // enough bytes to read that length, then load more if necessary.
                    let min_header_buf = load!(sizes::chunk::MIN_HEADER);
                    let compression_len = u32::from_le_bytes(
                        min_header_buf[sizes::chunk::COMPRESSION_LEN_OFFSET
                            ..sizes::chunk::COMPRESSION_LEN_END]
                            .try_into()
                            .unwrap(),
                    );
                    let header_len = sizes::chunk::MIN_HEADER + compression_len as usize;
                    let header_buf = consume!(header_len);
                    let header: ChunkHeader = check!(std::io::Cursor::new(header_buf).read_le());
                    // Re-use or construct a compressor
                    let decompressor = check!(get_decompressor(
                        &mut self.decompressors,
                        &header.compression
                    ));

                    let chunk_data_len = check!(len
                        .checked_sub(header_len as u64)
                        .ok_or(McapError::UnexpectedEoc));

                    let padding_after_compressed_data = check!(check_len(
                        check!(chunk_data_len
                            .checked_sub(header.compressed_size)
                            .ok_or(McapError::UnexpectedEoc)),
                        self.options.record_length_limit
                    )
                    .ok_or(McapError::ChunkTooLarge(len)));

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
                    *self.decompressed_content.hasher_mut() = if self.options.validate_chunk_crcs
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
                            let to_load = check!(check_len(
                                state.compressed_remaining,
                                self.options.record_length_limit
                            )
                            .ok_or(McapError::ChunkTooLarge(state.compressed_remaining)));
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
                            // file_data, decompress! will return a Read event for more data.
                            let uncompressed_len = check!(check_len(
                                state.uncompressed_len,
                                self.options.record_length_limit
                            )
                            .ok_or(McapError::ChunkTooLarge(state.uncompressed_len)));
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
                            let opcode_len_buf = load!(OPCODE_LEN_SIZE);
                            let opcode = opcode_len_buf[0];
                            let len = u64::from_le_bytes(opcode_len_buf[1..].try_into().unwrap());
                            let len = check!(check_len(len, self.options.record_length_limit)
                                .ok_or(McapError::RecordTooLarge { opcode, len }));
                            let opcode_len_data = consume!(OPCODE_LEN_SIZE + len);
                            let data = &opcode_len_data[OPCODE_LEN_SIZE..];
                            if let Some(hasher) = state.uncompressed_data_hasher.as_mut() {
                                hasher.update(opcode_len_data);
                            }
                            state.compressed_remaining -= (OPCODE_LEN_SIZE + len) as u64;
                            return Some(Ok(LinearReadEvent::Record { data, opcode }));
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
                                state.padding_after_compressed_data += check!(check_len(
                                    state.compressed_remaining,
                                    self.options.record_length_limit
                                )
                                .ok_or(McapError::ChunkTooLarge(state.compressed_remaining)));
                                state.compressed_remaining = 0;
                                self.currently_reading = PaddingAfterChunk;
                                continue;
                            }
                            let opcode_len_buf = decompress!(OPCODE_LEN_SIZE, state, decompressor);
                            let Some((&[opcode], rest)) = opcode_len_buf.split_first_chunk() else {
                                return Some(Err(McapError::UnexpectedEoc));
                            };
                            let Some((&len_buf, _)) = rest.split_first_chunk() else {
                                return Some(Err(McapError::UnexpectedEoc));
                            };
                            let len = u64::from_le_bytes(len_buf);
                            let len = check!(check_len(len, self.options.record_length_limit)
                                .ok_or(McapError::RecordTooLarge { opcode, len }));
                            let _ = decompress!(OPCODE_LEN_SIZE + len, state, decompressor);
                            self.decompressed_content.mark_read(OPCODE_LEN_SIZE);
                            let data = self.decompressed_content.consume(len);
                            return Some(Ok(LinearReadEvent::Record { data, opcode }));
                        }
                    }
                }
                PaddingAfterChunk => {
                    // discard any padding bytes after the chunk records and validate CRCs if
                    // necessary
                    let state = self
                        .chunk_state
                        .as_mut()
                        .expect("chunk state should be set");
                    let _ = consume!(state.padding_after_compressed_data);
                    if let Some(mut decompressor) = state.decompressor.take() {
                        check!(decompressor.reset());
                        self.decompressors
                            .insert(decompressor.name().into(), decompressor);
                        if let Some(hasher) = self.decompressed_content.hasher_mut().take() {
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
                    if self.options.skip_end_magic {
                        return None;
                    } else {
                        let data = consume!(MAGIC.len());
                        if *data == *MAGIC {
                            self.currently_reading = AfterEndMagic;
                            continue;
                        } else {
                            return Some(Err(McapError::BadMagic));
                        }
                    }
                }
                AfterEndMagic => {
                    if self.options.check_finishes_after_end_magic {
                        if self.file_data.len() > 0 {
                            return Some(Err(McapError::BytesAfterEndMagic));
                        } else if self.at_eof {
                            return None;
                        } else {
                            return Some(Ok(LinearReadEvent::ReadRequest(1)));
                        }
                    } else {
                        return None;
                    }
                }
            }
        }
    }
}

/// Events emitted by the linear reader.
#[derive(Debug)]
pub enum LinearReadEvent<'a> {
    /// The reader needs more data to provide the next record. Call [`LinearReader::insert`] then
    /// [`LinearReader::notify_read`] to load more data. The value provided here is a hint for how
    /// much data to insert.
    ReadRequest(usize),
    /// A new record from the MCAP file. Use [`crate::parse_record`] to parse the record.
    Record { data: &'a [u8], opcode: u8 },
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
    let additional = n.saturating_sub(dest_buf.len());
    if additional == 0 {
        return Ok(None);
    }
    dest_buf.reserve_exact(additional);
    loop {
        let need = decompressor.next_read_size();
        let have = src_buf.len();
        if need > have {
            return Ok(Some(need - have));
        }
        let dst = dest_buf.unwritten_mut();
        if dst.is_empty() {
            return Ok(None);
        }
        if *uncompressed_remaining == 0 {
            return Err(McapError::UnexpectedEoc);
        }
        let src_len = have.min(clamp_to_usize(*compressed_remaining));
        let src = &src_buf.unread()[..src_len];
        let res = decompressor.decompress(src, dst)?;
        src_buf.mark_read(res.consumed);
        dest_buf.mark_written(res.wrote);
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
        while let Some(event) = reader.next_event() {
            match event? {
                LinearReadEvent::ReadRequest(n) => {
                    let written = cursor.read(reader.insert(n))?;
                    reader.notify_read(written);
                }
                LinearReadEvent::Record { data, opcode } => {
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
        while let Some(event) = reader.next_event() {
            match event? {
                LinearReadEvent::ReadRequest(n) => {
                    let written = cursor.read(reader.insert(n))?;
                    reader.notify_read(written);
                }
                LinearReadEvent::Record { data, opcode } => {
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
    fn test_record_length_limit() {
        let mut reader = LinearReader::new_with_options(
            LinearReaderOptions::default().with_record_length_limit(10),
        );
        let mut cursor = std::io::Cursor::new(basic_chunked_file(None).unwrap());
        let mut opcodes: Vec<u8> = Vec::new();
        let mut iter_count = 0;
        while let Some(event) = reader.next_event() {
            match event {
                Ok(LinearReadEvent::ReadRequest(n)) => {
                    let written = cursor
                        .read(reader.insert(n))
                        .expect("insert should not fail");
                    reader.notify_read(written);
                }
                Ok(LinearReadEvent::Record { data, opcode }) => {
                    opcodes.push(opcode);
                    parse_record(opcode, data).expect("parse should not fail");
                }
                Err(err) => {
                    assert!(matches!(
                        err,
                        McapError::RecordTooLarge {
                            opcode: op::HEADER,
                            len: 22
                        }
                    ));
                    return;
                }
            }
            iter_count += 1;
            // guard against infinite loop
            assert!(iter_count < 10000);
        }
        panic!("should have errored")
    }

    fn test_chunked(
        compression: Option<Compression>,
        options: LinearReaderOptions,
    ) -> McapResult<()> {
        let mut reader = LinearReader::new_with_options(options);
        let mut cursor = std::io::Cursor::new(basic_chunked_file(compression)?);
        let mut opcodes: Vec<u8> = Vec::new();
        let mut iter_count = 0;
        while let Some(event) = reader.next_event() {
            match event? {
                LinearReadEvent::ReadRequest(n) => {
                    let written = cursor.read(reader.insert(n))?;
                    reader.notify_read(written);
                }
                LinearReadEvent::Record { data, opcode } => {
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
                op::STATISTICS,
                op::CHUNK_INDEX,
                op::CHUNK_INDEX,
                op::SUMMARY_OFFSET,
                op::SUMMARY_OFFSET,
                op::SUMMARY_OFFSET,
                op::FOOTER
            ]
        );
        Ok(())
    }
    use assert_matches::assert_matches;
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
            while let Some(event) = reader.next_event() {
                match event? {
                    LinearReadEvent::ReadRequest(n) => {
                        let written = cursor.read(reader.insert(n))?;
                        reader.notify_read(written);
                    }
                    LinearReadEvent::Record { data, opcode } => {
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
                    op::STATISTICS,
                    op::CHUNK_INDEX,
                    op::CHUNK_INDEX,
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
        while let Some(event) = reader.next_event() {
            match event? {
                LinearReadEvent::ReadRequest(n) => {
                    let written = cursor.read(reader.insert(n))?;
                    reader.notify_read(written);
                }
                LinearReadEvent::Record { data, opcode } => {
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
                op::STATISTICS,
                op::CHUNK_INDEX,
                op::CHUNK_INDEX,
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
    fn test_buffer_compevent() -> McapResult<()> {
        let mut buf = Vec::new();
        {
            let mut cursor = std::io::Cursor::new(buf);
            let data = Vec::from_iter(std::iter::repeat_n(0x20u8, 1024 * 1024 * 4));
            let mut writer = crate::WriteOptions::new()
                .compression(None)
                .chunk_size(None)
                .create(&mut cursor)?;
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
        while let Some(event) = reader.next_event() {
            match event? {
                LinearReadEvent::ReadRequest(n) => {
                    max_needed = std::cmp::max(max_needed, n);
                    // read slightly more than requested, such that the data in the buffer does not
                    // hit zero after the next event.
                    let written = cursor.read(reader.insert(n + 1))?;
                    reader.notify_read(written);
                    let buffer_size = reader.file_data.buffer().len();
                    assert!(
                        buffer_size < std::cmp::max(max_needed * 2, 4096),
                        "max needed: {max_needed}, buffer size: {buffer_size}",
                    );
                }
                LinearReadEvent::Record { data, opcode } => {
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
        while let Some(event) = reader.next_event() {
            match event.expect("failed to get next event") {
                LinearReadEvent::Record { opcode, .. } => {
                    if opcode == op::MESSAGE {
                        message_count += 1;
                    }
                }
                LinearReadEvent::ReadRequest(_) => {
                    let read = f
                        .read(reader.insert(blocksize))
                        .expect("failed to read from file");
                    reader.notify_read(read);
                }
            }
        }
        assert_eq!(message_count, 12);
    }

    #[test]
    fn test_handles_failed_to_close_chunks() {
        let mut f =
            std::fs::File::open("tests/data/chunk_not_closed.mcap").expect("failed to open file");
        let mut output = vec![];
        f.read_to_end(&mut output).expect("failed to read");

        let mut reader = LinearReader::new();
        reader.insert(output.len()).copy_from_slice(&output[..]);
        reader.notify_read(output.len());

        // the first record is the header;
        let next = reader
            .next_event()
            .expect("there should be one event")
            .expect("first record should be header");

        let LinearReadEvent::Record { opcode: 1, .. } = next else {
            panic!("expected first record to be header");
        };

        let next = reader.next_event().expect("there should be one event");

        // test fails with unexpected EOC because sizes are u64::max
        assert_matches!(next, Err(McapError::UnexpectedEoc));
    }

    #[test]
    fn test_notifying_eof_after_writing_whole_file() {
        let mcap = basic_chunked_file(None).unwrap();
        let mut reader = LinearReader::new();
        reader.insert(mcap.len()).copy_from_slice(mcap.as_slice());
        reader.notify_read(mcap.len());
        while let Some(event) = reader.next_event() {
            match event.unwrap() {
                LinearReadEvent::ReadRequest(_) => {
                    panic!("should not request read because file is complete");
                }
                LinearReadEvent::Record { .. } => {}
            }
        }
        reader.notify_read(0);
        assert_matches!(reader.next_event(), None);
    }

    #[test]
    fn test_trailing_garbage_after_end_magic() {
        let mcap = basic_chunked_file(None).unwrap();
        let mut reader = LinearReader::new_with_options(
            LinearReaderOptions::default().with_check_finishes_after_end_magic(true),
        );
        reader.insert(mcap.len()).copy_from_slice(mcap.as_slice());
        reader.notify_read(mcap.len());
        while let Some(event) = reader.next_event() {
            match event.unwrap() {
                LinearReadEvent::ReadRequest(_) => break,
                LinearReadEvent::Record { .. } => {}
            }
        }
        assert_matches!(
            reader.next_event(),
            Some(Ok(LinearReadEvent::ReadRequest(_)))
        );
        let garbage = b"garbage";
        reader.insert(garbage.len()).copy_from_slice(garbage);
        reader.notify_read(garbage.len());
        assert_matches!(
            reader.next_event(),
            Some(Err(McapError::BytesAfterEndMagic))
        );
    }
}
