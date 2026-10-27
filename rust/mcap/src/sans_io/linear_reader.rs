//! Contains a [sans-io](https://sans-io.readthedocs.io/) MCAP reader struct, [`LinearReader`].
//! This can be used to read MCAP data from any source of bytes.
use super::decompressor::{
    check_decompress_result, no_progress_error, Decompressor, Decompressors,
};
use crate::{
    records::{op, ChunkHeader},
    sans_io::check_len,
    McapError, McapResult, MAGIC,
};
use binrw::BinReaderExt;

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
    /// An error left the reader with no record boundary to resume from. Every later
    /// `next_event` returns `None`.
    Failed,
}
use CurrentlyReading::*;

struct ChunkState {
    // The decompressor to use for loading records for this chunk, taken out of
    // `LinearReader::decompressors` for the duration of the chunk. None if not compressed.
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
    /// chunk record where the compressed OR decompressed length are > `limit`. It also caps how
    /// much compressed chunk data is buffered for a decompressor.
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
    // Reused across chunks. Starts with the built-in decoders.
    decompressors: Decompressors,
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
            decompressors: Decompressors::builtin(),
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
            decompressor: get_decompressor(&mut result.decompressors, &header.compression)?,
            crc: header.uncompressed_crc,
            uncompressed_data_hasher: Some(crc32fast::Hasher::new()),
            uncompressed_len: header.uncompressed_size,
            compressed_remaining: header.compressed_size,
            uncompressed_remaining: header.uncompressed_size,
            padding_after_compressed_data: 0,
        });
        Ok(result)
    }

    /// Registers a decompressor for chunks whose `compression` field equals
    /// [`Decompressor::name`].
    ///
    /// Call this before reading the chunks it should decode. The built-in `"zstd"` and `"lz4"`
    /// decoders are registered by default when their features are enabled. Registering under an
    /// existing name replaces that decompressor from the next chunk on; a chunk with no
    /// registration for its compression string returns [`McapError::UnsupportedCompression`].
    ///
    /// Before each `decompress` call, this reader buffers [`Decompressor::next_read_size`] bytes,
    /// or up to 64 KiB when that returns 0. A small hint produces small reads. After a call that
    /// makes no progress, it buffers at least twice as much input, or the hint if that is larger.
    /// If that call still makes no progress, it returns [`McapError::ChunkTooLarge`] when the
    /// buffer is already at [`record_length_limit`](LinearReaderOptions::record_length_limit), or
    /// a decompression error when every remaining compressed byte is already buffered.
    ///
    /// Returns [`McapError::EmptyDecompressorName`] when `name()` is empty.
    pub fn add_decompressor(
        &mut self,
        decompressor: impl Decompressor + 'static,
    ) -> McapResult<()> {
        self.decompressors.register(decompressor)
    }

    /// Whether a decompressor is registered under `name`, counting one that is decoding the
    /// current chunk.
    #[cfg(test)]
    pub(crate) fn has_decompressor(&self, name: &str) -> bool {
        self.decompressors.contains(name)
            || self
                .chunk_state
                .as_ref()
                .and_then(|state| state.decompressor.as_ref())
                .is_some_and(|decompressor| decompressor.name() == name)
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

    /// Whether the record most recently yielded came from inside a chunk.
    pub(crate) fn in_chunk(&self) -> bool {
        self.chunk_state.is_some()
    }

    /// Whether the reader ended on an error it could not resume from. Lets a caller that
    /// keeps going after errors tell that ending apart from reaching the end of the file.
    pub fn failed(&self) -> bool {
        matches!(self.currently_reading, Failed)
    }

    /// Yields the next event the caller should take to progress through the file.
    ///
    /// Some errors are non-terminal: the reader may yield more events after returning one.
    /// Callers should keep calling `next_event` until it returns `None`.
    pub fn next_event(&mut self) -> Option<McapResult<LinearReadEvent<'_>>> {
        if matches!(self.currently_reading, Failed) {
            return None;
        }
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
            self.currently_reading = Failed;
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
                    self.options.record_length_limit,
                ) {
                    Ok(None) => &self.decompressed_content.unread()[..$n],
                    Ok(Some(n)) => return Some(Ok(LinearReadEvent::ReadRequest(n))),
                    Err(err) => fail!(err),
                }
            }};
        }

        // Returns an error that the reader cannot resume after. The reader ends here: every later
        // next_event() returns None.
        macro_rules! fail {
            ($err:expr) => {{
                self.currently_reading = Failed;
                return Some(Err($err.into()));
            }};
        }

        // returns early on error. This is similar to the ? operator, but it returns an
        // Option<Result> instead of a Result. The reader ends like fail!.
        macro_rules! check {
            ($t:expr) => {
                match $t {
                    Ok(t) => t,
                    Err(err) => fail!(err),
                }
            };
        }

        loop {
            match self.currently_reading.clone() {
                StartMagic => {
                    if !self.options.skip_start_magic {
                        let data = consume!(MAGIC.len());
                        if *data != *MAGIC {
                            fail!(McapError::BadMagic);
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
                            // Report once. The record is loaded, so the next call yields it and
                            // reading goes on with the summary section.
                            self.currently_reading = DataEnd {
                                len: len as u64,
                                calculated: None,
                            };
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
                    let data = load!(len);
                    let footer: crate::records::Footer =
                        check!(std::io::Cursor::new(data).read_le());
                    if let Some(mut hasher) = hasher {
                        // Check the CRC of all bytes up to the CRC bytes in the footer
                        // record.
                        hasher.update(&data[..16]);
                        let calculated = hasher.finalize();
                        let saved = footer.summary_crc;
                        if saved != 0 && saved != calculated {
                            // Report once. The record is loaded, so the next call yields it
                            // and reading goes on with the end magic.
                            self.currently_reading = Footer {
                                len: len as u64,
                                hasher: None,
                            };
                            return Some(Err(McapError::BadSummaryCrc { saved, calculated }));
                        }
                    }
                    let data = consume!(len);
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
                    const MIN_CHUNK_HEADER_SIZE: usize = 8  // start time
                                                       + 8  // end time
                                                       + 8  // uncompressed size
                                                       + 4  // uncompressed CRC
                                                       + 4  // compression string length
                                                       + 8; // compressed size

                    let min_header_buf = load!(MIN_CHUNK_HEADER_SIZE);
                    let compression_len =
                        u32::from_le_bytes(min_header_buf[28..32].try_into().unwrap());
                    // compression_len is untrusted. Reject an overlong header before loading it.
                    let header_len =
                        (MIN_CHUNK_HEADER_SIZE as u64).saturating_add(u64::from(compression_len));
                    if header_len > len {
                        fail!(McapError::BadChunkLength {
                            header: header_len,
                            available: len,
                        });
                    }
                    let header_len =
                        check!(check_len(header_len, self.options.record_length_limit)
                            .ok_or(McapError::ChunkTooLarge(header_len)));
                    let header_buf = consume!(header_len);
                    let header: ChunkHeader = check!(std::io::Cursor::new(header_buf).read_le());

                    // Length checks run before the decompressor is taken out of the map. An error
                    // here must leave the decoder in place for a later chunk.
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

                    let decompressor = check!(get_decompressor(
                        &mut self.decompressors,
                        &header.compression
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
                                // Skip the chunk: its records are the bytes in doubt. They
                                // still count toward the data section CRC. Reading resumes
                                // with the record after the chunk.
                                self.file_data.mark_read(to_load);
                                state.compressed_remaining = 0;
                                self.currently_reading = PaddingAfterChunk;
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
                                    // Skip the chunk: its records are the bytes in doubt.
                                    // Compressed bytes the decompressor did not need are
                                    // padding to step over. Reading resumes with the record
                                    // after the chunk.
                                    self.decompressed_content.clear();
                                    state.padding_after_compressed_data += check!(check_len(
                                        state.compressed_remaining,
                                        self.options.record_length_limit
                                    )
                                    .ok_or(McapError::ChunkTooLarge(state.compressed_remaining)));
                                    state.compressed_remaining = 0;
                                    state.uncompressed_remaining = 0;
                                    self.currently_reading = PaddingAfterChunk;
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
                                fail!(McapError::UnexpectedEoc);
                            };
                            let Some((&len_buf, _)) = rest.split_first_chunk() else {
                                fail!(McapError::UnexpectedEoc);
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
                    // necessary. Check the CRC before reset: a failed reset must not hide a bad
                    // chunk CRC. Move back to file records before returning either error so a
                    // later next_event does not consume this padding again.
                    let padding = self
                        .chunk_state
                        .as_ref()
                        .expect("chunk state should be set")
                        .padding_after_compressed_data;
                    let _ = consume!(padding);
                    let crc_error = {
                        let state = self
                            .chunk_state
                            .as_mut()
                            .expect("chunk state should be set");
                        // for_chunk always installs a decompressed-content hasher, including for
                        // uncompressed chunks. Those records are read from file_data, so only
                        // uncompressed_data_hasher has hashed them.
                        let hasher = if state.decompressor.is_some() {
                            self.decompressed_content.hasher_mut().take()
                        } else {
                            state.uncompressed_data_hasher.take()
                        };
                        let saved = state.crc;
                        hasher
                            .map(|hasher| hasher.finalize())
                            .and_then(|calculated| {
                                (saved != 0 && saved != calculated)
                                    .then_some(McapError::BadChunkCrc { saved, calculated })
                            })
                    };
                    let decompressor = self
                        .chunk_state
                        .as_mut()
                        .expect("chunk state should be set")
                        .decompressor
                        .take();
                    self.chunk_state = None;
                    self.currently_reading = FileRecord;
                    let reset_result = match decompressor {
                        Some(decompressor) => self.decompressors.restore(decompressor),
                        None => Ok(()),
                    };
                    match (crc_error, reset_result) {
                        (Some(err), _) => return Some(Err(err)),
                        (None, Err(err)) => return Some(Err(err)),
                        (None, Ok(())) => {}
                    }
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
                            fail!(McapError::BadMagic);
                        }
                    }
                }
                Failed => return None,
                AfterEndMagic => {
                    if self.options.check_finishes_after_end_magic {
                        if self.file_data.len() > 0 {
                            fail!(McapError::BytesAfterEndMagic);
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

/// Takes the decompressor for `name` out of `decompressors` for the duration of a chunk.
///
/// Returns `None` for an empty name, which means the chunk is uncompressed.
fn get_decompressor(
    decompressors: &mut Decompressors,
    name: &str,
) -> McapResult<Option<Box<dyn Decompressor>>> {
    if name.is_empty() {
        return Ok(None);
    }
    decompressors
        .take(name)
        .map(Some)
        .ok_or_else(|| McapError::UnsupportedCompression(name.into()))
}

/// Compressed bytes to request when a decompressor's `next_read_size()` is 0.
const DEFAULT_COMPRESSED_READ_SIZE: usize = 64 * 1024;

// decompresses up to `n` bytes from `src_buf` into `dest_buf`. Repeatedly calls `decompress` until
// either the input is exhausted or enough data has been written. Returns None if all required
// data has been decompressed, or Some(need) if more bytes need to be read from the input.
//
// Compressed input is buffered up to the smaller of the chunk's remaining compressed size and
// `record_length_limit`, so a corrupt chunk size cannot make a large `next_read_size()` allocate
// more than the limit.
fn decompress_inner(
    decompressor: &mut Box<dyn Decompressor>,
    n: usize,
    src_buf: &mut RwBuf,
    dest_buf: &mut RwBuf,
    compressed_remaining: &mut u64,
    uncompressed_remaining: &mut u64,
    record_length_limit: Option<usize>,
) -> McapResult<Option<usize>> {
    let additional = n.saturating_sub(dest_buf.len());
    if additional == 0 {
        return Ok(None);
    }
    dest_buf.reserve_exact(additional);
    let max_buffered = record_length_limit.unwrap_or(usize::MAX);
    loop {
        let remaining = clamp_to_usize(*compressed_remaining);
        let have = src_buf.len();
        let need = decompressor
            .next_read_size()
            .min(remaining)
            .min(max_buffered);
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
        // A hint of 0 means "any amount". Do not call decompress with an empty buffer while the
        // chunk still has compressed bytes, or a frame boundary (where the built-in lz4 and zstd
        // decoders hint 0) looks like a stall. Checked after the output is known to have room, so
        // a full output buffer does not trigger an extra read.
        if have == 0 && remaining > 0 {
            return Ok(Some(
                DEFAULT_COMPRESSED_READ_SIZE
                    .min(remaining)
                    .min(max_buffered)
                    .max(1),
            ));
        }
        let src_len = have.min(remaining);
        let src = &src_buf.unread()[..src_len];
        let dst_len = dst.len();
        let res = check_decompress_result(decompressor.decompress(src, dst)?, src_len, dst_len)?;
        if res.consumed == 0 && res.wrote == 0 {
            // No progress asks for more input. Count it as a stall only once every remaining
            // compressed byte was already passed in. Without a larger hint, double the buffered
            // input so a decoder waiting for a whole frame does not get one byte per call.
            if src_len < remaining {
                if have >= max_buffered {
                    return Err(McapError::ChunkTooLarge(*compressed_remaining));
                }
                let want = decompressor
                    .next_read_size()
                    .max(have.saturating_mul(2))
                    .min(remaining)
                    .min(max_buffered);
                return Ok(Some(want - have));
            }
            return Err(no_progress_error());
        }
        if res.wrote as u64 > *uncompressed_remaining {
            return Err(McapError::DecompressionError(format!(
                "decompressor wrote {} bytes with only {} uncompressed bytes remaining",
                res.wrote, *uncompressed_remaining
            )));
        }
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
    use assert_matches::assert_matches;
    use std::collections::BTreeMap;
    use std::io::Read;

    const DEFAULT_LIBRARY_LENGTH: u64 = crate::LIBRARY_IDENTIFIER.len() as u64;

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
                    match err {
                        McapError::RecordTooLarge { opcode, len } => {
                            assert_eq!(opcode, op::HEADER);
                            assert_eq!(len, 8 + DEFAULT_LIBRARY_LENGTH);
                        }
                        _ => panic!("expected RecordTooLarge, got {err:?}"),
                    }
                    return;
                }
            }
            iter_count += 1;
            // guard against infinite loop
            assert!(iter_count < 10000);
        }
        panic!("should have errored")
    }

    /// Drives the reader over `bytes` to the end, recording every record opcode and error in
    /// the order they were returned.
    fn collect_events(bytes: &[u8], options: LinearReaderOptions) -> Vec<Result<u8, McapError>> {
        let mut reader = LinearReader::new_with_options(options);
        let mut cursor = std::io::Cursor::new(bytes);
        let mut events = Vec::new();
        while let Some(event) = reader.next_event() {
            match event {
                Ok(LinearReadEvent::ReadRequest(n)) => {
                    let written = cursor
                        .read(reader.insert(n))
                        .expect("insert should not fail");
                    reader.notify_read(written);
                }
                Ok(LinearReadEvent::Record { opcode, .. }) => events.push(Ok(opcode)),
                Err(err) => events.push(Err(err)),
            }
            assert!(events.len() < 10000, "the reader never ended");
        }
        events
    }

    #[test]
    fn an_error_the_reader_cannot_resume_after_ends_it() {
        let file = basic_chunked_file(None).unwrap();
        let events = collect_events(
            &file,
            LinearReaderOptions::default().with_record_length_limit(10),
        );
        assert_matches!(
            events.as_slice(),
            [Err(McapError::RecordTooLarge { .. })],
            "{events:?}"
        );

        // Cut off inside a record: the reader reports it once and ends.
        let events = collect_events(&file[..file.len() / 2], LinearReaderOptions::default());
        assert_matches!(events.last(), Some(Err(McapError::UnexpectedEof)));
        assert_eq!(
            events.iter().filter(|event| event.is_err()).count(),
            1,
            "{events:?}"
        );
    }

    /// Drives the reader over `bytes` to the end and reports whether it ended in failure.
    fn ends_in_failure(bytes: &[u8], options: LinearReaderOptions) -> bool {
        let mut reader = LinearReader::new_with_options(options);
        let mut cursor = std::io::Cursor::new(bytes);
        let mut events = 0;
        while let Some(event) = reader.next_event() {
            if let Ok(LinearReadEvent::ReadRequest(n)) = event {
                let written = cursor
                    .read(reader.insert(n))
                    .expect("insert should not fail");
                reader.notify_read(written);
            }
            events += 1;
            assert!(events < 10000, "the reader never ended");
        }
        reader.failed()
    }

    #[test]
    fn failed_tells_an_ended_reader_from_one_that_finished() {
        let file = basic_chunked_file(None).unwrap();
        assert!(!ends_in_failure(&file, LinearReaderOptions::default()));
        assert!(ends_in_failure(
            &file[..file.len() / 2],
            LinearReaderOptions::default()
        ));

        // A CRC mismatch is reported but does not end the reader.
        let mut bad_footer = file.clone();
        let crc_field = first_record_offset(&file, op::FOOTER) + OPCODE_LEN_SIZE + 16;
        bad_footer[crc_field] ^= 0xFF;
        assert!(!ends_in_failure(
            &bad_footer,
            LinearReaderOptions::default().with_validate_summary_section_crc(true)
        ));
    }

    #[test]
    fn a_bad_chunk_crc_found_after_its_records_does_not_end_the_reader() {
        let mut file = basic_chunked_file(None).unwrap();
        // Flip the last payload byte of the first message, which lies inside the first chunk.
        let payload: [u8; 10] = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9];
        let payload_at = file
            .windows(payload.len())
            .position(|window| window == payload)
            .expect("first message payload");
        file[payload_at + payload.len() - 1] ^= 0xFF;

        // Checked after the chunk's records: the mismatch is reported once, then reading goes
        // on with the next chunk's records and the rest of the file.
        let events = collect_events(
            &file,
            LinearReaderOptions::default().with_validate_chunk_crcs(true),
        );
        let error_at = events
            .iter()
            .position(|event| matches!(event, Err(McapError::BadChunkCrc { .. })))
            .expect("bad chunk crc reported");
        let after: Vec<u8> = events[error_at + 1..]
            .iter()
            .map(|event| *event.as_ref().expect("no error after the chunk crc"))
            .collect();
        assert_eq!(&after[..2], &[op::MESSAGE_INDEX, op::MESSAGE], "{events:?}");
        assert_eq!(after.last(), Some(&op::FOOTER), "{events:?}");

        let messages_with_post_check = events
            .iter()
            .filter(|event| matches!(event, Ok(op::MESSAGE)))
            .count();

        // Checked before the chunk's records: the mismatch is reported once, the chunk's
        // records are skipped, and reading goes on with the record after the chunk.
        let events = collect_events(
            &file,
            LinearReaderOptions::default().with_prevalidate_chunk_crcs(true),
        );
        let error_at = events
            .iter()
            .position(|event| matches!(event, Err(McapError::BadChunkCrc { .. })))
            .expect("bad chunk crc reported");
        let after: Vec<u8> = events[error_at + 1..]
            .iter()
            .map(|event| *event.as_ref().expect("no error after the chunk crc"))
            .collect();
        assert_eq!(&after[..2], &[op::MESSAGE_INDEX, op::MESSAGE], "{events:?}");
        assert_eq!(after.last(), Some(&op::FOOTER), "{events:?}");
        let messages_with_pre_check = events
            .iter()
            .filter(|event| matches!(event, Ok(op::MESSAGE)))
            .count();
        assert!(
            messages_with_pre_check < messages_with_post_check,
            "the bad chunk's records were yielded: {events:?}"
        );
    }

    /// Offsets of the top-level records in `bytes`, with their opcodes.
    fn top_level_records(bytes: &[u8]) -> Vec<(u8, usize)> {
        let mut records = Vec::new();
        let mut offset = MAGIC.len();
        while offset + OPCODE_LEN_SIZE <= bytes.len() - MAGIC.len() {
            let opcode = bytes[offset];
            let len = u64::from_le_bytes(bytes[offset + 1..offset + 9].try_into().unwrap());
            records.push((opcode, offset));
            offset += OPCODE_LEN_SIZE + len as usize;
        }
        records
    }

    fn first_record_offset(bytes: &[u8], opcode: u8) -> usize {
        top_level_records(bytes)
            .into_iter()
            .find(|(record_opcode, _)| *record_opcode == opcode)
            .map(|(_, offset)| offset)
            .expect("record should exist")
    }

    #[cfg(feature = "lz4")]
    #[test]
    fn a_bad_chunk_crc_checked_first_skips_a_compressed_chunk() {
        let mut file = basic_chunked_file(Some(Compression::Lz4)).unwrap();
        // ChunkHeader: message_start_time, message_end_time, uncompressed_size (8 each), then
        // uncompressed_crc.
        let crc_field = first_record_offset(&file, op::CHUNK) + OPCODE_LEN_SIZE + 24;
        file[crc_field] ^= 0xFF;

        let events = collect_events(
            &file,
            LinearReaderOptions::default().with_prevalidate_chunk_crcs(true),
        );
        let error_at = events
            .iter()
            .position(|event| matches!(event, Err(McapError::BadChunkCrc { .. })))
            .expect("bad chunk crc reported");
        let after: Vec<u8> = events[error_at + 1..]
            .iter()
            .map(|event| *event.as_ref().expect("no error after the chunk crc"))
            .collect();
        assert_eq!(&after[..2], &[op::MESSAGE_INDEX, op::MESSAGE], "{events:?}");
        assert_eq!(after.last(), Some(&op::FOOTER), "{events:?}");
    }

    #[test]
    fn a_bad_section_crc_is_reported_once_and_the_record_follows() {
        let file = basic_chunked_file(None).unwrap();

        // DataEnd body: data_section_crc.
        let mut bad_data_end = file.clone();
        let crc_field = first_record_offset(&file, op::DATA_END) + OPCODE_LEN_SIZE;
        bad_data_end[crc_field] ^= 0xFF;
        let events = collect_events(
            &bad_data_end,
            LinearReaderOptions::default().with_validate_data_section_crc(true),
        );
        let error_at = events
            .iter()
            .position(|event| matches!(event, Err(McapError::BadDataCrc { .. })))
            .expect("bad data section crc reported");
        assert_matches!(events[error_at + 1], Ok(op::DATA_END), "{events:?}");
        assert_eq!(
            events.iter().filter(|event| event.is_err()).count(),
            1,
            "{events:?}"
        );
        assert_matches!(events.last(), Some(Ok(op::FOOTER)), "{events:?}");

        // Footer body: summary_start, summary_offset_start, summary_crc.
        let mut bad_footer = file.clone();
        let crc_field = first_record_offset(&file, op::FOOTER) + OPCODE_LEN_SIZE + 16;
        bad_footer[crc_field] ^= 0xFF;
        let events = collect_events(
            &bad_footer,
            LinearReaderOptions::default().with_validate_summary_section_crc(true),
        );
        assert_matches!(
            events.as_slice(),
            [.., Err(McapError::BadSummaryCrc { .. }), Ok(op::FOOTER)],
            "{events:?}"
        );
    }

    /// MAGIC + empty Header + a Chunk whose compression-string length disagrees with the
    /// chunk record length. `insert()` used to allocate the declared string length.
    fn file_with_chunk_compression_len(compression_len: u32, record_len: u64) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(MAGIC);
        // Header: empty profile and library strings.
        buf.push(op::HEADER);
        buf.extend_from_slice(&8u64.to_le_bytes());
        buf.extend_from_slice(&0u32.to_le_bytes());
        buf.extend_from_slice(&0u32.to_le_bytes());
        buf.push(op::CHUNK);
        buf.extend_from_slice(&record_len.to_le_bytes());
        buf.extend_from_slice(&0u64.to_le_bytes()); // message_start_time
        buf.extend_from_slice(&0u64.to_le_bytes()); // message_end_time
        buf.extend_from_slice(&0u64.to_le_bytes()); // uncompressed_size
        buf.extend_from_slice(&0u32.to_le_bytes()); // uncompressed_crc
        buf.extend_from_slice(&compression_len.to_le_bytes());
        buf.extend_from_slice(&0u64.to_le_bytes()); // compressed_size if compression_len == 0
        buf
    }

    fn drive_linear_reader(
        bytes: &[u8],
        options: LinearReaderOptions,
    ) -> (Option<McapError>, usize) {
        let mut reader = LinearReader::new_with_options(options);
        let mut cursor = std::io::Cursor::new(bytes);
        let mut max_insert = 0usize;
        let mut iter_count = 0;
        while let Some(event) = reader.next_event() {
            match event {
                Ok(LinearReadEvent::ReadRequest(n)) => {
                    assert!(
                        n <= bytes.len().max(64),
                        "insert requested {n} bytes for a {}-byte file",
                        bytes.len()
                    );
                    max_insert = max_insert.max(n);
                    let written = cursor
                        .read(reader.insert(n))
                        .expect("insert should not fail");
                    reader.notify_read(written);
                }
                Ok(LinearReadEvent::Record { .. }) => {}
                Err(err) => return (Some(err), max_insert),
            }
            iter_count += 1;
            assert!(iter_count < 10000);
        }
        (None, max_insert)
    }

    #[test]
    fn test_chunk_compression_len_exceeds_record() {
        // Record body is the 40-byte minimum header; compression_len claims ~1.69 GiB.
        const HUGE: u32 = 1_687_060_480;
        let bytes = file_with_chunk_compression_len(HUGE, 40);
        let (err, max_insert) = drive_linear_reader(&bytes, LinearReaderOptions::default());
        assert!(max_insert <= 1024);
        assert_matches!(
            err,
            Some(McapError::BadChunkLength {
                header,
                available: 40
            }) if header == 40 + HUGE as u64
        );
    }

    #[test]
    fn test_chunk_compression_len_exceeds_record_length_limit() {
        const HUGE: u32 = 1_687_060_480;
        // Length field agrees with the huge compression string, so BadChunkLength does not
        // fire; the record-length limit must still refuse to allocate it.
        let bytes = file_with_chunk_compression_len(HUGE, 40 + HUGE as u64);
        let (err, max_insert) = drive_linear_reader(
            &bytes,
            LinearReaderOptions::default().with_record_length_limit(1024),
        );
        assert!(max_insert <= 1024);
        assert_matches!(err, Some(McapError::ChunkTooLarge(n)) if n == 40 + HUGE as u64);
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
        none_before, None, LinearReaderOptions::default().with_prevalidate_chunk_crcs(true)
    }

    #[cfg(feature = "zstd")]
    test_chunked_parametrized! {
        zstd_none, Some(Compression::Zstd), LinearReaderOptions::default(),
        zstd_after, Some(Compression::Zstd), LinearReaderOptions::default().with_validate_chunk_crcs(true),
        zstd_before, Some(Compression::Zstd), LinearReaderOptions::default().with_prevalidate_chunk_crcs(true)
    }

    #[cfg(feature = "lz4")]
    test_chunked_parametrized! {
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
            let data = Vec::from_iter(std::iter::repeat(0x20u8).take(1024 * 1024 * 4));
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

    #[cfg(feature = "zstd")]
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
