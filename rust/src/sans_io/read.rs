use std::collections::HashMap;

use super::decompressor::Decompressor;
use crate::{
    parse_record,
    records::{op, ChunkHeader},
    McapError, McapResult, MAGIC,
};
use binrw::BinReaderExt;

#[cfg(feature = "lz4")]
use super::lz4;

#[cfg(feature = "zstd")]
use super::zstd;

enum CurrentlyReading {
    StartMagic,
    Record,
    ChunkHeader { len: u64 },
    ValidatingChunkCrc { len: u64, crc: u32 },
    PaddingAfterChunk { len: usize },
    EndMagic,
}
use CurrentlyReading::*;

const DEFAULT_CHUNK_DATA_READ_SIZE: usize = 32 * 1024;

struct ChunkState {
    decompressor: Option<Box<dyn Decompressor>>,
    compressed_remaining: u64,
    padding_after_compressed_data: usize,
    hasher: Option<crc32fast::Hasher>,
    crc: u32,
}

#[derive(Default)]
struct RWBuf {
    data: Vec<u8>,
    start: usize,
    end: usize,
}

impl RWBuf {
    // returns a mutable view of the un-written part of the buffer.
    fn tail<'a>(&'a mut self) -> &'a mut [u8] {
        &mut self.data[self.end..]
    }

    // Marks some bytes of the un-written part as written.
    fn mark_written(&mut self, written: usize) {
        self.end += written;
    }

    // Marks some bytes of the un-read part as read.
    fn mark_read(&mut self, read: usize) {
        self.start += read;
    }

    // returns the length of the unread section.
    fn len(&self) -> usize {
        self.end - self.start
    }

    // returns an immutable view of the entire unread section.
    fn unread<'a>(&'a self) -> &'a [u8] {
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
    fn view<'a>(&'a self, span: (usize, usize)) -> &'a [u8] {
        let (start, end) = span;
        &self.data[start..end]
    }

    // returns a mutable view of the un-written part of the buffer, resizing as needed to ensure
    // N bytes are available to write into.
    fn tail_with_size<'a>(&'a mut self, n: usize) -> &'a mut [u8] {
        let desired_end = self.end + n;
        self.data.resize(desired_end, 0);
        self.tail()
    }

    // resets the RWBuf.
    fn reset(&mut self) {
        self.data.clear();
        self.start = 0;
        self.end = 0;
    }
}

enum ReadingFrom {
    File,
    Chunk(ChunkState),
}
use ReadingFrom::*;

enum SpanOrRemainder {
    Span((usize, usize)),
    Remainder(usize),
}
use SpanOrRemainder::*;

#[derive(Debug, Default, Clone)]
pub struct LinearReaderOptions {
    /// If true, the reader will not expect the MCAP magic at the start of the stream.
    pub skip_start_magic: bool,
    /// If true, the reader will not expect the MCAP magic at the end of the stream.
    pub skip_end_magic: bool,
    /// If true, the reader will yield entire chunk records. Otherwise, the reader will decompress
    /// and read into the chunk, yielding the records inside.
    pub emit_chunks: bool,
    // whether to validate chunk CRCs. Ignored if `prevalidate_chunk_crcs` is true.
    pub validate_chunk_crcs: bool,
    // whether to validate the chunk CRC before yielding any records from the chunk.
    pub prevalidate_chunk_crcs: bool,
    // Whether to validate the data section CRC.
    pub validate_data_section_crc: bool,
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
        let mut res = Self {
            prevalidate_chunk_crcs,
            ..self
        };
        if res.prevalidate_chunk_crcs {
            res.validate_chunk_crcs = true;
        }
        res
    }
    pub fn with_validate_data_section_crc(self, validate_data_section_crc: bool) -> Self {
        Self {
            validate_data_section_crc,
            ..self
        }
    }
}

/// A mutable view that allows the user to write new MCAP data into the [`LinearReader`]. The user
/// is expected to copy up to `self.buf.len()` bytes into `self.buf`, then call `set_filled(usize)`
/// to notify the reader of how many bytes were successfully read.
pub struct InputBuf<'a> {
    pub buf: &'a mut [u8],
    last_write: &'a mut Option<usize>,
}

impl<'a> InputBuf<'a> {
    /// Notify the reader that `written` new bytes are available. Only call this method after
    /// copying data into [`self.buf`].
    pub fn set_filled(&'a mut self, written: usize) {
        *self.last_write = Some(written);
    }
    /// A convenience method to copy from the user's slice of MCAP data.
    pub fn copy_from(&'a mut self, other: &[u8]) -> usize {
        let len = std::cmp::min(self.buf.len(), other.len());
        let src = &other[..len];
        let dst = &mut self.buf[..len];
        dst.copy_from_slice(src);
        self.set_filled(len);
        len
    }
}

/// Reads an MCAP file from start to end, yielding raw records by opcode and data buffer. This struct
/// does not perform any I/O on its own, instead it yields slices to the caller and allows them to
/// use their own I/O primitives.
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
///     let mut record_buf: Vec<u8> = Vec::new();
///     let mut reader = mcap::sans_io::read::LinearReader::new();
///     while let Some(action) = reader.next_action() {
///         match action? {
///             ReadAction::Fill(mut into) => {
///                 let n = file.read(into.buf).await?;
///                 into.set_filled(n);
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
///     let mut record_buf: Vec<u8> = Vec::new();
///     let mut reader = mcap::sans_io::read::LinearReader::new();
///     while let Some(action) = reader.next_action() {
///         match action? {
///             ReadAction::Fill(mut into) => {
///                 let n = file.read(into.buf)?;
///                 into.set_filled(n);
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
    from: ReadingFrom,
    uncompressed: RWBuf,
    compressed: RWBuf,
    data_section_hasher: Option<crc32fast::Hasher>,
    calculated_data_section_crc: Option<u32>,
    decompressors: HashMap<String, Box<dyn Decompressor>>,
    options: LinearReaderOptions,
    at_eof: bool,
    last_write: Option<usize>,
    failed: bool,
}

impl LinearReader {
    pub fn new() -> Self {
        Self::new_with_options(LinearReaderOptions::default())
    }

    pub fn new_with_options(options: LinearReaderOptions) -> Self {
        LinearReader {
            currently_reading: if options.skip_start_magic {
                Record
            } else {
                StartMagic
            },
            from: File,
            uncompressed: RWBuf::default(),
            compressed: RWBuf::default(),
            data_section_hasher: if options.validate_data_section_crc {
                None
            } else {
                Some(crc32fast::Hasher::new())
            },
            calculated_data_section_crc: None,
            decompressors: HashMap::new(),
            last_write: None,
            at_eof: false,
            options,
            failed: false,
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
        result.currently_reading = Record;
        result.from = Chunk(ChunkState {
            decompressor: result.get_decompressor(&header.compression)?,
            hasher: Some(crc32fast::Hasher::new()),
            crc: header.uncompressed_crc,
            compressed_remaining: header.compressed_size,
            padding_after_compressed_data: 0,
        });
        Ok(result)
    }

    fn get_decompressor(&mut self, name: &str) -> McapResult<Option<Box<dyn Decompressor>>> {
        if let Some(decompressor) = self.decompressors.remove(name) {
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
    fn return_decompressor(&mut self, mut decompressor: Box<dyn Decompressor>) -> McapResult<()> {
        decompressor.reset()?;
        self.decompressors
            .insert(decompressor.name().into(), decompressor);
        Ok(())
    }

    /// Yields the next action the caller should take to progress through the file.
    pub fn next_action(&mut self) -> Option<McapResult<ReadAction>> {
        self.next_action_inner().transpose()
    }

    fn next_action_inner(&mut self) -> McapResult<Option<ReadAction>> {
        if self.failed {
            return Ok(None);
        }

        // process any newly-written data since the last next_action() call.
        if let Some(written) = self.last_write.take() {
            if written == 0 {
                self.at_eof = true;
            }

            let tail = match &self.from {
                File => self.uncompressed.tail(),
                Chunk(state) => match &state.decompressor {
                    Some(_) => self.compressed.tail(),
                    None => self.uncompressed.tail(),
                },
            };
            let written_region = &tail[..written];
            if let Some(hasher) = self.data_section_hasher.as_mut() {
                hasher.update(written_region);
            }
            // update end pointer, and update
            match &mut self.from {
                File => self.uncompressed.mark_written(written),
                Chunk(state) => match state.decompressor {
                    Some(_) => self.compressed.mark_written(written),
                    None => {
                        // for the special case of reading from an uncompressed chunk, we update
                        // the chunk CRC and compressed remaining here instead of after
                        // decompression.
                        if let Some(hasher) = state.hasher.as_mut() {
                            hasher.update(written_region);
                        }
                        self.uncompressed.mark_written(written);
                        state.compressed_remaining -= written as u64;
                    }
                },
            }
        }
        // keep processing through the data we have until we need more data or can yield a record.
        loop {
            // check if we have consume all uncompressed data in the last iteration - if so,
            // reset the buffer.
            if self.uncompressed.len() == 0 {
                self.uncompressed.reset();
            }
            // If there's no more data available, stop iterating.
            if self.at_eof {
                if matches!(&self.currently_reading, Record)
                    && self.uncompressed.len() == 0
                    && self.options.skip_end_magic
                {
                    return Ok(None);
                } else {
                    self.failed = true;
                    return Err(McapError::UnexpectedEof);
                }
            }

            match self.currently_reading {
                StartMagic => {
                    let span = match self.consume(MAGIC.len())? {
                        Span(span) => span,
                        Remainder(want) => return self.request(want),
                    };
                    let input = self.uncompressed.view(span);
                    if input != MAGIC {
                        self.failed = true;
                        return Err(McapError::BadMagic);
                    }
                    self.currently_reading = Record;
                }
                EndMagic => {
                    if self.options.skip_end_magic {
                        return Ok(None);
                    }
                    let span = match self.consume(MAGIC.len())? {
                        Span(span) => span,
                        Remainder(want) => return self.request(want),
                    };
                    let input = self.uncompressed.view(span);
                    if input != MAGIC {
                        self.failed = true;
                        return Err(McapError::BadMagic);
                    }
                    return Ok(None);
                }
                Record => {
                    // need one byte for opcode, then 8 bytes for record length.
                    let span = match self.load(9)? {
                        Span(span) => span,
                        Remainder(want) => return self.request(want),
                    };
                    let input = self.uncompressed.view(span);
                    let opcode = input[0];
                    let record_length: u64 = u64::from_le_bytes(input[1..].try_into().unwrap());
                    if opcode == op::CHUNK && !self.options.emit_chunks {
                        match self.consume(9)? {
                            Span(_) => {}
                            Remainder(_) => panic!("there should be 9 bytes available"),
                        };
                        self.currently_reading =
                            CurrentlyReading::ChunkHeader { len: record_length };
                        continue;
                    }
                    // get the rest of the record now.
                    let span = match self.consume(9 + len_as_usize(record_length)?)? {
                        Span(span) => span,
                        Remainder(want) => return self.request(want),
                    };
                    // some opcodes trigger state changes.
                    match opcode {
                        // A footer implies no more records in the MCAP.
                        op::FOOTER => self.currently_reading = EndMagic,
                        // Data end implies end of data section - validate the CRC if present.
                        op::DATA_END => {
                            if let Some(calculated) = self.calculated_data_section_crc {
                                let record_data = &self.uncompressed.view(span)[9..];
                                match parse_record(opcode, record_data)? {
                                    crate::records::Record::DataEnd(end) => {
                                        if end.data_section_crc != 0
                                            && end.data_section_crc != calculated
                                        {
                                            return Err(McapError::BadDataCrc {
                                                saved: end.data_section_crc,
                                                calculated,
                                            });
                                        }
                                    }
                                    _ => unreachable!("should not recieve any other record type"),
                                }
                            }
                            self.data_section_hasher = None;
                        }
                        _ => {}
                    };
                    if let Some(hasher) = &mut self.data_section_hasher {
                        self.calculated_data_section_crc = Some(hasher.clone().finalize());
                    }

                    // If this is the last record in the chunk, we need to do a little work before
                    // moving on to the next record. We immutably borrow self.from to check, then
                    // make sure to drop the borrow before taking action.
                    let padding = match &self.from {
                        Chunk(state) => {
                            if state.compressed_remaining == 0 && self.uncompressed.len() == 0 {
                                Some(state.padding_after_compressed_data)
                            } else {
                                None
                            }
                        }
                        File => None,
                    };
                    if let Some(padding) = padding {
                        let mut from = File;
                        std::mem::swap(&mut from, &mut self.from);
                        let state = match &mut from {
                            Chunk(state) => state,
                            File => panic!(
                                "invariant: padding should only be Some if reading from chunk"
                            ),
                        };
                        if let Some(decompressor) = state.decompressor.take() {
                            self.return_decompressor(decompressor)?
                        }
                        if let Some(hasher) = state.hasher.take() {
                            let calculated = hasher.finalize();
                            if state.crc != 0 && calculated != state.crc {
                                self.failed = true;
                                return Err(McapError::BadChunkCrc {
                                    saved: state.crc,
                                    calculated,
                                });
                            }
                        }
                        self.currently_reading = PaddingAfterChunk { len: padding }
                    }
                    return Ok(Some(ReadAction::GetRecord {
                        data: &self.uncompressed.view(span)[9..],
                        opcode,
                    }));
                }
                CurrentlyReading::ChunkHeader { len } => {
                    // Need to read _only_ the chunk header, which is of variable size.
                    // We load the minimum chunk header size, which is always enough to check
                    // the length of the compression string. With the compression string length,
                    // the true length of the chunk header is known, so we read that.
                    let min_chunk_header_len: usize = 8 + 8 + 8 + 4 + 4 + 8;
                    if len < min_chunk_header_len as u64 {
                        self.failed = true;
                        return Err(McapError::RecordTooShort {
                            opcode: op::CHUNK,
                            len,
                            expected: min_chunk_header_len as u64,
                        });
                    }
                    let span = match self.load(min_chunk_header_len)? {
                        Span(span) => span,
                        Remainder(remainder) => return self.request(remainder),
                    };
                    let input = self.uncompressed.view(span);
                    let compression_string_length =
                        u32::from_le_bytes(input[28..32].try_into().unwrap());
                    let true_chunk_header_len =
                        min_chunk_header_len + compression_string_length as usize;
                    if len < true_chunk_header_len as u64 {
                        self.failed = true;
                        return Err(McapError::RecordTooShort {
                            opcode: op::CHUNK,
                            len,
                            expected: true_chunk_header_len as u64,
                        });
                    }
                    // now we can load the full chunk header bytes.
                    let span = match self.consume(true_chunk_header_len)? {
                        Span(span) => span,
                        Remainder(need) => return self.request(need),
                    };
                    let mut cursor = std::io::Cursor::new(self.uncompressed.view(span));
                    let hdr: ChunkHeader = cursor.read_le()?;

                    let content_len = true_chunk_header_len as u64 + hdr.compressed_size;
                    if len < content_len {
                        self.failed = true;
                        return Err(McapError::RecordTooShort {
                            opcode: op::CHUNK,
                            len,
                            expected: content_len,
                        });
                    }
                    // switch to reading from the chunk data.
                    self.from = Chunk(ChunkState {
                        decompressor: self.get_decompressor(&hdr.compression)?,
                        hasher: if self.options.validate_chunk_crcs
                            && !self.options.prevalidate_chunk_crcs
                        {
                            Some(crc32fast::Hasher::new())
                        } else {
                            None
                        },
                        crc: hdr.uncompressed_crc,
                        compressed_remaining: hdr.compressed_size,
                        padding_after_compressed_data: len_as_usize(content_len - len)?,
                    });
                    self.compressed.reset();
                    // If we need to validate the CRC of all the chunk data before yielding
                    // any records, do that first. Otherwise, go ahead and yield the first record.
                    if self.options.prevalidate_chunk_crcs && hdr.uncompressed_crc != 0 {
                        self.currently_reading = ValidatingChunkCrc {
                            len: hdr.uncompressed_size,
                            crc: hdr.uncompressed_crc,
                        };
                    } else {
                        self.currently_reading = Record;
                    }
                }
                ValidatingChunkCrc { len, crc } => {
                    match self.load(len_as_usize(len)?)? {
                        Span(span) => {
                            let calculated = crc32fast::hash(&self.uncompressed.view(span));
                            if calculated != crc {
                                self.failed = true;
                                return Err(McapError::BadChunkCrc {
                                    saved: crc,
                                    calculated,
                                });
                            }
                            self.currently_reading = Record;
                        }
                        Remainder(remainder) => return self.request(remainder),
                    };
                }
                // A chunk record can have more bytes after the `data` member, which we need
                // to discard.
                PaddingAfterChunk { len } => match self.consume(len)? {
                    Span(_) => {
                        if let Some(hasher) = &mut self.data_section_hasher {
                            self.calculated_data_section_crc = Some(hasher.clone().finalize());
                        }
                        self.currently_reading = Record;
                    }
                    Remainder(need) => return self.request(need),
                },
            }
        }
    }

    // load `amount` bytes into the uncompressed data buffer, returning the remainder if more
    // needs to be loaded.
    fn load(&mut self, amount: usize) -> McapResult<SpanOrRemainder> {
        let want = match self.uncompressed.span(amount) {
            Span(b) => return Ok(Span(b)),
            Remainder(want) => want,
        };
        match &mut self.from {
            File => return Ok(Remainder(want)),
            Chunk(chunk_state) => {
                let decompressor = match &mut chunk_state.decompressor {
                    None => return Ok(Remainder(want)),
                    Some(dec) => dec,
                };
                self.uncompressed.data.resize(
                    std::cmp::max(self.uncompressed.data.len(), self.uncompressed.end + want),
                    0,
                );
                let src = self.compressed.unread();
                if src.len() == 0 {
                    return Ok(Remainder(std::cmp::min(
                        clamp_to_usize(chunk_state.compressed_remaining),
                        DEFAULT_CHUNK_DATA_READ_SIZE,
                    )));
                }
                let dst = self.uncompressed.tail();
                let res = decompressor.decompress(src, dst)?;
                let newly_decompressed = &self.uncompressed.tail()[..res.wrote];
                if let Some(hasher) = &mut chunk_state.hasher {
                    hasher.update(newly_decompressed);
                }
                self.uncompressed.mark_written(res.wrote);
                self.compressed.mark_read(res.consumed);
                chunk_state.compressed_remaining -= res.consumed as u64;
                if self.compressed.len() == 0 {
                    self.compressed.reset();
                }
                return match self.uncompressed.span(amount) {
                    Span(b) => Ok(Span(b)),
                    Remainder(_) => Ok(Remainder(std::cmp::min(
                        clamp_to_usize(chunk_state.compressed_remaining),
                        res.need,
                    ))),
                };
            }
        };
    }

    // Consume `amount` bytes of the uncompressed input buffer if enough is available. On failure,
    // return the extra amount required as an error value.
    fn consume(&mut self, amount: usize) -> McapResult<SpanOrRemainder> {
        match self.load(amount)? {
            Span(span) => {
                self.uncompressed.mark_read(amount);
                Ok(Span(span))
            }
            Remainder(remainder) => Ok(Remainder(remainder)),
        }
    }

    // Return an InputBuf that requests `want` bytes from the input file. If reading
    // from a chunk, reads into the compressed buffer, otherwise reads into the uncompressed buffer.
    fn request(&mut self, want: usize) -> McapResult<Option<ReadAction>> {
        return match &self.from {
            File => Ok(Some(ReadAction::Fill(InputBuf {
                buf: self.uncompressed.tail_with_size(want),
                last_write: &mut self.last_write,
            }))),
            Chunk(state) => {
                if let None = state.decompressor {
                    Ok(Some(ReadAction::Fill(InputBuf {
                        buf: self.uncompressed.tail_with_size(want),
                        last_write: &mut self.last_write,
                    })))
                } else {
                    Ok(Some(ReadAction::Fill(InputBuf {
                        buf: self.compressed.tail_with_size(want),
                        last_write: &mut self.last_write,
                    })))
                }
            }
        };
    }
}

impl Default for LinearReader {
    fn default() -> Self {
        Self::new()
    }
}

pub enum ReadAction<'a> {
    Fill(InputBuf<'a>),
    GetRecord { data: &'a [u8], opcode: u8 },
}

fn len_as_usize(len: u64) -> McapResult<usize> {
    len.try_into().map_err(|_| McapError::TooLong(len))
}

fn clamp_to_usize(len: u64) -> usize {
    len.try_into().unwrap_or(usize::MAX)
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
                ReadAction::Fill(mut into) => {
                    let written = cursor.read(into.buf)?;
                    into.set_filled(written);
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

    use paste::paste;

    macro_rules! test_chunk_parametrized {
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

    test_chunk_parametrized! {
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
                ReadAction::Fill(mut into) => {
                    let written = cursor.read(into.buf)?;
                    into.set_filled(written);
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
                    ReadAction::Fill(mut into) => {
                        let written = cursor.read(into.buf)?;
                        into.set_filled(written);
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
                ReadAction::Fill(mut into) => {
                    let written = cursor.read(into.buf)?;
                    into.set_filled(written);
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
