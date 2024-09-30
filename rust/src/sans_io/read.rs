use std::collections::HashMap;

use super::{
    decompressor::{Decompressor, NoneDecompressor},
    input_buf::InputBuf,
};
use crate::{
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
    RecordOpcodeLength,
    RecordContent { opcode: u8, len: u64 },
    ChunkHeader { len: u64 },
    ValidatingChunkCrc { len: u64, crc: u32 },
    PaddingAfterChunk { len: usize },
    EndMagic,
}
use CurrentlyReading::*;

const DEFAULT_CHUNK_DATA_READ_SIZE: usize = 32 * 1024;

struct ChunkState {
    decompressor: Box<dyn Decompressor>,
    next_read_size: usize,
    compressed_remaining: u64,
    padding_after_compressed_data: usize,
    hasher: Option<crc32fast::Hasher>,
    crc: u32,
}

enum ReadingFrom {
    File,
    Chunk(ChunkState),
}
use ReadingFrom::*;

enum BoundsOrRemainder {
    Bounds((usize, usize)),
    Remainder(usize),
}
use BoundsOrRemainder::*;

#[derive(Debug, Default, Clone)]
pub struct RecordReaderOptions {
    /// If true, the reader will not expect the MCAP magic at the start of the stream.
    pub skip_start_magic: bool,
    /// If true, the reader will not expect the MCAP magic at the end of the stream.
    pub skip_end_magic: bool,
    /// If true, the reader will yield entire chunk records. Otherwise, the reader will decompress
    /// and read into the chunk, yielding the records inside.
    pub emit_chunks: bool,
    pub chunk_crc_validation_strategy: CRCValidationStrategy,
    pub data_section_crc_validation_strategy: CRCValidationStrategy,
}

#[derive(Debug, Default, Clone)]
pub enum CRCValidationStrategy {
    #[default]
    None,
    // Validate CRC of region (data section, chunk or attachment data) before yielding any records.
    // This requires scanning the entire region before yielding the first record, which can be
    // prohibitive in I/O or memory cost.
    BeforeReading,
    // Validate CRC of region (data section, chunk or attachment) after yielding all data from it.
    // If the CRC check fails in this mode, the previously-yielded records may be corrupt, and
    // should be discarded.
    AfterReading,
}

pub struct RecordReader {
    currently_reading: CurrentlyReading,
    from: ReadingFrom,
    uncompressed_data_start: usize,
    uncompressed_data_end: usize,
    uncompressed_data: Vec<u8>,
    compressed_data_start: usize,
    compressed_data_end: usize,
    compressed_data: Vec<u8>,
    data_section_hasher: Option<crc32fast::Hasher>,
    calculated_data_section_crc: Option<u32>,
    decompressors: HashMap<String, Box<dyn Decompressor>>,
    options: RecordReaderOptions,
    at_eof: bool,
    failed: bool,
}

impl RecordReader {
    pub fn new() -> Self {
        Self::new_with_options(RecordReaderOptions::default())
    }

    pub fn new_with_options(options: RecordReaderOptions) -> Self {
        RecordReader {
            currently_reading: StartMagic,
            from: File,
            uncompressed_data: Vec::new(),
            uncompressed_data_start: 0,
            uncompressed_data_end: 0,
            compressed_data: Vec::new(),
            compressed_data_start: 0,
            compressed_data_end: 0,
            data_section_hasher: match options.data_section_crc_validation_strategy {
                CRCValidationStrategy::None => None,
                CRCValidationStrategy::BeforeReading => {
                    panic!("data section pre-validation not supported in linear reader")
                }
                CRCValidationStrategy::AfterReading => Some(crc32fast::Hasher::new()),
            },
            calculated_data_section_crc: None,
            decompressors: HashMap::new(),
            at_eof: false,
            options,
            failed: false,
        }
    }

    pub(crate) fn for_chunk(header: ChunkHeader) -> McapResult<Self> {
        let mut result = Self::new_with_options(RecordReaderOptions {
            skip_end_magic: true,
            skip_start_magic: true,
            chunk_crc_validation_strategy: CRCValidationStrategy::AfterReading,
            data_section_crc_validation_strategy: CRCValidationStrategy::None,
            emit_chunks: false,
        });
        result.currently_reading = RecordOpcodeLength;
        result.from = Chunk(ChunkState {
            decompressor: result.get_decompressor(&header.compression)?,
            hasher: Some(crc32fast::Hasher::new()),
            crc: header.uncompressed_crc,
            next_read_size: DEFAULT_CHUNK_DATA_READ_SIZE,
            compressed_remaining: header.compressed_size,
            padding_after_compressed_data: 0,
        });
        Ok(result)
    }

    fn get_decompressor(&mut self, name: &str) -> McapResult<Box<dyn Decompressor>> {
        if let Some(decompressor) = self.decompressors.remove(name) {
            return Ok(decompressor);
        }
        match name {
            #[cfg(feature = "zstd")]
            "zstd" => Ok(Box::new(zstd::ZstdDecoder::new())),
            #[cfg(feature = "lz4")]
            "lz4" => Ok(Box::new(lz4::Lz4Decoder::new()?)),
            "" => Ok(Box::new(NoneDecompressor {})),
            _ => Err(McapError::UnsupportedCompression(name.into())),
        }
    }
    fn return_decompressor(&mut self, mut decompressor: Box<dyn Decompressor>) -> McapResult<()> {
        decompressor.reset()?;
        self.decompressors
            .insert(decompressor.name().into(), decompressor);
        Ok(())
    }

    pub fn next_action(&mut self) -> Option<McapResult<ReadAction>> {
        self.next_action_inner().transpose()
    }

    fn next_action_inner(&mut self) -> McapResult<Option<ReadAction>> {
        if self.failed {
            return Ok(None);
        }
        // keep processing through the file until we need more data or can yield a record.
        loop {
            // check if we have consumed all uncompressed data in the last iteration - if so,
            // reset the buffer.
            if self.uncompressed_data_start == self.uncompressed_data_end {
                let empty_bytes = self.uncompressed_data.len() - self.uncompressed_data_end;
                self.uncompressed_data.resize(empty_bytes, 0);
                self.uncompressed_data_start = 0;
                self.uncompressed_data_end = 0;
            }
            if self.at_eof {
                if matches!(&self.currently_reading, RecordOpcodeLength)
                    && self.uncompressed_data_start == self.uncompressed_data_end
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
                    if self.options.skip_start_magic {
                        self.currently_reading = RecordOpcodeLength;
                        continue;
                    }
                    let (start, end) = match self.consume(MAGIC.len())? {
                        Bounds(input) => input,
                        Remainder(want) => return self.request(want),
                    };
                    let input = &self.uncompressed_data[start..end];
                    if input != MAGIC {
                        self.failed = true;
                        return Err(McapError::BadMagic);
                    }
                    self.currently_reading = RecordOpcodeLength;
                }
                EndMagic => {
                    if self.options.skip_end_magic {
                        return Ok(None);
                    }
                    let (start, end) = match self.consume(MAGIC.len())? {
                        Bounds(input) => input,
                        Remainder(want) => return self.request(want),
                    };
                    let input = &self.uncompressed_data[start..end];
                    if input != MAGIC {
                        self.failed = true;
                        return Err(McapError::BadMagic);
                    }
                    return Ok(None);
                }
                RecordOpcodeLength => {
                    let (start, end) = match self.consume(9)? {
                        Bounds(input) => input,
                        Remainder(want) => return self.request(want),
                    };
                    let input = &self.uncompressed_data[start..end];
                    let opcode = input[0];
                    let record_length: u64 = u64::from_le_bytes(input[1..9].try_into().unwrap());
                    if opcode == op::DATA_END {
                        self.data_section_hasher = None;
                    }
                    if opcode == op::CHUNK && !self.options.emit_chunks {
                        self.currently_reading =
                            CurrentlyReading::ChunkHeader { len: record_length };
                    } else {
                        self.currently_reading = RecordContent {
                            opcode,
                            len: record_length,
                        }
                    }
                }
                RecordContent { opcode, len } => {
                    let (start, end) = match self.consume(len_as_usize(len)?)? {
                        Bounds(bounds) => bounds,
                        Remainder(remainder) => return self.request(remainder),
                    };
                    if opcode == op::FOOTER {
                        self.currently_reading = EndMagic;
                    } else {
                        if let Some(hasher) = &mut self.data_section_hasher {
                            self.calculated_data_section_crc = Some(hasher.clone().finalize());
                        }
                        self.currently_reading = RecordOpcodeLength;
                    }
                    // need to borrow to check for end of chunk and mutate self after dropping that
                    // borrow.
                    let padding = match &self.from {
                        Chunk(state) => {
                            if state.compressed_remaining == 0
                                && self.uncompressed_data_start == self.uncompressed_data_end
                            {
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
                        let state = match from {
                            Chunk(state) => state,
                            File => panic!(
                                "invariant: padding should only be Some if reading from chunk"
                            ),
                        };
                        self.return_decompressor(state.decompressor)?;
                        if let Some(hasher) = state.hasher {
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
                    if let (op::DATA_END, Some(calculated_crc)) =
                        (opcode, self.calculated_data_section_crc)
                    {
                        if len < 4 {
                            self.failed = true;
                            return Err(McapError::RecordTooShort {
                                opcode: op::FOOTER,
                                len,
                                expected: 4,
                            });
                        }
                        let data = &self.uncompressed_data[start..end];
                        let crc = u32::from_le_bytes(data[..4].try_into().unwrap());
                        if crc != 0 && crc != calculated_crc {
                            self.failed = true;
                            return Err(McapError::BadDataCrc {
                                saved: crc,
                                calculated: calculated_crc,
                            });
                        }
                    }
                    return Ok(Some(ReadAction::GetRecord {
                        data: &self.uncompressed_data[start..end],
                        opcode,
                    }));
                }
                CurrentlyReading::ChunkHeader { len } => {
                    let min_chunk_header_len: usize = 8 + 8 + 8 + 4 + 4 + 8;
                    if len < min_chunk_header_len as u64 {
                        self.failed = true;
                        return Err(McapError::RecordTooShort {
                            opcode: op::CHUNK,
                            len,
                            expected: min_chunk_header_len as u64,
                        });
                    }
                    let (start, end) = match self.load(min_chunk_header_len)? {
                        Bounds(bounds) => bounds,
                        Remainder(remainder) => return self.request(remainder),
                    };
                    let input = &self.uncompressed_data[start..end];
                    let compression_string_length =
                        u32::from_le_bytes(input[28..32].try_into().unwrap());
                    let chunk_header_len =
                        min_chunk_header_len + compression_string_length as usize;
                    if len < chunk_header_len as u64 {
                        self.failed = true;
                        return Err(McapError::RecordTooShort {
                            opcode: op::CHUNK,
                            len,
                            expected: chunk_header_len as u64,
                        });
                    }
                    let (start, end) = match self.consume(chunk_header_len)? {
                        Bounds(buf) => buf,
                        Remainder(need) => return self.request(need),
                    };
                    let mut cursor = std::io::Cursor::new(&self.uncompressed_data[start..end]);
                    let hdr: ChunkHeader = cursor.read_le()?;
                    let decompressor = self.get_decompressor(&hdr.compression)?;
                    let content_len = chunk_header_len as u64 + hdr.compressed_size;
                    if len < content_len {
                        self.failed = true;
                        return Err(McapError::RecordTooShort {
                            opcode: op::CHUNK,
                            len,
                            expected: content_len,
                        });
                    }
                    self.from = Chunk(ChunkState {
                        next_read_size: std::cmp::min(
                            DEFAULT_CHUNK_DATA_READ_SIZE,
                            clamp_to_usize(hdr.compressed_size),
                        ),
                        decompressor,
                        hasher: match self.options.chunk_crc_validation_strategy {
                            CRCValidationStrategy::AfterReading => Some(crc32fast::Hasher::new()),
                            _ => None,
                        },
                        crc: hdr.uncompressed_crc,
                        compressed_remaining: hdr.compressed_size,
                        padding_after_compressed_data: len_as_usize(content_len - len)?,
                    });
                    self.compressed_data.clear();
                    self.compressed_data_end = 0;
                    self.compressed_data_start = 0;
                    if matches!(
                        self.options.chunk_crc_validation_strategy,
                        CRCValidationStrategy::BeforeReading
                    ) && hdr.uncompressed_crc != 0
                    {
                        self.currently_reading = ValidatingChunkCrc {
                            len: hdr.uncompressed_size,
                            crc: hdr.uncompressed_crc,
                        };
                    } else {
                        self.currently_reading = RecordOpcodeLength;
                    }
                }
                ValidatingChunkCrc { len, crc } => {
                    match self.load(len_as_usize(len)?)? {
                        Bounds((start, end)) => {
                            let calculated = crc32fast::hash(&self.uncompressed_data[start..end]);
                            if calculated != crc {
                                self.failed = true;
                                return Err(McapError::BadChunkCrc {
                                    saved: crc,
                                    calculated,
                                });
                            }
                            self.currently_reading = RecordOpcodeLength;
                        }
                        Remainder(remainder) => return self.request(remainder),
                    };
                }
                PaddingAfterChunk { len } => match self.consume(len)? {
                    Bounds(_) => {
                        if let Some(hasher) = &mut self.data_section_hasher {
                            self.calculated_data_section_crc = Some(hasher.clone().finalize());
                        }
                        self.currently_reading = RecordOpcodeLength;
                    }
                    Remainder(need) => return self.request(need),
                },
            }
        }
    }

    // load `amount` bytes into the uncompressed data buffer, returning the remainder if more
    // needs to be loaded.
    fn load(&mut self, amount: usize) -> McapResult<BoundsOrRemainder> {
        let slice_end = self.uncompressed_data_start + amount;
        if let Chunk(chunk_state) = &mut self.from {
            if self.compressed_data_end > self.compressed_data_start {
                self.uncompressed_data.resize(slice_end, 0);
                let src =
                    &self.compressed_data[self.compressed_data_start..self.compressed_data_end];
                let dst = &mut self.uncompressed_data[self.uncompressed_data_end..slice_end];
                let res = chunk_state.decompressor.decompress(src, dst)?;
                self.compressed_data_start += res.consumed;
                let newly_decompressed = &self.uncompressed_data
                    [self.uncompressed_data_end..self.uncompressed_data_end + res.wrote];
                if let Some(hasher) = &mut chunk_state.hasher {
                    hasher.update(newly_decompressed);
                }
                self.uncompressed_data_end += res.wrote;
                chunk_state.compressed_remaining -= res.consumed as u64;
                let next_size_hint = if res.need == 0 {
                    DEFAULT_CHUNK_DATA_READ_SIZE
                } else {
                    res.need
                };
                chunk_state.next_read_size = std::cmp::min(
                    next_size_hint,
                    clamp_to_usize(chunk_state.compressed_remaining),
                );
                // if we have cleared the compressed data buffer, reset it to 0 instead of infinitely growing
                if self.compressed_data_start == self.compressed_data_end {
                    let empty_bytes = self.compressed_data.len() - self.compressed_data_end;
                    self.compressed_data.resize(empty_bytes, 0);
                    self.compressed_data_start = 0;
                    self.compressed_data_end = 0;
                }
            }
        }
        if slice_end <= self.uncompressed_data_end {
            return Ok(Bounds((self.uncompressed_data_start, slice_end)));
        }
        Ok(Remainder(slice_end - self.uncompressed_data_end))
    }

    // Consume `amount` bytes of the uncompressed input buffer if enough is available. On failure,
    // return the extra amount required as an error value.
    fn consume(&mut self, amount: usize) -> McapResult<BoundsOrRemainder> {
        match self.load(amount)? {
            Bounds(bounds) => {
                self.uncompressed_data_start += amount;
                Ok(Bounds(bounds))
            }
            Remainder(remainder) => Ok(Remainder(remainder)),
        }
    }

    // Return an InputBuf that requests `want` uncompressed bytes from the input file. If reading
    // from a chunk, requests the amount hinted by the decompressor on the previous iteration.
    fn request(&mut self, want: usize) -> McapResult<Option<ReadAction>> {
        let desired_end = self.uncompressed_data_end + want;
        self.uncompressed_data
            .resize(std::cmp::max(self.uncompressed_data.len(), desired_end), 0);

        return match &self.from {
            File => Ok(Some(ReadAction::Fill(InputBuf {
                buf: &mut self.uncompressed_data[self.uncompressed_data_end..desired_end],
                total_filled: &mut self.uncompressed_data_end,
                at_eof: &mut self.at_eof,
                data_section_hasher: &mut self.data_section_hasher,
            }))),
            Chunk(chunk_state) => {
                let desired_compressed_end = self.compressed_data_end + chunk_state.next_read_size;
                self.compressed_data.resize(
                    std::cmp::max(self.compressed_data.len(), desired_compressed_end),
                    0,
                );
                Ok(Some(ReadAction::Fill(InputBuf {
                    buf: &mut self.compressed_data
                        [self.compressed_data_end..desired_compressed_end],
                    total_filled: &mut self.compressed_data_end,
                    at_eof: &mut self.at_eof,
                    data_section_hasher: &mut self.data_section_hasher,
                })))
            }
        };
    }
}

impl Default for RecordReader {
    fn default() -> Self {
        Self::new()
    }
}

pub enum ReadAction<'a> {
    Fill(super::input_buf::InputBuf<'a>),
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
        let mut reader = RecordReader::new();
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
    #[test]
    fn test_chunked() -> McapResult<()> {
        for strategy in [
            CRCValidationStrategy::None,
            CRCValidationStrategy::BeforeReading,
            CRCValidationStrategy::AfterReading,
        ] {
            for compression in [Some(Compression::Zstd), Some(Compression::Lz4), None] {
                let mut reader = RecordReader::new_with_options(RecordReaderOptions {
                    skip_end_magic: false,
                    skip_start_magic: false,
                    emit_chunks: false,
                    chunk_crc_validation_strategy: strategy.clone(),
                    data_section_crc_validation_strategy: CRCValidationStrategy::None,
                });
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
            }
        }
        Ok(())
    }
    #[test]
    fn test_no_magic() -> McapResult<()> {
        for options in [
            RecordReaderOptions {
                skip_start_magic: false,
                skip_end_magic: true,
                emit_chunks: false,
                chunk_crc_validation_strategy: CRCValidationStrategy::None,
                data_section_crc_validation_strategy: CRCValidationStrategy::None,
            },
            RecordReaderOptions {
                skip_start_magic: true,
                skip_end_magic: false,
                emit_chunks: false,
                chunk_crc_validation_strategy: CRCValidationStrategy::None,
                data_section_crc_validation_strategy: CRCValidationStrategy::None,
            },
        ] {
            let mcap = basic_chunked_file(None)?;
            let input = if options.skip_start_magic {
                &mcap[8..]
            } else if options.skip_end_magic {
                &mcap[..mcap.len() - 8]
            } else {
                panic!("options should either skip start or end magic")
            };
            let mut reader = RecordReader::new_with_options(options);
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
        let mut reader = RecordReader::new_with_options(RecordReaderOptions {
            skip_end_magic: false,
            skip_start_magic: false,
            emit_chunks: true,
            chunk_crc_validation_strategy: CRCValidationStrategy::None,
            data_section_crc_validation_strategy: CRCValidationStrategy::None,
        });
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
