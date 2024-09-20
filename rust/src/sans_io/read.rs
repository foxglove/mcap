use std::{collections::HashMap, hash::Hash};

use crate::{
    records::{op, AttachmentHeader, ChunkHeader},
    McapResult, MAGIC,
};
pub struct NeedData {
    reader: InternalReader,
    pub length: u64,
    pub offset: u64,
}

pub struct RecordReady {
    reader: InternalReader,
    opcode: u8,
}

pub struct ReadyWithAttachment {
    reader: InternalReader,
    header: AttachmentHeader,
}

use binrw::BinReaderExt;

use super::{lz4, zstd};
pub enum LinearReadState {
    NeedData(NeedData),
    RecordReady(RecordReady),
    AttachmentReady(ReadyWithAttachment),
    Finished,
}

enum CurrentlyReading {
    StartMagic,
    RecordOpcodeLength,
    RecordContent { opcode: u8, record_length: u64 },
    Chunkheader { record_length: u64 },
    EndMagic,
}

enum ReadingFrom {
    File,
    Chunk {
        decompressor: Box<dyn Decompressor>,
        compressed_remaining: u64,
        uncompressed_remaining: u64,
        chunk_record_len: u64,
    },
}

struct InternalReader {
    currently_reading: CurrentlyReading,
    from: ReadingFrom,
    buf: Vec<u8>,
    decompressors: HashMap<String, Box<dyn Decompressor>>,
}

impl InternalReader {
    fn switch_state(&mut self, cr: CurrentlyReading) {
        self.buf.clear();
        self.currently_reading = cr;
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
            _ => Err(crate::McapError::UnsupportedCompression(name.into())),
        }
    }
    fn return_decompressor(
        &mut self,
        name: &str,
        mut decompressor: Box<dyn Decompressor>,
    ) -> McapResult<()> {
        decompressor.reset()?;
        self.decompressors.insert(name.into(), decompressor);
        Ok(())
    }
}

impl RecordReady {
    pub fn get_record(self, into: &mut Vec<u8>) -> McapResult<(u8, LinearReadState)> {}
}

impl NeedData {
    fn request_more(self, consumed: u64, need: u64) -> LinearReadState {
        LinearReadState::NeedData(Self {
            reader: self.reader,
            offset: consumed + self.offset,
            length: need,
        })
    }
    pub fn load_input(mut self, input: &[u8], input_offset: u64) -> McapResult<LinearReadState> {
        let needed_uncompressed = match self.reader.currently_reading {
            CurrentlyReading::StartMagic => 8,
            CurrentlyReading::EndMagic => 8,
            CurrentlyReading::RecordOpcodeLength => 9,
            CurrentlyReading::RecordContent {
                opcode: _,
                record_length,
            } => record_length,
            CurrentlyReading::Chunkheader { record_length } => {
                let len_before_compression_string: u64 = 8 + 8 + 8 + 4 + 4;
                if (self.reader.buf.len() as u64) < len_before_compression_string {
                    len_before_compression_string
                } else {
                    let compression_string_length =
                        u32::from_le_bytes(self.reader.buf[28..32].try_into().unwrap()) as u64;
                    len_before_compression_string + compression_string_length + 8
                }
            }
        };
        // check overlap
        let needed_data_start = self.offset;
        if needed_data_start < input_offset {
            return Err(crate::McapError::UnexpectedEof);
        }
        let slice_start = needed_data_start - input_offset;
        let slice_end = match self.reader.from {
            ReadingFrom::File => slice_start + needed_uncompressed,
            ReadingFrom::Chunk { uncompressed_remaining} => uncompressed_remaining,
        }
        if slice_end > input.len() as u64 {
            return Err(McapError::UnexpectedEof);
        }
        let new_data = input[]

        match self.reader.currently_reading {
            CurrentlyReading::StartMagic => {
                if self.reader.buf.len() < MAGIC.len() {
                    return Ok(
                        self.request_more(consumed, (MAGIC.len() - self.reader.buf.len()) as u64)
                    );
                }
                if &self.reader.buf[..MAGIC.len()] != MAGIC {
                    return Err(crate::McapError::BadMagic);
                }
                self.reader
                    .switch_state(CurrentlyReading::RecordOpcodeLength);
                Ok(self.request_more(consumed, 9))
            }
            CurrentlyReading::EndMagic => {
                if self.reader.buf.len() < MAGIC.len() {
                    return Ok(
                        self.request_more(consumed, (MAGIC.len() - self.reader.buf.len()) as u64)
                    );
                }
                if &self.reader.buf[..MAGIC.len()] != MAGIC {
                    return Err(crate::McapError::BadMagic);
                }
                Ok(LinearReadState::Finished)
            }
            CurrentlyReading::RecordOpcodeLength => {
                if self.reader.buf.len() < 9 {
                    return Ok(self.request_more(consumed, 9 - self.reader.buf.len() as u64));
                }
                let opcode = self.reader.buf[0];
                let length = u64::from_le_bytes(self.reader.buf[1..9].try_into().unwrap());
                if opcode == op::CHUNK {
                    self.reader.switch_state(CurrentlyReading::Chunkheader {
                        record_length: length,
                    });
                    Ok(self.request_more(consumed, 8 + 8 + 8 + 4 + 4))
                } else {
                    self.reader.switch_state(CurrentlyReading::RecordContent {
                        opcode,
                        record_length: length,
                    });
                    Ok(self.request_more(consumed, length.try_into().unwrap()))
                }
            }
            CurrentlyReading::RecordContent {
                opcode,
                record_length: length,
            } => {
                if opcode == op::FOOTER {
                    self.reader.currently_reading = CurrentlyReading::EndMagic;
                } else {
                    self.reader.currently_reading = CurrentlyReading::RecordOpcodeLength;
                }
                if self.reader.buf.len() >= length as usize {
                    self.reader.buf.truncate(length as usize);
                    Ok(LinearReadState::RecordReady(RecordReady {
                        opcode,
                        reader: self.reader,
                    }))
                } else {
                    Ok(self.request_more(consumed, length as u64 - self.reader.buf.len() as u64))
                }
            }
            CurrentlyReading::Chunkheader {
                record_length: length,
            } => {
                let have = self.reader.buf.len();
                let chunk_header_size_up_to_compression_string = 8 + 8 + 8 + 4 + 4;
                if have < chunk_header_size_up_to_compression_string {
                    return Ok(self.request_more(
                        consumed,
                        (chunk_header_size_up_to_compression_string - have) as u64,
                    ));
                }
                let chunk_header_string_size = u32::from_le_bytes(
                    self.reader.buf[chunk_header_size_up_to_compression_string - 4
                        ..chunk_header_size_up_to_compression_string]
                        .try_into()
                        .unwrap(),
                );
                let total_chunk_header_size = chunk_header_size_up_to_compression_string
                    + (chunk_header_string_size as usize)
                    + 8;
                if have < total_chunk_header_size {
                    return Ok(self.request_more(consumed, (total_chunk_header_size - have) as u64));
                }
                let mut cursor = std::io::Cursor::new(&self.reader.buf[..total_chunk_header_size]);
                let chunk_header: ChunkHeader = cursor.read_le()?;
                self.reader
                    .switch_state(CurrentlyReading::ChunkRecordOpcodeLength {
                        decompressor: self.reader.get_decompressor(&chunk_header.compression)?,
                        compressed_remaining: chunk_header.compressed_size,
                        uncompressed_remaining: chunk_header.uncompressed_size,
                        next_file_record_start: self.offset as u64 + length,
                    });
                return Ok(LinearReadState::NeedData(self));
            }
            _ => panic!("unfinished"),
        }
    }
}

pub struct DecompressResult {
    pub consumed: usize,
    pub wrote: usize,
    pub need: usize,
}

pub trait Decompressor {
    fn decompress(&mut self, src: &[u8], dst: &mut [u8]) -> McapResult<DecompressResult>;
    fn reset(&mut self) -> McapResult<()>;
    fn next_size_hint(&self, u64) -> usize;
}

struct NoneDecompressor {}

impl Decompressor for NoneDecompressor {
    fn decompress(&mut self, src: &[u8], dst: &mut [u8]) -> McapResult<DecompressResult> {
        let len = std::cmp::min(src.len(), dst.len());
        dst[..len].copy_from_slice(&src[..len]);
        return Ok(DecompressResult {
            consumed: len,
            wrote: len,
            need: (dst.len() - len),
        });
    }
    fn reset(&mut self) -> McapResult<()> {
        Ok(())
    }
    fn next_size_hint(&self, ) -> usize {
        return 0;
    }
}

fn new_reader() -> LinearReadState {
    let reader = InternalReader {
        currently_reading: CurrentlyReading::StartMagic,
        buf: Vec::new(),
        decompressors: HashMap::new(),
    };
    LinearReadState::NeedData(NeedData {
        reader,
        offset: 0,
        length: 8,
    })
}
