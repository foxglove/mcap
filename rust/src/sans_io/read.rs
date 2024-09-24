use std::{collections::HashMap, usize};

use super::{
    decompressor::{Decompressor, NoneDecompressor},
    input_buf::InputBuf,
};
use crate::{
    records::{op, ChunkHeader},
    McapError, McapResult, MAGIC,
};
use binrw::BinReaderExt;

use super::{lz4, zstd};
enum CurrentlyReading {
    StartMagic,
    RecordOpcodeLength,
    RecordContent { opcode: u8, record_length: u64 },
    ChunkHeader { record_length: u64 },
    EndMagic,
}

struct ChunkState {
    decompressor: Box<dyn Decompressor>,
    next_read_size: usize,
    compressed_remaining: u64,
    uncompressed_remaining: u64,
    chunk_record_len: u64,
}

enum ReadingFrom {
    File,
    Chunk(ChunkState),
}

pub struct LinearReader {
    currently_reading: CurrentlyReading,
    from: ReadingFrom,
    uncompressed_data_start: usize,
    uncompressed_data_end: usize,
    uncompressed_data: Vec<u8>,
    compressed_data_start: usize,
    compressed_data_end: usize,
    compressed_data: Vec<u8>,
    decompressors: HashMap<String, Box<dyn Decompressor>>,
}

impl LinearReader {
    pub fn new() -> Self {
        LinearReader {
            currently_reading: CurrentlyReading::StartMagic,
            from: ReadingFrom::File,
            uncompressed_data: Vec::new(),
            uncompressed_data_start: 0,
            uncompressed_data_end: 0,
            compressed_data: Vec::new(),
            compressed_data_start: 0,
            compressed_data_end: 0,
            decompressors: HashMap::new(),
        }
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

    pub fn next(&mut self) -> McapResult<ReadState> {
        // keep processing through the file until we need more data or can yield a record.
        loop {
            // check if we have consumed all uncompressed data in the last iteration - if so,
            // reset the buffer.
            if self.uncompressed_data_start == self.uncompressed_data_end {
                self.uncompressed_data.clear();
                self.uncompressed_data_start = 0;
                self.uncompressed_data_end = 0;
            }
            // decompress any compressed data we might have lying around
            if let ReadingFrom::Chunk(chunk_state) = &mut self.from {
                if self.uncompressed_data_end > self.uncompressed_data_start {
                    let src =
                        &self.compressed_data[self.compressed_data_start..self.compressed_data_end];
                    let dst = &mut self.uncompressed_data[self.uncompressed_data_end..];
                    let res = chunk_state.decompressor.decompress(src, dst)?;
                    self.compressed_data_start += res.consumed;
                    self.uncompressed_data_end += res.wrote;
                    chunk_state.next_read_size = res.need;
                    chunk_state.compressed_remaining -= res.consumed as u64;
                    chunk_state.uncompressed_remaining -= res.wrote as u64;
                    // if we have cleared the compressed data buffer, reset it to 0 instead of infinitely growing
                    if self.compressed_data_start == self.compressed_data_end {
                        self.compressed_data.clear();
                        self.compressed_data_start = 0;
                        self.compressed_data_end = 0;
                    }
                }
            }

            match self.currently_reading {
                CurrentlyReading::StartMagic => {
                    let input = match self.consume(MAGIC.len()) {
                        Ok(input) => input,
                        Err(want) => return self.request(want),
                    };
                    if input != MAGIC {
                        return Err(McapError::BadMagic);
                    }
                    self.currently_reading = CurrentlyReading::RecordOpcodeLength;
                }
                CurrentlyReading::EndMagic => {
                    let input = match self.consume(MAGIC.len()) {
                        Ok(input) => input,
                        Err(want) => return self.request(want),
                    };
                    if input != MAGIC {
                        return Err(McapError::BadMagic);
                    }
                    return Ok(ReadState::Finished);
                }
                CurrentlyReading::RecordOpcodeLength => {
                    let input = match self.consume(9) {
                        Ok(input) => input,
                        Err(want) => return self.request(want),
                    };
                    let opcode = input[0];
                    let record_length: u64 = u64::from_le_bytes(input[1..9].try_into().unwrap());
                    if opcode == op::CHUNK {
                        self.currently_reading = CurrentlyReading::ChunkHeader { record_length };
                    } else {
                        self.currently_reading = CurrentlyReading::RecordContent {
                            opcode,
                            record_length,
                        }
                    }
                }
                CurrentlyReading::RecordContent {
                    opcode,
                    record_length,
                } => {
                    let mut other_from = ReadingFrom::File;
                    std::mem::swap(&mut other_from, &mut self.from);
                    if let ReadingFrom::Chunk(state) = other_from {
                        if state.compressed_remaining == 0 {
                            self.return_decompressor(state.decompressor)?;
                        } else {
                            // swap back!
                            let mut new_from = ReadingFrom::Chunk(state);
                            std::mem::swap(&mut new_from, &mut self.from);
                        }
                    }

                    // improvement: the borrow checker doesn't let us just use `consume()` here to
                    // determine the request amount and yield the input buffer at the same time.
                    if self.uncompressed_data_end - self.uncompressed_data_start
                        < (record_length as usize)
                    {
                        return self.request(
                            (record_length as usize)
                                - (self.uncompressed_data_end - self.uncompressed_data_start),
                        );
                    }
                    if opcode == op::FOOTER {
                        self.currently_reading = CurrentlyReading::EndMagic;
                    } else {
                        self.currently_reading = CurrentlyReading::RecordOpcodeLength;
                    }
                    return Ok(ReadState::GetRecord {
                        data: self.consume(record_length as usize).unwrap(),
                        opcode,
                    });
                }
                CurrentlyReading::ChunkHeader { record_length } => {
                    let min_chunk_header_len: usize = 8 + 8 + 8 + 4 + 4 + 8;
                    let have = self.uncompressed_data_end - self.uncompressed_data_start;
                    if have < min_chunk_header_len {
                        return self.request(min_chunk_header_len - have);
                    }
                    let input = &self.uncompressed_data
                        [self.uncompressed_data_start..self.uncompressed_data_end];
                    let compression_string_length =
                        u32::from_le_bytes(input[28..32].try_into().unwrap());
                    let needed_len = min_chunk_header_len + compression_string_length as usize;
                    if needed_len as u64 > record_length {
                        return Err(McapError::RecordTooShort {
                            opcode: op::CHUNK,
                            len: record_length,
                            expected: needed_len as u64,
                        });
                    }
                    if needed_len < have {
                        return self.request(needed_len - have);
                    }
                    let mut cursor = std::io::Cursor::new(self.consume(needed_len).unwrap());
                    let hdr: ChunkHeader = cursor.read_le()?;
                    let decompressor = self.get_decompressor(&hdr.compression)?;
                    self.from = ReadingFrom::Chunk(ChunkState {
                        next_read_size: decompressor.init_size_hint(),
                        decompressor,
                        compressed_remaining: hdr.compressed_size,
                        uncompressed_remaining: hdr.uncompressed_size,
                        chunk_record_len: record_length,
                    })
                }
            }
        }
    }

    // Consume `amount` bytes of the uncompressed input buffer if enough is available. On failure,
    // return the extra amount required as an error value.
    fn consume(&mut self, amount: usize) -> Result<&[u8], usize> {
        let slice_start = self.uncompressed_data_start;
        let slice_end = slice_start + amount;
        if slice_end > self.uncompressed_data_end {
            return Err(slice_end - self.uncompressed_data_end);
        }
        self.uncompressed_data_start = slice_end;
        return Ok(&self.uncompressed_data[slice_start..slice_end]);
    }

    // Return a FillBuff that requests `want` uncompressed bytes from the input file. If reading
    // from a chunk, requests the amount hinted by the decompressor on the previous iteration.
    fn request(&mut self, want: usize) -> McapResult<ReadState> {
        let desired_end = self.uncompressed_data_end + want;
        self.uncompressed_data
            .resize(std::cmp::max(self.uncompressed_data.len(), desired_end), 0);
        return match &self.from {
            ReadingFrom::File => Ok(ReadState::Fill(InputBuf {
                buf: &mut self.uncompressed_data[self.uncompressed_data_end..desired_end],
                written: &mut self.uncompressed_data_end,
            })),
            ReadingFrom::Chunk(chunk_state) => {
                let desired_compressed_end = self.compressed_data_end + chunk_state.next_read_size;
                self.compressed_data.resize(
                    std::cmp::max(self.compressed_data.len(), desired_compressed_end),
                    0,
                );
                Ok(ReadState::Fill(InputBuf {
                    buf: &mut self.compressed_data
                        [self.compressed_data_end..desired_compressed_end],
                    written: &mut self.compressed_data_end,
                }))
            }
        };
    }
}

pub enum ReadState<'a> {
    Fill(super::input_buf::InputBuf<'a>),
    GetRecord { data: &'a [u8], opcode: u8 },
    Finished,
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::{parse_record, McapError};
    use std::collections::BTreeMap;
    use std::io::Read;

    #[test]
    fn maybe_it_works() -> Result<(), McapError> {
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
        loop {
            match reader.next()? {
                ReadState::Finished => break,
                ReadState::Fill(mut into) => {
                    let written = cursor.read(into.buf)?;
                    into.set_filled(written);
                }
                ReadState::GetRecord { data, opcode } => {
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
}
