use std::pin::{pin, Pin};
use std::task::{Context, Poll};

#[cfg(feature = "zstd")]
use async_compression::tokio::bufread::ZstdDecoder;
use byteorder::ByteOrder;
use tokio::io::{AsyncRead, AsyncReadExt, ReadBuf, Take};

#[cfg(feature = "lz4")]
use crate::tokio::lz4::Lz4Decoder;
use crate::{records, McapError, McapResult, MAGIC};

enum ReaderState<R> {
    Base(R),
    UncompressedChunk(Take<R>),
    #[cfg(feature = "zstd")]
    ZstdChunk(ZstdDecoder<tokio::io::BufReader<Take<R>>>),
    #[cfg(feature = "lz4")]
    Lz4Chunk(Lz4Decoder<Take<R>>),
    Empty,
}

impl<R> AsyncRead for ReaderState<R>
where
    R: AsyncRead + std::marker::Unpin,
{
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            ReaderState::Base(r) => pin!(r).poll_read(cx, buf),
            ReaderState::UncompressedChunk(r) => pin!(r).poll_read(cx, buf),
            #[cfg(feature = "zstd")]
            ReaderState::ZstdChunk(r) => pin!(r).poll_read(cx, buf),
            #[cfg(feature = "lz4")]
            ReaderState::Lz4Chunk(r) => pin!(r).poll_read(cx, buf),
            ReaderState::Empty => {
                panic!("invariant: reader is only set to empty while swapping with another valid variant")
            }
        }
    }
}
impl<R> ReaderState<R>
where
    R: AsyncRead,
{
    pub fn into_inner(self) -> McapResult<R> {
        match self {
            ReaderState::Base(reader) => Ok(reader),
            ReaderState::UncompressedChunk(take) => Ok(take.into_inner()),
            #[cfg(feature = "zstd")]
            ReaderState::ZstdChunk(decoder) => Ok(decoder.into_inner().into_inner().into_inner()),
            #[cfg(feature = "lz4")]
            ReaderState::Lz4Chunk(decoder) => {
                let (output, result) = decoder.finish();
                result?;
                Ok(output.into_inner())
            }
            ReaderState::Empty => {
                panic!("invariant: reader is only set to empty while swapping with another valid variant")
            }
        }
    }
}
/// Reads an MCAP file record-by-record, writing the raw record data into a caller-provided Vec.
pub struct RecordReader<R> {
    reader: ReaderState<R>,
    options: RecordReaderOptions,
    start_magic_seen: bool,
    footer_seen: bool,
    to_discard_after_chunk: usize,
    scratch: Box<[u8]>,
}

#[derive(Default, Clone)]
pub struct RecordReaderOptions {
    /// If true, the reader will not expect the MCAP magic at the start of the stream.
    skip_start_magic: bool,
    /// If true, the reader will not expect the MCAP magic at the end of the stream.
    skip_end_magic: bool,
    // If true, the reader will yield entire chunk records. Otherwise, the reader will decompress
    // and read into the chunk, yielding the records inside.
    emit_chunks: bool,
}

enum Cmd {
    YieldRecord(u8),
    EnterChunk {
        header: records::ChunkHeader,
        len: u64,
    },
    ExitChunk,
    Stop,
}

impl<R> RecordReader<R>
where
    R: AsyncRead + std::marker::Unpin,
{
    pub fn new(reader: R) -> Self {
        Self::new_with_options(reader, &RecordReaderOptions::default())
    }

    pub fn new_with_options(reader: R, options: &RecordReaderOptions) -> Self {
        Self {
            reader: ReaderState::Base(reader),
            options: options.clone(),
            start_magic_seen: false,
            footer_seen: false,
            to_discard_after_chunk: 0,
            scratch: vec![0; 1024].into_boxed_slice(),
        }
    }

    pub fn into_inner(self) -> McapResult<R> {
        self.reader.into_inner()
    }

    /// Reads the next record from the input stream and copies the raw content into `data`.
    /// Returns the record's opcode as a result.
    pub async fn next_record(&mut self, data: &mut Vec<u8>) -> Option<McapResult<u8>> {
        loop {
            let cmd = match self.next_record_inner(data).await {
                Ok(cmd) => cmd,
                Err(err) => return Some(Err(err)),
            };
            match cmd {
                Cmd::Stop => return None,
                Cmd::YieldRecord(opcode) => return Some(Ok(opcode)),
                Cmd::EnterChunk { header, len } => {
                    let mut reader_state = ReaderState::Empty;
                    std::mem::swap(&mut reader_state, &mut self.reader);
                    match header.compression.as_str() {
                        #[cfg(feature = "zstd")]
                        "zstd" => {
                            let reader = match reader_state.into_inner() {
                                Ok(reader) => reader,
                                Err(err) => return Some(Err(err)),
                            };
                            self.reader = ReaderState::ZstdChunk(ZstdDecoder::new(
                                tokio::io::BufReader::new(reader.take(header.compressed_size)),
                            ));
                        }
                        #[cfg(feature = "lz4")]
                        "lz4" => {
                            let reader = match reader_state.into_inner() {
                                Ok(reader) => reader,
                                Err(err) => return Some(Err(err)),
                            };
                            let decoder = match Lz4Decoder::new(reader.take(header.compressed_size))
                            {
                                Ok(decoder) => decoder,
                                Err(err) => return Some(Err(err.into())),
                            };
                            self.reader = ReaderState::Lz4Chunk(decoder);
                        }
                        "" => {
                            let reader = match reader_state.into_inner() {
                                Ok(reader) => reader,
                                Err(err) => return Some(Err(err)),
                            };
                            self.reader =
                                ReaderState::UncompressedChunk(reader.take(header.compressed_size));
                        }
                        _ => {
                            std::mem::swap(&mut reader_state, &mut self.reader);
                            return Some(Err(McapError::UnsupportedCompression(
                                header.compression.clone(),
                            )));
                        }
                    }
                    self.to_discard_after_chunk = len as usize
                        - (40 + header.compression.len() + header.compressed_size as usize);
                }
                Cmd::ExitChunk => {
                    let mut reader_state = ReaderState::Empty;
                    std::mem::swap(&mut reader_state, &mut self.reader);
                    self.reader = ReaderState::Base(match reader_state.into_inner() {
                        Ok(reader) => reader,
                        Err(err) => return Some(Err(err)),
                    });
                    while self.to_discard_after_chunk > 0 {
                        let to_read = if self.to_discard_after_chunk > self.scratch.len() {
                            self.scratch.len()
                        } else {
                            self.to_discard_after_chunk
                        };
                        if let Err(err) = self.reader.read_exact(&mut self.scratch[..to_read]).await
                        {
                            return Some(Err(err.into()));
                        }
                        self.to_discard_after_chunk -= to_read;
                    }
                }
            };
        }
    }

    async fn next_record_inner(&mut self, data: &mut Vec<u8>) -> McapResult<Cmd> {
        if let ReaderState::Base(reader) = &mut self.reader {
            if !self.start_magic_seen && !self.options.skip_start_magic {
                reader.read_exact(&mut self.scratch[..MAGIC.len()]).await?;
                if &self.scratch[..MAGIC.len()] != MAGIC {
                    return Err(McapError::BadMagic);
                }
                self.start_magic_seen = true;
            }
            if self.footer_seen && !self.options.skip_end_magic {
                reader.read_exact(&mut self.scratch[..MAGIC.len()]).await?;
                if &self.scratch[..MAGIC.len()] != MAGIC {
                    return Err(McapError::BadMagic);
                }
                return Ok(Cmd::Stop);
            }
            reader.read_exact(&mut self.scratch[..9]).await?;
            let opcode = self.scratch[0];
            if opcode == records::op::FOOTER {
                self.footer_seen = true;
            }
            let record_len = byteorder::LittleEndian::read_u64(&self.scratch[1..9]);
            if opcode == records::op::CHUNK && !self.options.emit_chunks {
                let header = read_chunk_header(reader, data, record_len).await?;
                return Ok(Cmd::EnterChunk {
                    header,
                    len: record_len,
                });
            }
            data.resize(record_len as usize, 0);
            reader.read_exact(&mut data[..]).await?;
            Ok(Cmd::YieldRecord(opcode))
        } else {
            let len = self.reader.read(&mut self.scratch[..9]).await?;
            if len == 0 {
                return Ok(Cmd::ExitChunk);
            }
            if len != 9 {
                return Err(McapError::UnexpectedEof);
            }
            let opcode = self.scratch[0];
            let record_len = byteorder::LittleEndian::read_u64(&self.scratch[1..9]);
            data.resize(record_len as usize, 0);
            self.reader.read_exact(&mut data[..]).await?;
            Ok(Cmd::YieldRecord(opcode))
        }
    }
}

async fn read_chunk_header<R: AsyncRead + std::marker::Unpin>(
    reader: &mut R,
    scratch: &mut Vec<u8>,
    record_len: u64,
) -> McapResult<records::ChunkHeader> {
    let mut header = records::ChunkHeader {
        message_start_time: 0,
        message_end_time: 0,
        uncompressed_size: 0,
        uncompressed_crc: 0,
        compression: String::new(),
        compressed_size: 0,
    };
    if record_len < 40 {
        return Err(McapError::RecordTooShort {
            opcode: records::op::CHUNK,
            len: record_len,
            expected: 40,
        });
    }
    scratch.resize(32, 0);
    reader.read_exact(&mut scratch[..]).await?;
    header.message_start_time = byteorder::LittleEndian::read_u64(&scratch[0..8]);
    header.message_end_time = byteorder::LittleEndian::read_u64(&scratch[8..16]);
    header.uncompressed_size = byteorder::LittleEndian::read_u64(&scratch[16..24]);
    header.uncompressed_crc = byteorder::LittleEndian::read_u32(&scratch[24..28]);
    let compression_len = byteorder::LittleEndian::read_u32(&scratch[28..32]);
    scratch.resize(compression_len as usize, 0);
    if record_len < (40 + compression_len) as u64 {
        return Err(McapError::RecordTooShort {
            opcode: records::op::CHUNK,
            len: record_len,
            expected: (40 + compression_len) as u64,
        });
    }
    reader.read_exact(&mut scratch[..]).await?;
    header.compression = match std::str::from_utf8(&scratch[..]) {
        Ok(val) => val.to_owned(),
        Err(err) => {
            return Err(McapError::Parse(binrw::error::Error::Custom {
                pos: 32,
                err: Box::new(err),
            }));
        }
    };
    scratch.resize(8, 0);
    reader.read_exact(&mut scratch[..]).await?;
    header.compressed_size = byteorder::LittleEndian::read_u64(&scratch[..]);
    let available = record_len - (32 + compression_len as u64 + 8);
    if available < header.compressed_size {
        return Err(McapError::BadChunkLength {
            header: header.compressed_size,
            available,
        });
    }
    Ok(header)
}

#[cfg(test)]
mod tests {
    use crate::read::parse_record;
    use std::collections::BTreeMap;

    use super::*;
    #[tokio::test]
    async fn test_record_reader() -> Result<(), McapError> {
        for compression in [
            None,
            #[cfg(feature = "zstd")]
            Some(crate::Compression::Zstd),
            #[cfg(feature = "lz4")]
            Some(crate::Compression::Lz4),
        ] {
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
            let mut reader = RecordReader::new(std::io::Cursor::new(buf.into_inner()));
            let mut record = Vec::new();
            let mut opcodes: Vec<u8> = Vec::new();
            while let Some(opcode) = reader.next_record(&mut record).await {
                let opcode = opcode?;
                opcodes.push(opcode);
                parse_record(opcode, &record)?;
            }
            assert_eq!(
                opcodes.as_slice(),
                [
                    records::op::HEADER,
                    records::op::CHANNEL,
                    records::op::MESSAGE,
                    records::op::MESSAGE_INDEX,
                    records::op::DATA_END,
                    records::op::CHANNEL,
                    records::op::CHUNK_INDEX,
                    records::op::STATISTICS,
                    records::op::SUMMARY_OFFSET,
                    records::op::SUMMARY_OFFSET,
                    records::op::SUMMARY_OFFSET,
                    records::op::FOOTER,
                ],
                "reads opcodes from MCAP compressed with {:?}",
                compression
            );
        }
        Ok(())
    }
}
