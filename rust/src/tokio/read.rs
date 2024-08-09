use byteorder::ByteOrder;
use std::future::Future;
use tokio::io::{AsyncRead, AsyncReadExt, BufReader, Take};
use tokio_stream::Stream;

use crate::tokio::lz4::Lz4Decoder;
use crate::{records, McapError, McapResult, MAGIC};
use async_compression::tokio::bufread::ZstdDecoder;

enum ReaderState<R> {
    Base(R),
    UncompressedChunk(Take<R>),
    ZstdChunk(ZstdDecoder<BufReader<Take<R>>>),
    Lz4Chunk(Lz4Decoder<Take<R>>),
    Empty,
}

impl<R> AsyncRead for ReaderState<R>
where
    R: AsyncRead + std::marker::Unpin,
{
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match self.get_mut() {
            ReaderState::Base(r) => std::pin::pin!(r).poll_read(cx, buf),
            ReaderState::UncompressedChunk(r) => std::pin::pin!(r).poll_read(cx, buf),
            ReaderState::ZstdChunk(r) => std::pin::pin!(r).poll_read(cx, buf),
            ReaderState::Lz4Chunk(r) => std::pin::pin!(r).poll_read(cx, buf),
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
            ReaderState::ZstdChunk(decoder) => Ok(decoder.into_inner().into_inner().into_inner()),
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
    options: Options,
    start_magic_seen: bool,
    footer_seen: bool,
    scratch: [u8; 9],
}

#[derive(Default, Clone)]
pub struct Options {
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
    EnterChunk(records::ChunkHeader),
    ExitChunk,
    Stop,
}

impl<R> RecordReader<R>
where
    R: AsyncRead + std::marker::Unpin,
{
    pub fn new(reader: R) -> Self {
        Self::new_with_options(reader, &Options::default())
    }

    pub fn new_with_options(reader: R, options: &Options) -> Self {
        Self {
            reader: ReaderState::Base(reader),
            options: options.clone(),
            start_magic_seen: false,
            footer_seen: false,
            scratch: [0; 9],
        }
    }

    pub fn into_inner(self) -> McapResult<R> {
        self.reader.into_inner()
    }

    /// Reads the next record from the input stream and copies the raw content into `data`.
    /// Returns the record's opcode as a result.
    pub async fn next_record(&mut self, data: &mut Vec<u8>) -> McapResult<Option<u8>> {
        loop {
            let cmd = self.next_record_inner(data).await?;
            match cmd {
                Cmd::Stop => return Ok(None),
                Cmd::YieldRecord(opcode) => return Ok(Some(opcode)),
                Cmd::EnterChunk(header) => {
                    let mut rdr = ReaderState::Empty;
                    std::mem::swap(&mut rdr, &mut self.reader);
                    match header.compression.as_str() {
                        "zstd" => {
                            self.reader = ReaderState::ZstdChunk(ZstdDecoder::new(BufReader::new(
                                rdr.into_inner()?.take(header.compressed_size),
                            )));
                        }
                        "lz4" => {
                            let decoder =
                                Lz4Decoder::new(rdr.into_inner()?.take(header.compressed_size))?;
                            self.reader = ReaderState::Lz4Chunk(decoder);
                        }
                        "" => {
                            self.reader = ReaderState::UncompressedChunk(
                                rdr.into_inner()?.take(header.compressed_size),
                            );
                        }
                        _ => {
                            std::mem::swap(&mut rdr, &mut self.reader);
                            return Err(McapError::UnsupportedCompression(
                                header.compression.clone(),
                            ));
                        }
                    }
                }
                Cmd::ExitChunk => {
                    let mut rdr = ReaderState::Empty;
                    std::mem::swap(&mut rdr, &mut self.reader);
                    self.reader = ReaderState::Base(rdr.into_inner()?)
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
            reader.read_exact(&mut self.scratch).await?;
            let opcode = self.scratch[0];
            if opcode == records::op::FOOTER {
                self.footer_seen = true;
            }
            let record_len = byteorder::LittleEndian::read_u64(&self.scratch[1..]);
            if opcode == records::op::CHUNK && !self.options.emit_chunks {
                let chunk_header = read_chunk_header(reader, data, record_len).await?;
                return Ok(Cmd::EnterChunk(chunk_header));
            }
            data.resize(record_len as usize, 0);
            reader.read_exact(&mut data[..]).await?;
            Ok(Cmd::YieldRecord(opcode))
        } else {
            let len = self.reader.read(&mut self.scratch).await?;
            if len == 0 {
                return Ok(Cmd::ExitChunk);
            }
            if len != self.scratch.len() {
                return Err(McapError::UnexpectedEof);
            }
            let opcode = self.scratch[0];
            let record_len = byteorder::LittleEndian::read_u64(&self.scratch[1..]);
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

/// implements a Stream of owned `crate::record::Record` values.
pub struct LinearStream<R> {
    r: RecordReader<R>,
    buf: Vec<u8>,
}

impl<R: AsyncRead + std::marker::Unpin> LinearStream<R> {
    /// Creates a new stream of records from a reader.
    pub fn new(r: R) -> Self {
        Self {
            r: RecordReader::new(r),
            buf: Vec::new(),
        }
    }

    pub fn into_inner(self) -> McapResult<R> {
        self.r.into_inner()
    }
}

impl<R: AsyncRead + std::marker::Unpin> Stream for LinearStream<R> {
    type Item = McapResult<crate::records::Record<'static>>;
    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        // we do this swap maneuver in order to appease the borrow checker and also reuse one read
        // buf across several records.
        let mut buf = Vec::new();
        std::mem::swap(&mut buf, &mut self.buf);
        let opcode = {
            let res = std::pin::pin!((&mut self).r.next_record(&mut buf)).poll(cx);
            match res {
                std::task::Poll::Pending => {
                    std::mem::swap(&mut buf, &mut self.buf);
                    return std::task::Poll::Pending;
                }
                std::task::Poll::Ready(result) => match result {
                    Err(err) => {
                        std::mem::swap(&mut buf, &mut self.buf);
                        return std::task::Poll::Ready(Some(Err(err)));
                    }
                    Ok(None) => {
                        std::mem::swap(&mut buf, &mut self.buf);
                        return std::task::Poll::Ready(None);
                    }
                    Ok(Some(op)) => op,
                },
            }
        };
        let parse_res = crate::read::read_record(opcode, &buf[..]);
        let result = std::task::Poll::Ready(Some(match parse_res {
            Ok(record) => Ok(record.into_owned()),
            Err(err) => Err(err),
        }));
        std::mem::swap(&mut buf, &mut self.buf);
        return result;
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use tokio_stream::StreamExt;

    use super::*;
    #[tokio::test]
    async fn test_record_reader() -> Result<(), McapError> {
        for compression in [
            None,
            Some(crate::Compression::Lz4),
            Some(crate::Compression::Zstd),
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
            loop {
                let opcode = reader.next_record(&mut record).await?;
                if let Some(opcode) = opcode {
                    opcodes.push(opcode);
                } else {
                    break;
                }
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
    #[tokio::test]
    async fn test_linear_stream() -> Result<(), McapError> {
        for compression in [
            None,
            Some(crate::Compression::Lz4),
            Some(crate::Compression::Zstd),
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
            let mut reader = LinearStream::new(std::io::Cursor::new(buf.into_inner()));
            let mut opcodes: Vec<u8> = Vec::new();
            while let Some(result) = reader.next().await {
                let record = result?;
                opcodes.push(record.opcode())
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
                "reads records from MCAP compressed with {:?}",
                compression
            );
        }
        Ok(())
    }
}
