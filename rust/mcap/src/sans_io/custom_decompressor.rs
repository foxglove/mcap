use std::collections::BTreeMap;
use std::io::{Cursor, Read};

use binrw::{BinWrite, BinWriterExt};

use super::decompressor::{DecompressResult, Decompressor};
use super::{IndexedReadEvent, IndexedReader, LinearReadEvent, LinearReader};
use crate::records::{
    op, Channel, ChunkHeader, ChunkIndex, DataEnd, Footer, Header, MessageHeader, Record,
};
use crate::{parse_record, McapError, McapResult, Summary, MAGIC};

const XOR: u8 = 0x5A;

enum DecoderKind {
    /// Length-prefixed XOR. `remaining == Some(0)` means the frame finished and `reset` has not run.
    Xor {
        remaining: Option<usize>,
        len_buf: [u8; 4],
        len_filled: usize,
    },
    Stall,
    Fail,
}

struct TestDecoder {
    name: &'static str,
    kind: DecoderKind,
}

impl TestDecoder {
    fn xor(name: &'static str) -> Self {
        Self {
            name,
            kind: DecoderKind::Xor {
                remaining: None,
                len_buf: [0; 4],
                len_filled: 0,
            },
        }
    }

    fn stall(name: &'static str) -> Self {
        Self {
            name,
            kind: DecoderKind::Stall,
        }
    }

    fn fail(name: &'static str) -> Self {
        Self {
            name,
            kind: DecoderKind::Fail,
        }
    }
}

impl Decompressor for TestDecoder {
    fn next_read_size(&self) -> usize {
        1
    }

    fn decompress(&mut self, src: &[u8], dst: &mut [u8]) -> McapResult<DecompressResult> {
        match &mut self.kind {
            DecoderKind::Stall => Ok(DecompressResult {
                consumed: 0,
                wrote: 0,
            }),
            DecoderKind::Fail => Err(McapError::DecompressionError("caller decompressor".into())),
            DecoderKind::Xor {
                remaining,
                len_buf,
                len_filled,
            } => xor_decompress(remaining, len_buf, len_filled, src, dst),
        }
    }

    fn reset(&mut self) -> McapResult<()> {
        if let DecoderKind::Xor {
            remaining,
            len_filled,
            ..
        } = &mut self.kind
        {
            *remaining = None;
            *len_filled = 0;
        }
        Ok(())
    }

    fn name(&self) -> &'static str {
        self.name
    }
}

fn xor_decompress(
    remaining: &mut Option<usize>,
    len_buf: &mut [u8; 4],
    len_filled: &mut usize,
    src: &[u8],
    dst: &mut [u8],
) -> McapResult<DecompressResult> {
    if src.is_empty() {
        return Ok(DecompressResult {
            consumed: 0,
            wrote: 0,
        });
    }
    match *remaining {
        Some(0) => Err(McapError::DecompressionError(
            "decompressor was not reset".into(),
        )),
        Some(left) => {
            if dst.is_empty() {
                return Ok(DecompressResult {
                    consumed: 0,
                    wrote: 0,
                });
            }
            let take = src.len().min(dst.len()).min(left);
            for (out, input) in dst.iter_mut().zip(src.iter()).take(take) {
                *out = input ^ XOR;
            }
            *remaining = Some(left - take);
            Ok(DecompressResult {
                consumed: take,
                wrote: take,
            })
        }
        None => {
            let take = (4 - *len_filled).min(src.len());
            len_buf[*len_filled..*len_filled + take].copy_from_slice(&src[..take]);
            *len_filled += take;
            if *len_filled == 4 {
                let len = u32::from_le_bytes(*len_buf) as usize;
                *remaining = Some(len);
                *len_filled = 0;
            }
            Ok(DecompressResult {
                consumed: take,
                wrote: 0,
            })
        }
    }
}

fn append_record(buf: &mut Vec<u8>, opcode: u8, body: &[u8]) {
    buf.push(opcode);
    buf.extend_from_slice(&(body.len() as u64).to_le_bytes());
    buf.extend_from_slice(body);
}

fn write_body<T>(value: &T) -> Vec<u8>
where
    T: BinWrite,
    for<'a> T::Args<'a>: Default,
{
    let mut body = Vec::new();
    Cursor::new(&mut body).write_le(value).unwrap();
    body
}

struct ChunkSpec {
    log_time: u64,
    payload: &'static [u8],
    trailing: usize,
}

/// Builds an indexed MCAP whose chunks use the `"xor"` compression string.
fn xor_mcap(chunks: &[ChunkSpec]) -> Vec<u8> {
    let mut file = Vec::new();
    file.extend_from_slice(MAGIC);
    append_record(
        &mut file,
        op::HEADER,
        &write_body(&Header {
            profile: String::new(),
            library: "test".into(),
        }),
    );

    let mut indexes = Vec::new();
    for chunk in chunks {
        let mut message_body = write_body(&MessageHeader {
            channel_id: 1,
            sequence: 1,
            log_time: chunk.log_time,
            publish_time: chunk.log_time,
        });
        message_body.extend_from_slice(chunk.payload);
        let mut records = Vec::new();
        append_record(&mut records, op::MESSAGE, &message_body);
        let mut compressed = Vec::with_capacity(4 + records.len() + chunk.trailing);
        compressed.extend_from_slice(&(records.len() as u32).to_le_bytes());
        compressed.extend(records.iter().map(|byte| byte ^ XOR));
        compressed.extend(std::iter::repeat_n(0xA5u8, chunk.trailing));

        let mut chunk_body = write_body(&ChunkHeader {
            message_start_time: chunk.log_time,
            message_end_time: chunk.log_time,
            uncompressed_size: records.len() as u64,
            uncompressed_crc: 0,
            compression: "xor".into(),
            compressed_size: compressed.len() as u64,
        });
        chunk_body.extend_from_slice(&compressed);
        let chunk_start = file.len() as u64;
        append_record(&mut file, op::CHUNK, &chunk_body);
        indexes.push(ChunkIndex {
            message_start_time: chunk.log_time,
            message_end_time: chunk.log_time,
            chunk_start_offset: chunk_start,
            chunk_length: file.len() as u64 - chunk_start,
            message_index_offsets: BTreeMap::new(),
            message_index_length: 0,
            compression: "xor".into(),
            compressed_size: compressed.len() as u64,
            uncompressed_size: records.len() as u64,
        });
    }

    append_record(
        &mut file,
        op::DATA_END,
        &write_body(&DataEnd {
            data_section_crc: 0,
        }),
    );
    let summary_start = file.len() as u64;
    append_record(
        &mut file,
        op::CHANNEL,
        &write_body(&Channel {
            id: 1,
            schema_id: 0,
            topic: "topic".into(),
            message_encoding: "raw".into(),
            metadata: BTreeMap::new(),
        }),
    );
    for index in &indexes {
        append_record(&mut file, op::CHUNK_INDEX, &write_body(index));
    }
    append_record(
        &mut file,
        op::FOOTER,
        &write_body(&Footer {
            summary_start,
            summary_offset_start: 0,
            summary_crc: 0,
        }),
    );
    file.extend_from_slice(MAGIC);
    file
}

fn sample_chunks() -> Vec<ChunkSpec> {
    vec![
        ChunkSpec {
            log_time: 10,
            payload: b"one",
            trailing: 0,
        },
        ChunkSpec {
            log_time: 20,
            payload: b"two",
            trailing: 3,
        },
    ]
}

fn read_linear(
    mcap: &[u8],
    decompressor: Option<TestDecoder>,
) -> McapResult<Vec<(u16, u64, Vec<u8>)>> {
    let mut reader = LinearReader::new();
    if let Some(decompressor) = decompressor {
        reader.add_decompressor(decompressor)?;
    }
    let mut cursor = Cursor::new(mcap);
    let mut messages = Vec::new();
    let mut iterations = 0;
    while let Some(event) = reader.next_event() {
        iterations += 1;
        assert!(iterations < 100_000, "linear reader did not finish");
        match event? {
            LinearReadEvent::ReadRequest(need) => {
                let read = cursor.read(reader.insert(need))?;
                reader.notify_read(read);
            }
            LinearReadEvent::Record { opcode, data } => {
                if opcode == op::MESSAGE {
                    let Record::Message { header, data } = parse_record(opcode, data)? else {
                        panic!("opcode was MESSAGE");
                    };
                    messages.push((header.channel_id, header.log_time, data.into_owned()));
                }
            }
        }
    }
    Ok(messages)
}

fn read_indexed(
    mcap: &[u8],
    decompressor: Option<TestDecoder>,
) -> McapResult<Vec<(u16, u64, Vec<u8>)>> {
    let summary = Summary::read(mcap)?.expect("file should contain a summary");
    let mut reader = IndexedReader::new(&summary)?;
    if let Some(decompressor) = decompressor {
        reader.add_decompressor(decompressor)?;
    }
    let mut messages = Vec::new();
    let mut iterations = 0;
    while let Some(event) = reader.next_event() {
        iterations += 1;
        assert!(iterations < 100_000, "indexed reader did not finish");
        match event? {
            IndexedReadEvent::ReadChunkRequest { offset, length } => {
                let start = offset as usize;
                reader.insert_chunk_record_data(offset, &mcap[start..start + length])?;
            }
            IndexedReadEvent::Message { header, data } => {
                messages.push((header.channel_id, header.log_time, data.to_vec()));
            }
        }
    }
    Ok(messages)
}

fn uncompressed_mcap(compression: Option<crate::Compression>) -> Vec<u8> {
    let mut writer = crate::WriteOptions::new()
        .compression(compression)
        .chunk_size(None)
        .create(Cursor::new(Vec::new()))
        .expect("writer");
    writer
        .write(&crate::Message {
            channel: std::sync::Arc::new(crate::Channel {
                id: 1,
                topic: "topic".into(),
                schema: None,
                message_encoding: "raw".into(),
                metadata: BTreeMap::new(),
            }),
            sequence: 1,
            log_time: 5,
            publish_time: 5,
            data: std::borrow::Cow::Borrowed(b"plain"),
        })
        .expect("write");
    writer.finish().expect("finish");
    writer.into_inner().into_inner()
}

fn assert_no_progress(err: McapError) {
    match err {
        McapError::DecompressionError(message) => {
            assert!(
                message.contains("no progress"),
                "unexpected decompression error: {message}"
            );
        }
        other => panic!("expected a stalled decompressor, got {other}"),
    }
}

fn assert_caller_decompressor(err: McapError) {
    match err {
        McapError::DecompressionError(message) => {
            assert!(
                message.contains("caller decompressor"),
                "builtin decoder ran instead of the registered one: {message}"
            );
        }
        other => panic!("expected the caller decompressor to run, got {other}"),
    }
}

#[test]
fn add_decompressor_rejects_empty_and_duplicate_names() {
    let mut linear = LinearReader::new();
    assert!(matches!(
        linear.add_decompressor(TestDecoder::fail("")),
        Err(McapError::EmptyDecompressorName)
    ));
    linear
        .add_decompressor(TestDecoder::xor("xor"))
        .expect("first registration");
    assert!(matches!(
        linear.add_decompressor(TestDecoder::xor("xor")),
        Err(McapError::DuplicateDecompressor(name)) if name == "xor"
    ));

    let mut indexed = IndexedReader::new(&Summary::default()).expect("empty summary");
    assert!(matches!(
        indexed.add_decompressor(TestDecoder::fail("")),
        Err(McapError::EmptyDecompressorName)
    ));
    indexed
        .add_decompressor(TestDecoder::xor("xor"))
        .expect("first registration");
    assert!(matches!(
        indexed.add_decompressor(TestDecoder::xor("xor")),
        Err(McapError::DuplicateDecompressor(name)) if name == "xor"
    ));
}

#[test]
fn custom_compression_round_trips_through_both_readers() {
    let mcap = xor_mcap(&sample_chunks());
    let expected = vec![(1, 10, b"one".to_vec()), (1, 20, b"two".to_vec())];
    assert_eq!(
        read_linear(&mcap, Some(TestDecoder::xor("xor"))).expect("linear"),
        expected
    );
    assert_eq!(
        read_indexed(&mcap, Some(TestDecoder::xor("xor"))).expect("indexed"),
        expected
    );
}

#[test]
fn custom_compression_without_registration_is_unsupported() {
    let mcap = xor_mcap(&sample_chunks());
    assert!(matches!(
        read_linear(&mcap, None),
        Err(McapError::UnsupportedCompression(name)) if name == "xor"
    ));
    assert!(matches!(
        read_indexed(&mcap, None),
        Err(McapError::UnsupportedCompression(name)) if name == "xor"
    ));
}

#[test]
fn stalled_decompressor_returns_an_error() {
    let mcap = xor_mcap(&sample_chunks());
    assert_no_progress(
        read_linear(&mcap, Some(TestDecoder::stall("xor"))).expect_err("linear should stall"),
    );
    assert_no_progress(
        read_indexed(&mcap, Some(TestDecoder::stall("xor"))).expect_err("indexed should stall"),
    );
}

#[test]
fn uncompressed_chunks_ignore_registered_decompressors() {
    let mcap = uncompressed_mcap(None);
    let expected = vec![(1, 5, b"plain".to_vec())];
    assert_eq!(
        read_linear(&mcap, Some(TestDecoder::stall("xor"))).expect("linear"),
        expected
    );
    assert_eq!(
        read_indexed(&mcap, Some(TestDecoder::stall("xor"))).expect("indexed"),
        expected
    );
}

#[cfg(feature = "zstd")]
#[test]
fn registered_zstd_decompressor_replaces_builtin() {
    let mcap = uncompressed_mcap(Some(crate::Compression::Zstd));
    assert_caller_decompressor(
        read_linear(&mcap, Some(TestDecoder::fail("zstd"))).expect_err("linear should use caller"),
    );
    assert_caller_decompressor(
        read_indexed(&mcap, Some(TestDecoder::fail("zstd")))
            .expect_err("indexed should use caller"),
    );
    let expected = vec![(1, 5, b"plain".to_vec())];
    assert_eq!(
        read_linear(&mcap, Some(TestDecoder::xor("xor"))).expect("builtin zstd"),
        expected
    );
    assert_eq!(
        read_indexed(&mcap, Some(TestDecoder::xor("xor"))).expect("builtin zstd"),
        expected
    );
}

#[cfg(feature = "lz4")]
#[test]
fn registered_lz4_decompressor_replaces_builtin() {
    let mcap = uncompressed_mcap(Some(crate::Compression::Lz4));
    assert_caller_decompressor(
        read_linear(&mcap, Some(TestDecoder::fail("lz4"))).expect_err("linear should use caller"),
    );
    assert_caller_decompressor(
        read_indexed(&mcap, Some(TestDecoder::fail("lz4"))).expect_err("indexed should use caller"),
    );
}
