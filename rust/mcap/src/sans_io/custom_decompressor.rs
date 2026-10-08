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
    /// Length-prefixed XOR. After the payload, an optional `0xFF` footer must be consumed before
    /// [`Decompressor::reset`]. Further bytes are padding and are not consumed. `remaining ==
    /// Some(0)` with no footer pending means the frame is finished.
    Xor {
        remaining: Option<usize>,
        len_buf: [u8; 4],
        len_filled: usize,
        require_footer: bool,
        need_footer: bool,
    },
    Stall,
    Fail,
    /// Reports a `consumed` or `wrote` past the end of the buffer it was given.
    OverReport {
        wrote: bool,
    },
    /// Fills the entire output buffer on every call.
    Greedy,
}

struct TestDecoder {
    name: &'static str,
    kind: DecoderKind,
}

impl TestDecoder {
    fn xor(name: &'static str) -> Self {
        Self::xor_inner(name, false)
    }

    fn xor_with_footer(name: &'static str) -> Self {
        Self::xor_inner(name, true)
    }

    fn xor_inner(name: &'static str, require_footer: bool) -> Self {
        Self {
            name,
            kind: DecoderKind::Xor {
                remaining: None,
                len_buf: [0; 4],
                len_filled: 0,
                require_footer,
                need_footer: false,
            },
        }
    }

    fn over_report(name: &'static str, wrote: bool) -> Self {
        Self {
            name,
            kind: DecoderKind::OverReport { wrote },
        }
    }

    fn greedy(name: &'static str) -> Self {
        Self {
            name,
            kind: DecoderKind::Greedy,
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
            DecoderKind::OverReport { wrote } => Ok(DecompressResult {
                consumed: src.len() + 1,
                wrote: if *wrote { dst.len() + 1 } else { 0 },
            }),
            DecoderKind::Greedy => Ok(DecompressResult {
                consumed: usize::from(!src.is_empty()),
                wrote: dst.len(),
            }),
            DecoderKind::Xor {
                remaining,
                len_buf,
                len_filled,
                require_footer,
                need_footer,
            } => xor_decompress(
                remaining,
                len_buf,
                len_filled,
                *require_footer,
                need_footer,
                src,
                dst,
            ),
        }
    }

    fn reset(&mut self) -> McapResult<()> {
        if let DecoderKind::Xor {
            remaining,
            len_filled,
            need_footer,
            ..
        } = &mut self.kind
        {
            if *need_footer {
                return Err(McapError::DecompressionError(
                    "frame footer was not consumed".into(),
                ));
            }
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
    require_footer: bool,
    need_footer: &mut bool,
    src: &[u8],
    dst: &mut [u8],
) -> McapResult<DecompressResult> {
    if *need_footer {
        if src.is_empty() {
            return Ok(DecompressResult {
                consumed: 0,
                wrote: 0,
            });
        }
        if src[0] != 0xFF {
            return Err(McapError::DecompressionError("bad frame footer".into()));
        }
        *need_footer = false;
        *remaining = Some(0);
        return Ok(DecompressResult {
            consumed: 1,
            wrote: 0,
        });
    }
    if src.is_empty() {
        return Ok(DecompressResult {
            consumed: 0,
            wrote: 0,
        });
    }
    match *remaining {
        // Frame is finished. Leftover bytes are padding; a later chunk has to reset() first or
        // this stays finished and the reader observes no progress.
        Some(0) => Ok(DecompressResult {
            consumed: 0,
            wrote: 0,
        }),
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
            if left == take && require_footer {
                *need_footer = true;
            }
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
    /// When true, a `0xFF` footer sits between the XOR payload and any trailing padding.
    footer: bool,
    /// Overrides the uncompressed size declared in the chunk header and chunk index.
    declared_uncompressed: Option<u64>,
}

impl ChunkSpec {
    fn new(log_time: u64, payload: &'static [u8], trailing: usize) -> Self {
        Self {
            log_time,
            payload,
            trailing,
            footer: false,
            declared_uncompressed: None,
        }
    }
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
        let mut compressed = Vec::with_capacity(4 + records.len() + chunk.trailing + 1);
        compressed.extend_from_slice(&(records.len() as u32).to_le_bytes());
        compressed.extend(records.iter().map(|byte| byte ^ XOR));
        if chunk.footer {
            compressed.push(0xFF);
        }
        compressed.extend(std::iter::repeat_n(0xA5u8, chunk.trailing));
        let uncompressed_size = chunk.declared_uncompressed.unwrap_or(records.len() as u64);

        let mut chunk_body = write_body(&ChunkHeader {
            message_start_time: chunk.log_time,
            message_end_time: chunk.log_time,
            uncompressed_size,
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
            uncompressed_size,
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
    vec![ChunkSpec::new(10, b"one", 0), ChunkSpec::new(20, b"two", 3)]
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

#[test]
fn over_reported_buffer_lengths_are_errors() {
    let mcap = xor_mcap(&sample_chunks());
    for wrote in [false, true] {
        let linear = read_linear(&mcap, Some(TestDecoder::over_report("xor", wrote)))
            .expect_err("linear should reject an over-reported result");
        let indexed = read_indexed(&mcap, Some(TestDecoder::over_report("xor", wrote)))
            .expect_err("indexed should reject an over-reported result");
        for err in [linear, indexed] {
            match err {
                McapError::DecompressionError(message) => {
                    assert!(
                        message.contains("more bytes than the buffers"),
                        "unexpected error: {message}"
                    );
                }
                other => panic!("expected a decompression error, got {other}"),
            }
        }
    }
}

#[test]
fn indexed_reader_feeds_frame_footer_then_leaves_padding() {
    let mut chunks = sample_chunks();
    for chunk in &mut chunks {
        chunk.footer = true;
    }
    let mcap = xor_mcap(&chunks);
    let expected = vec![(1, 10, b"one".to_vec()), (1, 20, b"two".to_vec())];
    assert_eq!(
        read_indexed(&mcap, Some(TestDecoder::xor_with_footer("xor"))).expect("indexed footer"),
        expected
    );
}

#[test]
fn greedy_output_past_the_declared_size_is_an_error() {
    let mut chunks = vec![ChunkSpec::new(10, b"one", 0)];
    chunks[0].declared_uncompressed = Some(4);
    let mcap = xor_mcap(&chunks);
    let linear = read_linear(&mcap, Some(TestDecoder::greedy("xor")))
        .expect_err("linear should reject output past the uncompressed size");
    match linear {
        McapError::DecompressionError(message) => {
            assert!(
                message.contains("uncompressed bytes remaining"),
                "unexpected error: {message}"
            );
        }
        other => panic!("expected a decompression error, got {other}"),
    }
    let indexed = read_indexed(&mcap, Some(TestDecoder::greedy("xor")))
        .expect_err("indexed should reject output past the uncompressed size");
    match indexed {
        McapError::DecompressionError(message) => {
            assert!(
                message.contains("produced more than 4 bytes"),
                "unexpected error: {message}"
            );
        }
        other => panic!("expected a decompression error, got {other}"),
    }
}

fn two_chunk_mcap(compression: Option<crate::Compression>) -> Vec<u8> {
    let mut writer = crate::WriteOptions::new()
        .compression(compression)
        .chunk_size(None)
        .create(Cursor::new(Vec::new()))
        .expect("writer");
    let channel = std::sync::Arc::new(crate::Channel {
        id: 1,
        topic: "topic".into(),
        schema: None,
        message_encoding: "raw".into(),
        metadata: BTreeMap::new(),
    });
    for (i, payload) in [b"aaa".as_slice(), b"bbb".as_slice()]
        .into_iter()
        .enumerate()
    {
        writer
            .write(&crate::Message {
                channel: channel.clone(),
                sequence: i as u32,
                log_time: i as u64,
                publish_time: i as u64,
                data: std::borrow::Cow::Borrowed(payload),
            })
            .expect("write");
        writer.flush().expect("flush");
    }
    writer.finish().expect("finish");
    writer.into_inner().into_inner()
}

#[cfg(feature = "zstd")]
struct ResetFails {
    inner: super::zstd::ZstdDecoder,
    poisoned: bool,
}

#[cfg(feature = "zstd")]
impl Decompressor for ResetFails {
    fn next_read_size(&self) -> usize {
        self.inner.next_read_size()
    }

    fn decompress(&mut self, src: &[u8], dst: &mut [u8]) -> McapResult<DecompressResult> {
        if self.poisoned {
            return Err(McapError::DecompressionError(
                "reused after failed reset".into(),
            ));
        }
        self.inner.decompress(src, dst)
    }

    fn reset(&mut self) -> McapResult<()> {
        self.poisoned = true;
        Err(McapError::DecompressionError("reset failed".into()))
    }

    fn name(&self) -> &'static str {
        "zstd"
    }
}

#[cfg(feature = "zstd")]
#[test]
fn failed_reset_does_not_fall_back_to_the_builtin_decoder() {
    let mcap = two_chunk_mcap(Some(crate::Compression::Zstd));
    let mut reader = LinearReader::new();
    reader
        .add_decompressor(ResetFails {
            inner: super::zstd::ZstdDecoder::new(),
            poisoned: false,
        })
        .expect("register");
    let mut cursor = Cursor::new(&mcap);
    let mut errors = Vec::new();
    let mut messages = 0;
    let mut iterations = 0;
    while let Some(event) = reader.next_event() {
        iterations += 1;
        assert!(iterations < 100_000, "linear reader did not finish");
        match event {
            Ok(LinearReadEvent::ReadRequest(need)) => {
                let read = cursor.read(reader.insert(need)).expect("read");
                reader.notify_read(read);
            }
            Ok(LinearReadEvent::Record { opcode, .. }) => {
                if opcode == op::MESSAGE {
                    messages += 1;
                }
            }
            Err(err) => {
                errors.push(err.to_string());
                if errors
                    .iter()
                    .any(|err| err.contains("reused after failed reset"))
                {
                    break;
                }
            }
        }
    }
    assert!(
        errors.iter().any(|err| err.contains("reset failed")),
        "errors: {errors:?}"
    );
    assert!(
        errors
            .iter()
            .any(|err| err.contains("reused after failed reset")),
        "the built-in decoder ran after reset failed: {errors:?}"
    );
    assert_eq!(messages, 1, "the second chunk should not decode");
}

#[cfg(feature = "zstd")]
#[test]
fn builtin_cache_does_not_count_as_a_caller_registration() {
    let mcap = two_chunk_mcap(Some(crate::Compression::Zstd));
    let messages = read_linear(&mcap, None).expect("builtin zstd");
    assert_eq!(messages.len(), 2);
    let mut reader = LinearReader::new();
    let mut cursor = Cursor::new(&mcap);
    while let Some(event) = reader.next_event() {
        match event.expect("read") {
            LinearReadEvent::ReadRequest(need) => {
                let read = cursor.read(reader.insert(need)).expect("read");
                reader.notify_read(read);
            }
            LinearReadEvent::Record { .. } => {}
        }
    }
    reader
        .add_decompressor(TestDecoder::fail("zstd"))
        .expect("a cached built-in decoder is not a caller registration");
}
