use std::collections::BTreeMap;
use std::io::{Cursor, Read};

use binrw::{BinRead, BinWrite, BinWriterExt};

use super::decompressor::{DecompressResult, Decompressor};
use super::{IndexedReadEvent, IndexedReader, LinearReadEvent, LinearReader, LinearReaderOptions};
use crate::records::{
    op, Channel, ChunkHeader, ChunkIndex, DataEnd, Footer, Header, MessageHeader, Record,
};
use crate::{parse_record, McapError, McapResult, Summary, MAGIC};

const XOR_KEY: u8 = 0x5A;

/// Decoding state for one length-prefixed XOR frame.
#[derive(Default)]
struct XorFrame {
    remaining: Option<usize>,
    len_buf: [u8; 4],
    len_filled: usize,
}

enum DecoderKind {
    /// Length-prefixed XOR. Once the payload is finished, passing more input is an error so tests
    /// can tell that padding after the frame was fed to the decoder. `reset` starts a new frame.
    Xor(XorFrame),
    /// Like `Xor`, but makes no progress until the whole frame is in `src`.
    WholeFrame {
        frame: XorFrame,
        /// Raise `next_read_size` to the frame size once the length prefix has been seen.
        hint_frame_size: bool,
        hint: Option<usize>,
    },
    Stall,
    Fail,
    /// Reports a `consumed` past the end of `src`.
    OverConsume,
    /// Reports a `wrote` past the end of `dst`.
    OverWrite,
    /// Fills the entire output buffer on every call.
    Greedy,
}

struct TestDecoder {
    name: &'static str,
    kind: DecoderKind,
    read_size: usize,
    fail_reset: bool,
}

impl TestDecoder {
    fn new(name: &'static str, kind: DecoderKind) -> Self {
        Self {
            name,
            kind,
            read_size: 1,
            fail_reset: false,
        }
    }

    fn with_read_size(mut self, read_size: usize) -> Self {
        self.read_size = read_size;
        self
    }

    fn with_failing_reset(mut self) -> Self {
        self.fail_reset = true;
        self
    }

    fn xor(name: &'static str) -> Self {
        Self::new(name, DecoderKind::Xor(XorFrame::default()))
    }

    fn whole_frame(name: &'static str) -> Self {
        Self::new(
            name,
            DecoderKind::WholeFrame {
                frame: XorFrame::default(),
                hint_frame_size: false,
                hint: None,
            },
        )
    }

    fn whole_frame_with_hint(name: &'static str) -> Self {
        Self::new(
            name,
            DecoderKind::WholeFrame {
                frame: XorFrame::default(),
                hint_frame_size: true,
                hint: None,
            },
        )
    }

    fn over_consume(name: &'static str) -> Self {
        Self::new(name, DecoderKind::OverConsume)
    }

    fn over_write(name: &'static str) -> Self {
        Self::new(name, DecoderKind::OverWrite)
    }

    fn greedy(name: &'static str) -> Self {
        Self::new(name, DecoderKind::Greedy)
    }

    fn stall(name: &'static str) -> Self {
        Self::new(name, DecoderKind::Stall)
    }

    fn fail(name: &'static str) -> Self {
        Self::new(name, DecoderKind::Fail)
    }
}

impl Decompressor for TestDecoder {
    fn next_read_size(&self) -> usize {
        match &self.kind {
            DecoderKind::WholeFrame {
                hint: Some(hint), ..
            } => *hint,
            _ => self.read_size,
        }
    }

    fn decompress(&mut self, src: &[u8], dst: &mut [u8]) -> McapResult<DecompressResult> {
        match &mut self.kind {
            DecoderKind::Stall => Ok(DecompressResult {
                consumed: 0,
                wrote: 0,
            }),
            DecoderKind::Fail => Err(McapError::DecompressionError("caller decompressor".into())),
            DecoderKind::OverConsume => Ok(DecompressResult {
                consumed: src.len() + 1,
                wrote: 0,
            }),
            DecoderKind::OverWrite => Ok(DecompressResult {
                consumed: 0,
                wrote: dst.len() + 1,
            }),
            DecoderKind::Greedy => Ok(DecompressResult {
                consumed: usize::from(!src.is_empty()),
                wrote: dst.len(),
            }),
            DecoderKind::Xor(frame) => xor_decompress(frame, src, dst),
            DecoderKind::WholeFrame {
                frame,
                hint_frame_size,
                hint,
            } => {
                if frame.remaining.is_none() {
                    let frame_size = src
                        .get(..4)
                        .map(|len| 4 + u32::from_le_bytes(len.try_into().unwrap()) as usize);
                    if frame_size.map_or(true, |size| src.len() < size) {
                        if *hint_frame_size {
                            *hint = frame_size;
                        }
                        return Ok(DecompressResult {
                            consumed: 0,
                            wrote: 0,
                        });
                    }
                }
                xor_decompress(frame, src, dst)
            }
        }
    }

    fn reset(&mut self) -> McapResult<()> {
        match &mut self.kind {
            DecoderKind::Xor(frame) => *frame = XorFrame::default(),
            DecoderKind::WholeFrame { frame, hint, .. } => {
                *frame = XorFrame::default();
                *hint = None;
            }
            DecoderKind::Stall
            | DecoderKind::Fail
            | DecoderKind::OverConsume
            | DecoderKind::OverWrite
            | DecoderKind::Greedy => {}
        }
        if self.fail_reset {
            return Err(McapError::DecompressionError("reset failed".into()));
        }
        Ok(())
    }

    fn name(&self) -> &'static str {
        self.name
    }
}

fn xor_decompress(
    frame: &mut XorFrame,
    src: &[u8],
    dst: &mut [u8],
) -> McapResult<DecompressResult> {
    if src.is_empty() {
        return Ok(DecompressResult {
            consumed: 0,
            wrote: 0,
        });
    }
    match frame.remaining {
        Some(0) => Err(McapError::DecompressionError(
            "padding was fed to a finished frame".into(),
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
                *out = input ^ XOR_KEY;
            }
            frame.remaining = Some(left - take);
            Ok(DecompressResult {
                consumed: take,
                wrote: take,
            })
        }
        None => {
            let filled = frame.len_filled;
            let take = (4 - filled).min(src.len());
            frame.len_buf[filled..filled + take].copy_from_slice(&src[..take]);
            frame.len_filled += take;
            if frame.len_filled == 4 {
                frame.remaining = Some(u32::from_le_bytes(frame.len_buf) as usize);
                frame.len_filled = 0;
            }
            Ok(DecompressResult {
                consumed: take,
                wrote: 0,
            })
        }
    }
}

#[cfg(feature = "zstd")]
struct FailingResetDecoder {
    inner: super::zstd::ZstdDecoder,
    poisoned: bool,
}

#[cfg(feature = "zstd")]
impl Decompressor for FailingResetDecoder {
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
    /// Bytes appended after the compressed frame.
    padding: usize,
    /// Overrides the uncompressed size declared in the chunk header and chunk index.
    declared_uncompressed_size: Option<u64>,
}

impl ChunkSpec {
    fn new(log_time: u64, payload: &'static [u8], padding: usize) -> Self {
        Self {
            log_time,
            payload,
            padding,
            declared_uncompressed_size: None,
        }
    }
}

struct BuiltChunk {
    log_time: u64,
    compression: String,
    uncompressed_size: u64,
    uncompressed_crc: u32,
    compressed: Vec<u8>,
}

/// Builds an indexed MCAP from chunks that are already compressed.
fn write_indexed_mcap(chunks: &[BuiltChunk]) -> Vec<u8> {
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

    let mut indexes = Vec::new();
    for chunk in chunks {
        let mut chunk_body = write_body(&ChunkHeader {
            message_start_time: chunk.log_time,
            message_end_time: chunk.log_time,
            uncompressed_size: chunk.uncompressed_size,
            uncompressed_crc: chunk.uncompressed_crc,
            compression: chunk.compression.clone(),
            compressed_size: chunk.compressed.len() as u64,
        });
        chunk_body.extend_from_slice(&chunk.compressed);
        let chunk_start = file.len() as u64;
        append_record(&mut file, op::CHUNK, &chunk_body);
        indexes.push(ChunkIndex {
            message_start_time: chunk.log_time,
            message_end_time: chunk.log_time,
            chunk_start_offset: chunk_start,
            chunk_length: file.len() as u64 - chunk_start,
            message_index_offsets: BTreeMap::new(),
            message_index_length: 0,
            compression: chunk.compression.clone(),
            compressed_size: chunk.compressed.len() as u64,
            uncompressed_size: chunk.uncompressed_size,
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

/// Builds an indexed MCAP whose chunks use the `"xor"` compression string.
fn xor_mcap(chunks: &[ChunkSpec]) -> Vec<u8> {
    build_xor_mcap(chunks, false)
}

/// Like [`xor_mcap`], but each chunk's `uncompressed_crc` is wrong.
fn xor_mcap_bad_crc(chunks: &[ChunkSpec]) -> Vec<u8> {
    build_xor_mcap(chunks, true)
}

fn build_xor_mcap(chunks: &[ChunkSpec], corrupt_crc: bool) -> Vec<u8> {
    let built = chunks
        .iter()
        .map(|chunk| {
            let mut message_body = write_body(&MessageHeader {
                channel_id: 1,
                sequence: 1,
                log_time: chunk.log_time,
                publish_time: chunk.log_time,
            });
            message_body.extend_from_slice(chunk.payload);
            let mut records = Vec::new();
            append_record(&mut records, op::MESSAGE, &message_body);
            let mut compressed = Vec::with_capacity(4 + records.len() + chunk.padding);
            compressed.extend_from_slice(&(records.len() as u32).to_le_bytes());
            compressed.extend(records.iter().map(|byte| byte ^ XOR_KEY));
            compressed.extend(std::iter::repeat(0xA5u8).take(chunk.padding));
            let crc = crc32fast::hash(&records);
            BuiltChunk {
                log_time: chunk.log_time,
                compression: "xor".into(),
                uncompressed_size: chunk
                    .declared_uncompressed_size
                    .unwrap_or(records.len() as u64),
                uncompressed_crc: if corrupt_crc { crc ^ 1 } else { crc },
                compressed,
            }
        })
        .collect::<Vec<_>>();
    write_indexed_mcap(&built)
}

/// Byte offset of the first chunk record's length field, and the end of its body.
fn chunk_record_span(mcap: &[u8]) -> (usize, usize) {
    let mut offset = MAGIC.len();
    while offset + 9 <= mcap.len() {
        let opcode = mcap[offset];
        let len = u64::from_le_bytes(mcap[offset + 1..offset + 9].try_into().unwrap()) as usize;
        if opcode == op::CHUNK {
            return (offset + 1, offset + 9 + len);
        }
        offset += 9 + len;
    }
    panic!("file has no chunk record");
}

/// Byte length of the first chunk's header, up to and including `compressed_size`.
fn chunk_header_len(mcap: &[u8]) -> usize {
    let (len_at, _) = chunk_record_span(mcap);
    // start time, end time, uncompressed size, uncompressed CRC
    let compression_len_at = len_at + 8 + 28;
    let compression_len = u32::from_le_bytes(
        mcap[compression_len_at..compression_len_at + 4]
            .try_into()
            .unwrap(),
    );
    32 + compression_len as usize + 8
}

fn sample_chunks() -> Vec<ChunkSpec> {
    vec![ChunkSpec::new(10, b"one", 0), ChunkSpec::new(20, b"two", 3)]
}

fn read_linear(
    mcap: &[u8],
    decompressor: Option<TestDecoder>,
) -> McapResult<Vec<(u16, u64, Vec<u8>)>> {
    read_linear_with_options(mcap, decompressor, LinearReaderOptions::default())
}

fn read_linear_with_options(
    mcap: &[u8],
    decompressor: Option<TestDecoder>,
    options: LinearReaderOptions,
) -> McapResult<Vec<(u16, u64, Vec<u8>)>> {
    let mut reader = LinearReader::new_with_options(options);
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

/// Reads `mcap` with `decompressor`, returning every `ReadRequest` size and the final result.
fn linear_read_requests(
    mcap: &[u8],
    decompressor: TestDecoder,
    options: LinearReaderOptions,
) -> (Vec<usize>, McapResult<()>) {
    let mut reader = LinearReader::new_with_options(options);
    reader.add_decompressor(decompressor).expect("register");
    let mut cursor = Cursor::new(mcap);
    let mut requests = Vec::new();
    let mut iterations = 0;
    while let Some(event) = reader.next_event() {
        iterations += 1;
        assert!(iterations < 100_000, "linear reader did not finish");
        match event {
            Ok(LinearReadEvent::ReadRequest(need)) => {
                requests.push(need);
                let read = cursor.read(reader.insert(need)).expect("read");
                reader.notify_read(read);
            }
            Ok(LinearReadEvent::Record { .. }) => {}
            Err(err) => return (requests, Err(err)),
        }
    }
    (requests, Ok(()))
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

fn one_message_mcap(compression: Option<crate::Compression>) -> Vec<u8> {
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

#[cfg(feature = "zstd")]
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

fn assert_caller_decompressor_ran(err: McapError) {
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
fn add_decompressor_rejects_a_name_held_by_the_open_chunk() {
    let mcap = xor_mcap(&sample_chunks());
    let mut reader = LinearReader::new();
    reader
        .add_decompressor(TestDecoder::xor("xor"))
        .expect("register");
    let mut cursor = Cursor::new(mcap.as_slice());
    let mut saw_message = false;
    while let Some(event) = reader.next_event() {
        match event.expect("read") {
            LinearReadEvent::ReadRequest(need) => {
                let read = cursor.read(reader.insert(need)).expect("read");
                reader.notify_read(read);
            }
            LinearReadEvent::Record { opcode, .. } if opcode == op::MESSAGE => {
                saw_message = true;
                break;
            }
            LinearReadEvent::Record { .. } => {}
        }
    }
    assert!(saw_message, "expected to be inside a chunk");
    assert!(matches!(
        reader.add_decompressor(TestDecoder::xor("xor")),
        Err(McapError::DuplicateDecompressor(name)) if name == "xor"
    ));
}

#[test]
fn chunk_header_error_keeps_the_registered_decompressor() {
    let mut mcap = xor_mcap(&[ChunkSpec::new(10, b"one", 0)]);
    let (len_at, _) = chunk_record_span(&mcap);
    let len = u64::from_le_bytes(mcap[len_at..len_at + 8].try_into().unwrap());
    mcap[len_at..len_at + 8].copy_from_slice(&(len + 10_000).to_le_bytes());

    let mut reader = LinearReader::new_with_options(
        LinearReaderOptions::default().with_record_length_limit(128),
    );
    reader
        .add_decompressor(TestDecoder::xor("xor"))
        .expect("register");
    let mut cursor = Cursor::new(mcap.as_slice());
    let mut iterations = 0;
    let err = loop {
        iterations += 1;
        assert!(iterations < 100_000, "linear reader did not finish");
        match reader.next_event().expect("header length is known") {
            Ok(LinearReadEvent::ReadRequest(need)) => {
                let read = cursor.read(reader.insert(need)).expect("read");
                reader.notify_read(read);
            }
            Ok(LinearReadEvent::Record { .. }) => {}
            Err(err) => break err,
        }
    };
    assert!(
        matches!(err, McapError::ChunkTooLarge(_)),
        "unexpected error: {err}"
    );
    assert!(
        matches!(
            reader.add_decompressor(TestDecoder::xor("xor")),
            Err(McapError::DuplicateDecompressor(name)) if name == "xor"
        ),
        "the header error dropped the registered decompressor"
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
fn streaming_message_readers_forward_the_decompressor() {
    let mcap = xor_mcap(&sample_chunks());
    let expected = vec![(10, b"one".to_vec()), (20, b"two".to_vec())];

    let mut sans_io = crate::sans_io::MessageReader::new();
    sans_io
        .add_decompressor(TestDecoder::xor("xor"))
        .expect("sans-io register");
    let mut cursor = Cursor::new(mcap.as_slice());
    let mut sans_io_messages = Vec::new();
    let mut iterations = 0;
    while let Some(event) = sans_io.next_event() {
        iterations += 1;
        assert!(iterations < 100_000, "message reader did not finish");
        match event.expect("sans-io message") {
            crate::sans_io::MessageReadEvent::ReadRequest(need) => {
                let read = cursor.read(sans_io.insert(need)).expect("read");
                sans_io.notify_read(read);
            }
            crate::sans_io::MessageReadEvent::Message(message) => {
                sans_io_messages.push((message.log_time, message.data.into_owned()));
            }
        }
    }
    assert_eq!(sans_io_messages, expected);

    let mut blocking = crate::io::MessageReader::new(Cursor::new(mcap));
    blocking
        .add_decompressor(TestDecoder::xor("xor"))
        .expect("io register");
    let blocking_messages = blocking
        .map(|message| {
            let message = message.expect("io message");
            (message.log_time, message.data.into_owned())
        })
        .collect::<Vec<_>>();
    assert_eq!(blocking_messages, expected);
}

#[cfg(feature = "tokio")]
#[tokio::test]
async fn tokio_linear_reader_forwards_the_decompressor() {
    let mcap = xor_mcap(&sample_chunks());
    let mut reader = crate::tokio::LinearReader::new(Cursor::new(mcap));
    reader
        .add_decompressor(TestDecoder::xor("xor"))
        .expect("tokio register");
    let mut buf = Vec::new();
    let mut messages = 0;
    while let Some(opcode) = reader.next_record(&mut buf).await {
        if opcode.expect("record") == op::MESSAGE {
            messages += 1;
        }
    }
    assert_eq!(messages, 2);
}

#[test]
fn registered_decompressor_replaces_builtin() {
    let builtins: &[(crate::Compression, &'static str)] = &[
        #[cfg(feature = "zstd")]
        (crate::Compression::Zstd, "zstd"),
        #[cfg(feature = "lz4")]
        (crate::Compression::Lz4, "lz4"),
    ];
    for &(compression, name) in builtins {
        let mcap = one_message_mcap(Some(compression));
        assert_caller_decompressor_ran(
            read_linear(&mcap, Some(TestDecoder::fail(name)))
                .expect_err("linear should use caller"),
        );
        assert_caller_decompressor_ran(
            read_indexed(&mcap, Some(TestDecoder::fail(name)))
                .expect_err("indexed should use caller"),
        );
    }
}

#[cfg(feature = "zstd")]
#[test]
fn builtin_cache_does_not_count_as_a_caller_registration() {
    let mcap = two_chunk_mcap(Some(crate::Compression::Zstd));
    let mut reader = LinearReader::new();
    let mut cursor = Cursor::new(&mcap);
    let mut messages = 0;
    let mut iterations = 0;
    let err = loop {
        iterations += 1;
        assert!(iterations < 100_000, "linear reader did not finish");
        match reader.next_event().expect("the second chunk should fail") {
            Ok(LinearReadEvent::ReadRequest(need)) => {
                let read = cursor.read(reader.insert(need)).expect("read");
                reader.notify_read(read);
            }
            Ok(LinearReadEvent::Record { opcode, .. }) => {
                if opcode == op::MESSAGE {
                    messages += 1;
                    if messages == 1 {
                        // The built-in zstd decoder is decoding the first chunk at this point.
                        reader
                            .add_decompressor(TestDecoder::fail("zstd"))
                            .expect("a built-in decoder is not a caller registration");
                    }
                }
            }
            Err(err) => break err,
        }
    };
    assert_eq!(messages, 1, "the second chunk should not decode");
    assert_caller_decompressor_ran(err);
}

#[cfg(feature = "zstd")]
#[test]
fn registered_decompressor_reads_multi_frame_chunks() {
    let mut message_body = write_body(&MessageHeader {
        channel_id: 1,
        sequence: 1,
        log_time: 7,
        publish_time: 7,
    });
    message_body.extend_from_slice(b"multi-frame payload");
    let mut records = Vec::new();
    append_record(&mut records, op::MESSAGE, &message_body);
    let (head, tail) = records.split_at(records.len() / 2);
    let encode = |bytes: &[u8]| zstd::encode_all(Cursor::new(bytes), 0).expect("encode");
    // A zstd skippable frame: magic number, content length, then that many bytes.
    let mut skippable = 0x184D_2A50u32.to_le_bytes().to_vec();
    skippable.extend_from_slice(&4u32.to_le_bytes());
    skippable.extend_from_slice(&[1, 2, 3, 4]);
    let cases = [
        ("concatenated frames", [encode(head), encode(tail)].concat()),
        (
            "skippable frame then data frame",
            [skippable, encode(&records)].concat(),
        ),
    ];

    for (label, compressed) in cases {
        let mcap = write_indexed_mcap(&[BuiltChunk {
            log_time: 7,
            compression: "zstd".into(),
            uncompressed_size: records.len() as u64,
            uncompressed_crc: 0,
            compressed,
        }]);

        // One-byte reads make the linear reader cross frame boundaries between reads.
        for read_size in [usize::MAX, 1] {
            let mut reader = LinearReader::new();
            reader
                .add_decompressor(super::zstd::ZstdDecoder::new())
                .expect("register");
            let mut cursor = Cursor::new(mcap.as_slice());
            let mut messages = 0;
            while let Some(event) = reader.next_event() {
                match event.unwrap_or_else(|err| panic!("{label}, linear: {err}")) {
                    LinearReadEvent::ReadRequest(need) => {
                        let read = cursor
                            .read(reader.insert(need.min(read_size)))
                            .expect("read");
                        reader.notify_read(read);
                    }
                    LinearReadEvent::Record { opcode, .. } if opcode == op::MESSAGE => {
                        messages += 1;
                    }
                    LinearReadEvent::Record { .. } => {}
                }
            }
            assert_eq!(messages, 1, "{label}, linear, read size {read_size}");
        }

        let summary = Summary::read(&mcap)
            .expect("summary read")
            .expect("summary");
        let mut reader = IndexedReader::new(&summary).expect("reader");
        reader
            .add_decompressor(super::zstd::ZstdDecoder::new())
            .expect("register");
        let mut messages = 0;
        while let Some(event) = reader.next_event() {
            match event.unwrap_or_else(|err| panic!("{label}, indexed: {err}")) {
                IndexedReadEvent::ReadChunkRequest { offset, length } => {
                    let start = offset as usize;
                    reader
                        .insert_chunk_record_data(offset, &mcap[start..start + length])
                        .unwrap_or_else(|err| panic!("{label}, indexed: {err}"));
                }
                IndexedReadEvent::Message { .. } => messages += 1,
            }
        }
        assert_eq!(messages, 1, "{label}, indexed");
    }
}

#[test]
fn over_reported_buffer_lengths_are_errors() {
    let mcap = xor_mcap(&sample_chunks());
    for decoder in [TestDecoder::over_consume, TestDecoder::over_write] {
        let linear = read_linear(&mcap, Some(decoder("xor")))
            .expect_err("linear should reject an over-reported result");
        let indexed = read_indexed(&mcap, Some(decoder("xor")))
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
fn greedy_output_past_the_declared_size_is_an_error() {
    let mut chunks = vec![ChunkSpec::new(10, b"one", 0)];
    chunks[0].declared_uncompressed_size = Some(4);
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
    // The output buffer is capped at the declared size, so the extra bytes the decoder would
    // have written never land. The 4-byte buffer is not a valid record.
    assert!(matches!(
        read_indexed(&mcap, Some(TestDecoder::greedy("xor"))),
        Err(McapError::UnexpectedEoc)
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
fn zero_read_size_stall_errors_after_the_chunk_is_buffered() {
    let mcap = xor_mcap(&sample_chunks());
    assert_no_progress(
        read_linear(&mcap, Some(TestDecoder::stall("xor").with_read_size(0)))
            .expect_err("a hint of 0 must not loop forever"),
    );
}

#[test]
fn no_progress_buffers_more_input_until_the_frame_fits() {
    let expected = vec![(1, 10, b"one".to_vec()), (1, 20, b"two".to_vec())];
    for decoder in [TestDecoder::whole_frame, TestDecoder::whole_frame_with_hint] {
        assert_eq!(
            read_linear(&xor_mcap(&sample_chunks()), Some(decoder("xor"))).expect("linear"),
            expected
        );
    }

    let mcap = xor_mcap(&[ChunkSpec::new(10, &[7; 4096], 0)]);
    let request_count = |decoder| {
        let (requests, result) =
            linear_read_requests(&mcap, decoder, LinearReaderOptions::default());
        result.expect("read");
        requests.len()
    };
    let doubled = request_count(TestDecoder::whole_frame("xor"));
    let hinted = request_count(TestDecoder::whole_frame_with_hint("xor"));
    assert!(
        doubled < 40,
        "no progress should grow the buffer, not add one byte per call: {doubled} requests"
    );
    assert!(
        hinted < doubled,
        "a raised next_read_size should be fetched at once: {hinted} vs {doubled} requests"
    );
}

#[test]
fn zero_read_size_still_decompresses_the_chunk() {
    let mcap = xor_mcap(&sample_chunks());
    let expected = vec![(1, 10, b"one".to_vec()), (1, 20, b"two".to_vec())];
    assert_eq!(
        read_linear(&mcap, Some(TestDecoder::xor("xor").with_read_size(0))).expect("linear"),
        expected
    );
}

#[test]
fn zero_read_size_reads_compressed_data_in_blocks() {
    let mcap = xor_mcap(&[ChunkSpec::new(10, &[7; 4096], 0)]);
    let (requests, result) = linear_read_requests(
        &mcap,
        TestDecoder::xor("xor").with_read_size(0),
        LinearReaderOptions::default(),
    );
    result.expect("read");
    assert!(
        requests.len() < 20,
        "a hint of 0 should not read one byte per call: {} requests",
        requests.len()
    );
}

#[test]
fn large_read_size_is_capped_by_the_record_length_limit() {
    let mut mcap = xor_mcap(&[ChunkSpec::new(10, b"one", 0)]);
    let (len_at, _) = chunk_record_span(&mcap);
    let header_len = chunk_header_len(&mcap);
    let compressed_size_at = len_at + 8 + header_len - 8;
    let huge = 64u64 << 30;
    mcap[len_at..len_at + 8].copy_from_slice(&huge.to_le_bytes());
    mcap[compressed_size_at..compressed_size_at + 8]
        .copy_from_slice(&(huge - header_len as u64).to_le_bytes());

    let limit = 1 << 16;
    let (requests, result) = linear_read_requests(
        &mcap,
        TestDecoder::xor("xor").with_read_size(usize::MAX),
        LinearReaderOptions::default().with_record_length_limit(limit),
    );
    assert!(
        matches!(result, Err(McapError::UnexpectedEof)),
        "the capped read should run off the end of the file: {result:?}"
    );
    assert!(
        requests.iter().all(|&need| need <= limit),
        "requests exceeded the record length limit: {requests:?}"
    );
}

#[test]
fn stall_at_the_record_length_limit_is_chunk_too_large() {
    let mcap = xor_mcap(&[ChunkSpec::new(10, &[7; 4096], 0)]);
    let (len_at, _) = chunk_record_span(&mcap);
    let data_start = len_at + 8 + chunk_header_len(&mcap);
    // Not a power of two, so doubling from a one-byte hint overshoots it.
    let limit = 1000;
    for read_size in [0, 1] {
        let (requests, result) = linear_read_requests(
            &mcap,
            TestDecoder::stall("xor").with_read_size(read_size),
            LinearReaderOptions::default().with_record_length_limit(limit),
        );
        assert!(
            matches!(result, Err(McapError::ChunkTooLarge(_))),
            "read size {read_size}: {result:?}"
        );
        let requested: usize = requests.iter().sum();
        assert!(
            requested <= data_start + limit,
            "read size {read_size} buffered past the limit: {requested} bytes requested, \
             chunk data starts at {data_start}"
        );
    }
}

#[cfg(feature = "zstd")]
#[test]
fn builtin_zstd_reads_with_a_small_record_length_limit() {
    let channel = std::sync::Arc::new(crate::Channel {
        id: 1,
        topic: "topic".into(),
        schema: None,
        message_encoding: "raw".into(),
        metadata: BTreeMap::new(),
    });
    let mut writer = crate::WriteOptions::new()
        .compression(Some(crate::Compression::Zstd))
        .chunk_size(None)
        .create(Cursor::new(Vec::new()))
        .expect("writer");
    // Incompressible payloads so the one chunk's compressed size is far above the limit.
    let mut state = 0x9E37_79B9u32;
    for sequence in 0..10 {
        let payload: Vec<u8> = (0..100)
            .map(|_| {
                state = state.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
                (state >> 24) as u8
            })
            .collect();
        writer
            .write(&crate::Message {
                channel: channel.clone(),
                sequence,
                log_time: sequence.into(),
                publish_time: sequence.into(),
                data: payload.into(),
            })
            .expect("write");
    }
    writer.finish().expect("finish");
    let mcap = writer.into_inner().into_inner();

    let limit = 256;
    let mut reader = LinearReader::new_with_options(
        LinearReaderOptions::default().with_record_length_limit(limit),
    );
    let mut cursor = Cursor::new(mcap.as_slice());
    let mut messages = 0;
    let mut largest_request = 0;
    while let Some(event) = reader.next_event() {
        match event.expect("a small limit only bounds buffering") {
            LinearReadEvent::ReadRequest(need) => {
                largest_request = largest_request.max(need);
                let read = cursor.read(reader.insert(need)).expect("read");
                reader.notify_read(read);
            }
            LinearReadEvent::Record { opcode, .. } if opcode == op::MESSAGE => messages += 1,
            LinearReadEvent::Record { .. } => {}
        }
    }
    assert_eq!(messages, 10);
    assert!(
        largest_request <= limit,
        "requested {largest_request} bytes with a {limit}-byte limit"
    );
}

#[cfg(feature = "zstd")]
#[test]
fn failed_reset_does_not_fall_back_to_the_builtin_decoder() {
    let mcap = two_chunk_mcap(Some(crate::Compression::Zstd));
    let mut reader = LinearReader::new();
    reader
        .add_decompressor(FailingResetDecoder {
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

#[test]
fn indexed_reader_reports_a_failed_reset() {
    let mcap = xor_mcap(&sample_chunks());
    let err = read_indexed(&mcap, Some(TestDecoder::xor("xor").with_failing_reset()))
        .expect_err("reset failure should be returned");
    assert!(
        err.to_string().contains("reset failed"),
        "unexpected error: {err}"
    );
    assert_caller_decompressor_ran(
        read_indexed(&mcap, Some(TestDecoder::fail("xor").with_failing_reset()))
            .expect_err("a decompression error should win over the reset error"),
    );
}

#[test]
fn custom_decompressor_checks_a_real_chunk_crc() {
    let chunks = [ChunkSpec::new(10, b"one", 0)];
    let good = xor_mcap(&chunks);
    let expected = vec![(1, 10, b"one".to_vec())];
    let modes = [
        LinearReaderOptions::default().with_validate_chunk_crcs(true),
        LinearReaderOptions::default().with_prevalidate_chunk_crcs(true),
    ];
    for options in modes.clone() {
        assert_eq!(
            read_linear_with_options(&good, Some(TestDecoder::xor("xor")), options).expect("crc"),
            expected
        );
    }

    let bad = xor_mcap_bad_crc(&chunks);
    for options in modes {
        let result = read_linear_with_options(&bad, Some(TestDecoder::xor("xor")), options);
        assert!(
            matches!(result, Err(McapError::BadChunkCrc { .. })),
            "a bad chunk CRC should be rejected, got {result:?}"
        );
    }
}

#[test]
fn bad_chunk_crc_is_reported_when_reset_fails() {
    let mut mcap = xor_mcap_bad_crc(&[ChunkSpec::new(10, b"one", 4)]);
    let (len_at, body_end) = chunk_record_span(&mcap);
    let len = u64::from_le_bytes(mcap[len_at..len_at + 8].try_into().unwrap());
    mcap[len_at..len_at + 8].copy_from_slice(&(len + 8).to_le_bytes());
    mcap.splice(body_end..body_end, std::iter::repeat(0xFF).take(8));

    let mut reader = LinearReader::new_with_options(
        LinearReaderOptions::default().with_validate_chunk_crcs(true),
    );
    reader
        .add_decompressor(TestDecoder::xor("xor").with_failing_reset())
        .expect("register");
    let mut cursor = Cursor::new(mcap.as_slice());
    let mut messages = 0;
    let mut errors = Vec::new();
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
            Err(err) => errors.push(err),
        }
    }
    assert_eq!(messages, 1);
    assert!(
        matches!(errors.first(), Some(McapError::BadChunkCrc { .. })),
        "a failed reset must not hide the chunk CRC: {errors:?}"
    );
    assert_eq!(
        errors.len(),
        1,
        "reading should finish after the CRC error: {errors:?}"
    );
}

#[test]
fn chunk_reader_checks_an_uncompressed_chunk_crc() {
    let mcap = one_message_mcap(None);
    let (len_at, body_end) = chunk_record_span(&mcap);
    let body = &mcap[len_at + 8..body_end];
    let mut cursor = Cursor::new(body);
    let header = ChunkHeader::read_le(&mut cursor).expect("chunk header");
    assert!(header.compression.is_empty());
    assert_ne!(header.uncompressed_crc, 0);
    let data = &body[cursor.position() as usize..];
    let records = crate::read::ChunkReader::new(header.clone(), data)
        .expect("chunk reader")
        .collect::<McapResult<Vec<_>>>()
        .expect("uncompressed chunk with a real CRC");
    assert!(records
        .iter()
        .any(|record| matches!(record, Record::Message { .. })));

    let mut bad_header = header;
    bad_header.uncompressed_crc ^= 1;
    let result = crate::read::ChunkReader::new(bad_header, data)
        .expect("chunk reader")
        .collect::<McapResult<Vec<_>>>();
    assert!(
        matches!(result, Err(McapError::BadChunkCrc { .. })),
        "a bad uncompressed chunk CRC must be rejected, got {result:?}"
    );
}
