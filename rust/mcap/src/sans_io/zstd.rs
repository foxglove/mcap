//! The built-in zstd chunk decompressor.
use crate::{
    sans_io::decompressor::{stream_chunk, DecompressResult, Decompressor},
    McapError, McapResult,
};
use zstd::zstd_safe::{
    find_frame_compressed_size, get_error_name, DParameter, DStream, InBuffer, OutBuffer,
    ResetDirective, SafeResult,
};

/// The largest window a frame may declare, as a power of two: zstd's own default of 128 MiB.
///
/// The streaming decoder enforces it to bound its buffer, and it is set on the context rather
/// than left at the library default so [`windows_fit`] applies exactly the same limit.
const WINDOW_LOG_LIMIT: u32 = 27;

/// The built-in [`Decompressor`] for `"zstd"` chunks.
///
/// Readers register one of these by default when the `zstd` feature is enabled. Constructing it
/// allocates nothing; the zstd decoding context is created on the first
/// [`decompress`](Decompressor::decompress) or
/// [`decompress_chunk`](Decompressor::decompress_chunk) call. The read hint is 0, meaning any
/// amount, before the first `decompress` call and after each [`reset`](Decompressor::reset).
#[derive(Default)]
pub struct ZstdDecoder {
    stream: Option<DStream<'static>>,
    need: usize,
}

impl ZstdDecoder {
    pub fn new() -> Self {
        Self::default()
    }

    fn stream(&mut self) -> McapResult<&mut DStream<'static>> {
        if self.stream.is_none() {
            let mut stream = DStream::create();
            handle_error(stream.init())?;
            handle_error(stream.set_parameter(DParameter::WindowLogMax(WINDOW_LOG_LIMIT)))?;
            self.stream = Some(stream);
        }
        Ok(self.stream.as_mut().expect("stream was just created"))
    }
}

fn handle_error(res: SafeResult) -> McapResult<usize> {
    match res {
        Ok(n) => Ok(n),
        Err(code) => Err(McapError::DecompressionError(get_error_name(code).into())),
    }
}

impl Decompressor for ZstdDecoder {
    fn next_read_size(&self) -> usize {
        self.need
    }

    fn decompress(&mut self, src: &[u8], dst: &mut [u8]) -> McapResult<DecompressResult> {
        let stream = self.stream()?;
        let mut in_buffer = InBuffer::around(src);
        let mut out_buffer = OutBuffer::around(dst);
        let need = handle_error(stream.decompress_stream(&mut out_buffer, &mut in_buffer))?;
        self.need = need;
        Ok(DecompressResult {
            consumed: in_buffer.pos,
            wrote: out_buffer.pos(),
        })
    }

    fn decompress_chunk(&mut self, src: &[u8], dst: &mut [u8]) -> McapResult<()> {
        // ZSTD_decompressDCtx writes every block straight into dst. The streaming call only
        // does that when the frame header carries the content size; otherwise it decodes each
        // block into the context's own buffer and copies it out, and frames compressed
        // incrementally, as this crate's writer does, never carry the size. The one-shot call
        // also fails on a frame larger than dst or with a bad checksum, where streaming stops
        // at the end of dst instead, so anything but a full dst is decoded again by streaming,
        // which reports what it finds. The one-shot call also ignores the window limit that
        // streaming enforces, so a chunk with a larger window goes straight to streaming and
        // fails the way it does in the linear reader.
        let stream = self.stream()?;
        if windows_fit(src) && stream.decompress(dst, src) == Ok(dst.len()) {
            return Ok(());
        }
        self.reset()?;
        stream_chunk(self, src, dst)
    }

    fn reset(&mut self) -> McapResult<()> {
        self.need = 0;
        if let Some(stream) = self.stream.as_mut() {
            handle_error(stream.reset(ResetDirective::SessionOnly))?;
        }
        Ok(())
    }

    fn name(&self) -> &'static str {
        "zstd"
    }
}

/// Whether every frame in `src` declares a window within [`WINDOW_LOG_LIMIT`].
///
/// Returns false for anything that does not parse as a sequence of frames, leaving the error
/// to the streaming decoder.
fn windows_fit(mut src: &[u8]) -> bool {
    while !src.is_empty() {
        match frame_window_size(src) {
            Some(window) if window <= 1 << WINDOW_LOG_LIMIT => {}
            _ => return false,
        }
        match find_frame_compressed_size(src) {
            Ok(len) if len > 0 && len <= src.len() => src = &src[len..],
            _ => return false,
        }
    }
    true
}

/// The window the frame at the start of `src` declares, read from its header as RFC 8878
/// section 3.1.1.1 lays it out, or 0 for a skippable frame. None when `src` does not start with
/// a complete frame header.
fn frame_window_size(src: &[u8]) -> Option<u64> {
    const FRAME_MAGIC: u32 = 0xFD2F_B528;
    const SKIPPABLE_MAGIC: u32 = 0x184D_2A50;
    const SKIPPABLE_MAGIC_MASK: u32 = 0xFFFF_FFF0;
    let magic = u32::from_le_bytes(src.get(..4)?.try_into().ok()?);
    if magic & SKIPPABLE_MAGIC_MASK == SKIPPABLE_MAGIC {
        return Some(0);
    }
    if magic != FRAME_MAGIC {
        return None;
    }
    let descriptor = *src.get(4)?;
    let single_segment = descriptor & 0x20 != 0;
    if !single_segment {
        let window = *src.get(5)?;
        let base = 1u64 << (10 + (window >> 3));
        return Some(base + (base / 8) * u64::from(window & 7));
    }
    // A single-segment frame has no window descriptor: its window is its content size, which
    // follows the dictionary ID.
    let dictionary_id_len = [0, 1, 2, 4][usize::from(descriptor & 3)];
    let size = src.get(5 + dictionary_id_len..)?;
    Some(match descriptor >> 6 {
        0 => u64::from(*size.first()?),
        1 => u64::from(u16::from_le_bytes(size.get(..2)?.try_into().ok()?)) + 256,
        2 => u64::from(u32::from_le_bytes(size.get(..4)?.try_into().ok()?)),
        _ => u64::from_le_bytes(size.get(..8)?.try_into().ok()?),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn frame_windows_are_read_from_the_header() {
        let data = vec![7u8; 4000];
        // Streamed without a size, so the frame carries a window descriptor.
        let streamed = zstd::stream::encode_all(Cursor::new(&data[..]), 0).unwrap();
        assert_eq!(streamed[4] & 0x20, 0, "expected a window descriptor");
        let window = streamed[5];
        let base = 1u64 << (10 + (window >> 3));
        assert_eq!(
            frame_window_size(&streamed),
            Some(base + (base / 8) * u64::from(window & 7))
        );
        assert!(windows_fit(&streamed));

        // Compressed with a known size, so the frame is single-segment and its window is its
        // content size.
        let bulk = zstd::bulk::compress(&data, 0).unwrap();
        assert_ne!(bulk[4] & 0x20, 0, "expected a single-segment frame");
        assert_eq!(frame_window_size(&bulk), Some(data.len() as u64));

        // A skippable frame followed by a data frame.
        let mut skippable = 0x184D_2A5Fu32.to_le_bytes().to_vec();
        skippable.extend_from_slice(&3u32.to_le_bytes());
        skippable.extend_from_slice(b"abc");
        assert_eq!(frame_window_size(&skippable), Some(0));
        assert!(windows_fit(&[skippable, streamed.clone()].concat()));

        // The window descriptor for 2^27 fits and the next one up does not.
        let mut at_limit = streamed.clone();
        at_limit[5] = (WINDOW_LOG_LIMIT as u8 - 10) << 3;
        assert!(windows_fit(&at_limit));
        at_limit[5] |= 1;
        assert!(!windows_fit(&at_limit));

        assert_eq!(frame_window_size(&streamed[..5]), None);
        assert!(!windows_fit(b"not a frame"));
        assert!(!windows_fit(&streamed[..streamed.len() - 1]));
    }
}
