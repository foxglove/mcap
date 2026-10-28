//! The built-in zstd chunk decompressor.
use crate::{
    sans_io::decompressor::{stream_chunk, DecompressResult, Decompressor},
    McapError, McapResult,
};
use zstd::zstd_safe::{get_error_name, DStream, InBuffer, OutBuffer, ResetDirective, SafeResult};

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
        // fails on bytes after the frame, a bad checksum or a frame of the wrong size, where
        // streaming stops at the end of dst instead, so anything but a full dst is decoded
        // again by streaming, which reports what it finds.
        //
        // The streaming decoder refuses a frame whose declared window is above 128 MiB, since
        // it would have to buffer that much. The one-shot call has no such buffer and accepts
        // the frame, and so does the streaming fallback when the frame also carries its
        // content size, since zstd then decodes it in one pass before checking the window. So
        // the indexed reader reads such chunks and the linear reader fails on them, as released
        // versions of both already do. Regular compression levels never produce these windows;
        // only long-distance mode or an explicit window size does.
        if self.stream()?.decompress(dst, src) == Ok(dst.len()) {
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
