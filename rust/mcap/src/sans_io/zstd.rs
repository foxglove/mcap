//! The built-in zstd chunk decompressor.
use crate::{
    sans_io::decompressor::{DecompressResult, Decompressor},
    McapError, McapResult,
};
use zstd::zstd_safe::{get_error_name, DStream, InBuffer, OutBuffer, ResetDirective, SafeResult};

/// The built-in [`Decompressor`] for `"zstd"` chunks.
///
/// Readers register one of these by default when the `zstd` feature is enabled. Constructing it
/// allocates nothing; the zstd decoding context is created on the first
/// [`decompress`](Decompressor::decompress) call. The read hint is 0, meaning any amount, before
/// that call and after each [`reset`](Decompressor::reset).
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
