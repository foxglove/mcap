use crate::{
    sans_io::decompressor::{DecompressResult, Decompressor},
    McapError, McapResult,
};
use zstd::zstd_safe::{get_error_name, DStream, InBuffer, OutBuffer, ResetDirective, SafeResult};

pub struct ZstdDecoder {
    s: DStream<'static>,
    need: usize,
}

/// A Decompressor wrapper for Zstd streaming decompression.
impl ZstdDecoder {
    pub fn new() -> Self {
        let mut stream = DStream::create();
        ZstdDecoder {
            need: stream.init().expect("zstd decoder init failed"),
            s: stream,
        }
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
    fn decompress(&mut self, src: &[u8], dst: &mut [u8]) -> crate::McapResult<DecompressResult> {
        let mut in_buffer = InBuffer::around(src);
        let mut out_buffer = OutBuffer::around(dst);
        let need = handle_error(self.s.decompress_stream(&mut out_buffer, &mut in_buffer))?;
        self.need = need;
        Ok(DecompressResult {
            consumed: in_buffer.pos,
            wrote: out_buffer.pos(),
        })
    }
    fn reset(&mut self) -> McapResult<()> {
        handle_error(self.s.reset(ResetDirective::SessionOnly))?;
        Ok(())
    }

    fn name(&self) -> &'static str {
        "zstd"
    }
}
