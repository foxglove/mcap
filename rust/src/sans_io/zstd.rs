use crate::{
    sans_io::read::{DecompressResult, Decompressor},
    McapError, McapResult,
};
use zstd::zstd_safe::{get_error_name, DStream, InBuffer, OutBuffer, SafeResult};

pub struct ZstdDecoder {
    s: DStream<'static>,
    next_size_hint: usize,
}

impl ZstdDecoder {
    pub fn new() -> Self {
        ZstdDecoder {
            s: DStream::create(),
            next_size_hint: DStream::in_size(),
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
    fn decompress(&mut self, src: &[u8], dst: &mut [u8]) -> crate::McapResult<DecompressResult> {
        let mut in_buffer = InBuffer::around(src);
        let mut out_buffer = OutBuffer::around(dst);
        let need = handle_error(self.s.decompress_stream(&mut out_buffer, &mut in_buffer))?;
        self.next_size_hint = need;
        Ok(DecompressResult {
            consumed: in_buffer.pos,
            wrote: out_buffer.pos(),
            need,
        })
    }
    fn reset(&mut self) -> McapResult<()> {
        handle_error(self.s.reset())?;
        Ok(())
    }
    fn next_size_hint(&self) -> usize {
        return self.next_size_hint;
    }
}
