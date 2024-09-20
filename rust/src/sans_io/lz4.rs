use crate::sans_io::read::{DecompressResult, Decompressor};
use crate::McapResult;
use std::ptr;

use lz4::liblz4::{
    check_error, LZ4FDecompressionContext, LZ4F_createDecompressionContext, LZ4F_decompress,
    LZ4F_freeDecompressionContext, LZ4F_resetDecompressionContext, LZ4F_VERSION,
};

#[derive(Debug)]
pub struct Lz4Decoder {
    c: LZ4FDecompressionContext,
    next_size_hint: usize,
}

impl Lz4Decoder {
    pub fn new() -> McapResult<Lz4Decoder> {
        let mut context = LZ4FDecompressionContext(ptr::null_mut());
        check_error(unsafe { LZ4F_createDecompressionContext(&mut context, LZ4F_VERSION) })?;
        Ok(Lz4Decoder {
            c: context,
            next_size_hint: 11,
        })
    }
}

impl Drop for Lz4Decoder {
    fn drop(&mut self) {
        unsafe { LZ4F_freeDecompressionContext(self.c) };
    }
}

impl Decompressor for Lz4Decoder {
    fn decompress(&mut self, src: &[u8], dst: &mut [u8]) -> McapResult<DecompressResult> {
        let mut dst_size = dst.len();
        let mut src_size = src.len();
        let need = check_error(unsafe {
            LZ4F_decompress(
                self.c,
                dst.as_mut_ptr(),
                &mut dst_size,
                src.as_ptr(),
                &mut src_size,
                ptr::null(),
            )
        })?;
        self.next_size_hint = need;
        return Ok(DecompressResult {
            consumed: src_size,
            wrote: dst_size,
            need,
        });
    }

    fn reset(&mut self) -> McapResult<()> {
        unsafe { LZ4F_resetDecompressionContext(self.c) };
        Ok(())
    }
}
