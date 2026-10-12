//! The built-in lz4 chunk decompressor.
use crate::sans_io::decompressor::{DecompressResult, Decompressor};
use crate::McapResult;
use std::ptr;

use lz4::liblz4::{
    check_error, LZ4FDecompressionContext, LZ4F_createDecompressionContext, LZ4F_decompress,
    LZ4F_freeDecompressionContext, LZ4F_resetDecompressionContext, LZ4F_VERSION,
};

/// The smallest possible lz4 frame, used as the read hint before the first call and after each
/// reset.
const MIN_FRAME_SIZE: usize = 13;

/// The built-in [`Decompressor`] for `"lz4"` chunks.
///
/// Readers register one of these by default when the `lz4` feature is enabled. Constructing it
/// allocates nothing; the lz4 frame decoding context is created on the first
/// [`decompress`](Decompressor::decompress) call.
#[derive(Debug)]
pub struct Lz4Decoder {
    // Null until the first decompress call.
    context: LZ4FDecompressionContext,
    next_read_size: usize,
}

impl Default for Lz4Decoder {
    fn default() -> Self {
        Self::new()
    }
}

impl Lz4Decoder {
    pub fn new() -> Self {
        Lz4Decoder {
            context: LZ4FDecompressionContext(ptr::null_mut()),
            next_read_size: MIN_FRAME_SIZE,
        }
    }

    fn context(&mut self) -> McapResult<LZ4FDecompressionContext> {
        if self.context.0.is_null() {
            let mut context = LZ4FDecompressionContext(ptr::null_mut());
            check_error(unsafe { LZ4F_createDecompressionContext(&mut context, LZ4F_VERSION) })?;
            self.context = context;
        }
        Ok(self.context)
    }
}

impl Drop for Lz4Decoder {
    fn drop(&mut self) {
        if !self.context.0.is_null() {
            unsafe { LZ4F_freeDecompressionContext(self.context) };
        }
    }
}

impl Decompressor for Lz4Decoder {
    fn next_read_size(&self) -> usize {
        self.next_read_size
    }

    fn decompress(&mut self, src: &[u8], dst: &mut [u8]) -> McapResult<DecompressResult> {
        let context = self.context()?;
        let mut dst_size = dst.len();
        let mut src_size = src.len();
        let need = check_error(unsafe {
            LZ4F_decompress(
                context,
                dst.as_mut_ptr(),
                &mut dst_size,
                src.as_ptr(),
                &mut src_size,
                ptr::null(),
            )
        })?;
        self.next_read_size = need;
        Ok(DecompressResult {
            consumed: src_size,
            wrote: dst_size,
        })
    }

    fn reset(&mut self) -> McapResult<()> {
        self.next_read_size = MIN_FRAME_SIZE;
        if !self.context.0.is_null() {
            unsafe { LZ4F_resetDecompressionContext(self.context) };
        }
        Ok(())
    }

    fn name(&self) -> &'static str {
        "lz4"
    }
}
