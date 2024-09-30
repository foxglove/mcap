use crate::McapResult;
pub struct DecompressResult {
    /// The number of bytes consumed from the input buffer.
    pub consumed: usize,
    /// The number of bytes written to the output buffer.
    pub wrote: usize,
    /// A hint indicating how many bytes should be provided for the next call to completely fill
    /// the output buffer.
    pub need: usize,
}

/// A trait for streaming decompression.
pub trait Decompressor {
    /// Decompresses up to `dst.len()` bytes, consuming up to `src.len()` bytes from `src`.
    fn decompress(&mut self, src: &[u8], dst: &mut [u8]) -> McapResult<DecompressResult>;
    /// Resets the internal state of the decompressor.
    fn reset(&mut self) -> McapResult<()>;
    /// Returns the MCAP chunk compression string for the format that this Decompressor handles.
    fn name(&self) -> &'static str;
}

/// A trivial Decompressor for un-compressed chunk data.
pub struct NoneDecompressor {}

impl Decompressor for NoneDecompressor {
    fn decompress(&mut self, src: &[u8], dst: &mut [u8]) -> McapResult<DecompressResult> {
        let len = std::cmp::min(src.len(), dst.len());
        dst[..len].copy_from_slice(&src[..len]);
        Ok(DecompressResult {
            consumed: len,
            wrote: len,
            need: (dst.len() - len),
        })
    }
    fn reset(&mut self) -> McapResult<()> {
        Ok(())
    }

    fn name(&self) -> &'static str {
        ""
    }
}
