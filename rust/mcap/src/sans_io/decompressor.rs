//! Implement decompression algorithms for MCAP chunk data
use crate::McapResult;
pub struct DecompressResult {
    /// The number of bytes consumed from the input buffer.
    pub consumed: usize,
    /// The number of bytes written to the output buffer.
    pub wrote: usize,
}

/// A trait for streaming decompression.
pub trait Decompressor: Send {
    /// Returns the recommended size of input to pass into `decompress()`.
    fn next_read_size(&self) -> usize;
    /// Decompresses up to `dst.len()` bytes, consuming up to `src.len()` bytes from `src`.
    fn decompress(&mut self, src: &[u8], dst: &mut [u8]) -> McapResult<DecompressResult>;
    /// Resets the internal state of the decompressor.
    fn reset(&mut self) -> McapResult<()>;
    /// Returns the MCAP chunk compression string for the format that this Decompressor handles.
    fn name(&self) -> &'static str;
}
