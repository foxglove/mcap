//! Implement decompression algorithms for MCAP chunk data
use std::collections::hash_map::Entry;
use std::collections::HashMap;

use crate::{McapError, McapResult};

/// How many bytes one [`Decompressor::decompress`] call consumed and wrote.
#[derive(Debug, Clone, Copy)]
pub struct DecompressResult {
    /// The number of bytes consumed from the input buffer.
    pub consumed: usize,
    /// The number of bytes written to the output buffer.
    pub wrote: usize,
}

/// A streaming decompressor for one MCAP chunk compression format.
///
/// [`name`](Self::name) is the chunk `compression` string this decompressor handles. Register an
/// instance before the reader reaches a chunk with that name.
/// [`LinearReader`](crate::sans_io::LinearReader),
/// [`IndexedReader`](crate::sans_io::IndexedReader), and the readers that wrap `LinearReader`
/// accept one. The readers in [`crate::read`] do not.
/// One instance is kept per name. [`reset`](Self::reset) is called after each chunk, and not
/// before the first, so the instance must be ready to decode when it is registered.
pub trait Decompressor: Send {
    /// How many compressed bytes should be available in `src` on the next
    /// [`decompress`](Self::decompress) call.
    ///
    /// Return 0 when any amount of input is acceptable. Otherwise return how many bytes to buffer
    /// before the next call, not a minimum. A caller may pass fewer bytes than this asks for.
    fn next_read_size(&self) -> usize;
    /// Decompresses bytes from `src` into `dst`.
    ///
    /// Consume at most `src.len()` bytes and write at most `dst.len()` bytes. [`DecompressResult`]
    /// reports those counts and must not claim more. `dst` may be as small as one byte; keep
    /// decoded output that does not fit and write it on a later call.
    ///
    /// A chunk may hold several frames, including skippable frames. Continue across frame
    /// boundaries until the caller stops. The caller stops once the chunk's declared uncompressed
    /// size has been written, and does not pass compressed bytes left after that point.
    ///
    /// Return `consumed: 0` and `wrote: 0` only when no output can be produced without more
    /// compressed input. Returning that when `src` already holds enough input to make progress is
    /// an error.
    fn decompress(&mut self, src: &[u8], dst: &mut [u8]) -> McapResult<DecompressResult>;
    /// Prepares this instance to decode another chunk.
    ///
    /// Called after each chunk, not before the first. On failure the error is returned and the
    /// instance stays registered.
    fn reset(&mut self) -> McapResult<()>;
    /// The chunk `compression` string this decompressor handles.
    ///
    /// Must be non-empty. An empty string means the chunk is not compressed, and registering this
    /// decompressor fails with [`McapError::EmptyDecompressorName`].
    fn name(&self) -> &'static str;
}

pub(crate) fn register_decompressor(
    decompressors: &mut HashMap<String, Box<dyn Decompressor>>,
    decompressor: impl Decompressor + 'static,
) -> McapResult<()> {
    let name = decompressor.name();
    if name.is_empty() {
        return Err(McapError::EmptyDecompressorName);
    }
    match decompressors.entry(name.to_owned()) {
        Entry::Occupied(_) => Err(McapError::DuplicateDecompressor(name.to_owned())),
        Entry::Vacant(entry) => {
            entry.insert(Box::new(decompressor));
            Ok(())
        }
    }
}

/// Rejects a [`DecompressResult`] that reports more bytes than the buffers it was given.
pub(crate) fn check_decompress_result(
    result: DecompressResult,
    src_len: usize,
    dst_len: usize,
) -> McapResult<DecompressResult> {
    if result.consumed > src_len || result.wrote > dst_len {
        return Err(McapError::DecompressionError(
            "decompressor reported more bytes than the buffers provided".into(),
        ));
    }
    Ok(result)
}

pub(crate) fn no_progress_error() -> McapError {
    McapError::DecompressionError("decompressor made no progress".into())
}
