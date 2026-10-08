//! Implement decompression algorithms for MCAP chunk data
use std::collections::hash_map::Entry;
use std::collections::HashMap;

use crate::{McapError, McapResult};

pub struct DecompressResult {
    /// The number of bytes consumed from the input buffer.
    pub consumed: usize,
    /// The number of bytes written to the output buffer.
    pub wrote: usize,
}

/// A streaming decompressor for one chunk compression format.
///
/// Register an instance with `add_decompressor` on
/// [`LinearReader`](crate::sans_io::LinearReader) or [`IndexedReader`](crate::sans_io::IndexedReader).
/// [`MessageReader`](crate::sans_io::MessageReader), [`io::MessageReader`](crate::io::MessageReader),
/// and, with the `tokio` feature, `tokio::LinearReader` forward to their inner `LinearReader`.
/// The reader uses it for chunks whose `compression` field equals [`Decompressor::name`], keeps
/// one instance per name, and calls [`Decompressor::reset`] after each chunk.
///
/// [`MessageStream`](crate::MessageStream) and [`ChunkReader`](crate::read::ChunkReader) do not
/// accept custom decompressors. For bytes already in memory, use
/// [`io::MessageReader`](crate::io::MessageReader) over a [`std::io::Cursor`] instead.
pub trait Decompressor: Send {
    /// Returns the recommended size of input to pass into `decompress()`.
    fn next_read_size(&self) -> usize;
    /// Decompresses up to `dst.len()` bytes, consuming up to `src.len()` bytes from `src`.
    ///
    /// A chunk may contain several frames, including skippable ones, so keep decoding across frame
    /// boundaries. The reader stops calling this once the chunk's declared uncompressed size has
    /// been written.
    ///
    /// `consumed` and `wrote` must not exceed the buffer lengths. Returning zero for both means
    /// more input is needed; the reader treats this as an error unless
    /// [`Decompressor::next_read_size`] now returns more than `src.len()` and more compressed
    /// input remains in the chunk.
    fn decompress(&mut self, src: &[u8], dst: &mut [u8]) -> McapResult<DecompressResult>;
    /// Resets internal state so this instance can decode the next chunk.
    fn reset(&mut self) -> McapResult<()>;
    /// The chunk `compression` string this decompressor handles.
    ///
    /// Must be non-empty: `""` marks an uncompressed chunk, and `add_decompressor` rejects it.
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
