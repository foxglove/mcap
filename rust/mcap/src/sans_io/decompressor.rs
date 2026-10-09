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

/// A streaming decompressor for one chunk compression format.
///
/// Register an instance with `add_decompressor` on
/// [`LinearReader`](crate::sans_io::LinearReader) or [`IndexedReader`](crate::sans_io::IndexedReader).
/// [`MessageReader`](crate::sans_io::MessageReader), [`io::MessageReader`](crate::io::MessageReader),
/// and, with the `tokio` feature, `tokio::LinearReader` forward to their inner `LinearReader`.
/// The reader uses it for chunks whose `compression` field equals [`Decompressor::name`], keeps
/// one instance per name, and calls [`Decompressor::reset`] after each chunk it finishes reading.
/// `reset` is not called before the first chunk, so the instance must be ready to decode when it
/// is registered.
///
/// [`MessageStream`](crate::MessageStream) and [`ChunkReader`](crate::read::ChunkReader) do not
/// accept custom decompressors. For bytes already in memory, use
/// [`io::MessageReader`](crate::io::MessageReader) over a [`std::io::Cursor`] instead.
pub trait Decompressor: Send {
    /// How many compressed bytes to buffer before the next [`Decompressor::decompress`] call.
    ///
    /// [`LinearReader`](crate::sans_io::LinearReader) calls this before every `decompress` call,
    /// including the first of a chunk, and waits until `min(next_read_size(), remaining compressed
    /// bytes)` are available. Return 0 when any amount will do. An empty buffer with compressed
    /// bytes still in the chunk makes `LinearReader` fetch at least one more byte instead of
    /// calling `decompress`. A large value makes `LinearReader` buffer that much of the chunk,
    /// capped at the chunk's compressed size. After a call that made no progress, return how many
    /// bytes you need. Otherwise `LinearReader` asks for one more byte at a time.
    /// [`IndexedReader`](crate::sans_io::IndexedReader) already holds the whole chunk and does not
    /// use this hint.
    fn next_read_size(&self) -> usize;
    /// Decompresses up to `dst.len()` bytes, consuming up to `src.len()` bytes from `src`.
    ///
    /// A chunk may contain several frames, including skippable ones, so keep decoding across frame
    /// boundaries. The reader stops calling this once the chunk's declared uncompressed size has
    /// been written. Bytes left unread after that, such as padding after the last frame, are
    /// skipped.
    ///
    /// `dst` may be as small as one byte: `LinearReader` sizes it to the next record it parses.
    /// Write as much as fits and keep any other decoded output for the next call.
    ///
    /// `consumed` and `wrote` must not exceed the buffer lengths. Returning zero for both asks for
    /// more compressed input, so do that only when no output can be produced without more input. [`LinearReader`](crate::sans_io::LinearReader) then reads at least
    /// one more byte of the chunk, and returns an error only when `src` already held every
    /// remaining compressed byte. [`IndexedReader`](crate::sans_io::IndexedReader) passes the whole
    /// chunk, so zero progress is an error there.
    fn decompress(&mut self, src: &[u8], dst: &mut [u8]) -> McapResult<DecompressResult>;
    /// Resets internal state so this instance can decode another chunk.
    ///
    /// Not called before the first chunk. Register an instance that is already ready to decode.
    /// On [`LinearReader`](crate::sans_io::LinearReader), a bad chunk CRC is returned instead of a
    /// reset error when both fail. The instance is still kept for the next chunk.
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
