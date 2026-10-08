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

/// A streaming decompressor for one chunk compression string.
///
/// [`LinearReader::add_decompressor`](crate::sans_io::LinearReader::add_decompressor) and
/// [`IndexedReader::add_decompressor`](crate::sans_io::IndexedReader::add_decompressor) store an
/// instance under [`Decompressor::name`]. That string is matched against the chunk `compression`
/// field. Readers keep one instance per name and call [`Decompressor::reset`] after each chunk.
pub trait Decompressor: Send {
    /// Returns the recommended size of input to pass into `decompress()`.
    fn next_read_size(&self) -> usize;
    /// Decompresses up to `dst.len()` bytes, consuming up to `src.len()` bytes from `src`.
    ///
    /// `consumed` and `wrote` must not exceed the buffer lengths. Returning no progress asks the
    /// reader for another call; [`Decompressor::next_read_size`] should grow when more input is
    /// required. A reader that already supplied that much input treats the call as a stall.
    fn decompress(&mut self, src: &[u8], dst: &mut [u8]) -> McapResult<DecompressResult>;
    /// Returns this decompressor to a fresh frame so it can decode another chunk.
    fn reset(&mut self) -> McapResult<()>;
    /// The chunk `compression` string this decompressor handles.
    ///
    /// An empty string is rejected at registration. In a chunk record, `""` means the records are
    /// stored uncompressed.
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
