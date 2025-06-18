//! Read MCAP files from any source of bytes
pub mod decompressor;
pub mod indexed_reader;
pub mod linear_reader;
pub mod summary_reader;

pub use indexed_reader::{IndexedReadEvent, IndexedReader, IndexedReaderOptions};
pub use linear_reader::{LinearReadEvent, LinearReader, LinearReaderOptions};
pub use summary_reader::{SummaryReadEvent, SummaryReader};

use crate::{McapError, McapResult};

#[cfg(feature = "lz4")]
mod lz4;

#[cfg(feature = "zstd")]
mod zstd;

/// Utility function for checking u64 lengths from MCAP files.
pub(crate) fn check_len(len: u64, limit: Option<usize>) -> Option<usize> {
    let len_as_usize: usize = len.try_into().ok()?;
    if limit.map(|l| len_as_usize > l).unwrap_or(false) {
        return None;
    }
    Some(len_as_usize)
}
