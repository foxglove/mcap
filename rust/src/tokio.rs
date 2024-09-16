//! Read MCAP data from a stream asynchronously
#[cfg(feature = "lz4")]
mod lz4;
pub mod read;
mod read_exact_or_zero;
pub mod read_indexed;

pub use read::{RecordReader, RecordReaderOptions};
use read_exact_or_zero::read_exact_or_zero;
