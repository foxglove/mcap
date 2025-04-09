//! Read MCAP files from any source of bytes
pub mod decompressor;
pub mod indexed_reader;
pub mod linear_reader;
pub mod summary_reader;

pub use indexed_reader::{IndexedReadEvent, IndexedReader, IndexedReaderOptions};
pub use linear_reader::{LinearReadEvent, LinearReader, LinearReaderOptions};
pub use summary_reader::{SummaryReadEvent, SummaryReader};

#[cfg(feature = "lz4")]
mod lz4;

#[cfg(feature = "zstd")]
mod zstd;
