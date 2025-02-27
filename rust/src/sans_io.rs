//! Read MCAP files from any source of bytes
pub mod decompressor;
pub mod linear_reader;

pub use linear_reader::{LinearReadEvent, LinearReader, LinearReaderOptions};

#[cfg(feature = "lz4")]
mod lz4;

#[cfg(feature = "zstd")]
mod zstd;
