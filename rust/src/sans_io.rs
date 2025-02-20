//! Read MCAP files from any source of bytes
pub mod decompressor;
pub mod read;
pub mod read_indexed;

#[cfg(feature = "lz4")]
mod lz4;

#[cfg(feature = "zstd")]
mod zstd;
