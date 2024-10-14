//! Read MCAP files from any source of bytes
pub mod decompressor;
pub mod read;

#[cfg(feature = "lz4")]
mod lz4;

#[cfg(feature = "zstd")]
mod zstd;
