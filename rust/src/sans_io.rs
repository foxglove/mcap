pub mod decompressor;
pub mod input_buf;
pub mod read;

#[cfg(feature = "lz4")]
mod lz4;

#[cfg(feature = "zstd")]
mod zstd;
