use std::{
    fs,
    io::{Read, Seek, SeekFrom},
};

use anyhow::{Context, Result};
use camino::Utf8Path;

/// Reads a fixture into memory for the slice-based readers under test.
pub fn read_mcap<P: AsRef<Utf8Path>>(p: P) -> Result<Vec<u8>> {
    let p = p.as_ref();
    fs::read(p).with_context(|| format!("Couldn't read {p}"))
}

#[allow(dead_code)]
pub fn mcap_test_file() -> Result<Vec<u8>> {
    if cfg!(feature = "zstd") {
        read_mcap("tests/data/compressed.mcap")
    } else {
        read_mcap("tests/data/uncompressed.mcap")
    }
}

/// Reads back everything written to a temporary file.
#[allow(dead_code)]
pub fn read_back(file: &mut fs::File) -> Result<Vec<u8>> {
    file.seek(SeekFrom::Start(0))?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)?;
    Ok(bytes)
}
