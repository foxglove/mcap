use std::fs;

use anyhow::{Context, Result};
use camino::Utf8Path;
use memmap::Mmap;

pub fn map_mcap<P: AsRef<Utf8Path>>(p: P) -> Result<Mmap> {
    let p = p.as_ref();
    let fd = fs::File::open(p).with_context(|| format!("Couldn't open {p}"))?;
    unsafe { Mmap::map(&fd) }.with_context(|| format!("Couldn't map {p}"))
}

#[allow(dead_code)]
pub fn mcap_test_file() -> Result<Mmap> {
    if cfg!(feature = "zstd") {
        map_mcap("tests/data/compressed.mcap")
    } else {
        map_mcap("tests/data/uncompressed.mcap")
    }
}
