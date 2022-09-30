use std::fs;

use anyhow::{Context, Result};
use camino::Utf8Path;
use memmap::Mmap;

pub fn map_mcap<P: AsRef<Utf8Path>>(p: P) -> Result<Mmap> {
    let p = p.as_ref();
    let fd = fs::File::open(p).with_context(|| format!("Couldn't open {p}"))?;
    unsafe { Mmap::map(&fd) }.with_context(|| format!("Couldn't map {p}"))
}
