mod common;

use common::*;

use std::io::BufWriter;

use anyhow::Result;
use itertools::Itertools;
use memmap::Mmap;
use tempfile::tempfile;

fn round_trip(comp: Option<mcap::Compression>) -> Result<()> {
    let mapped = mcap_test_file()?;

    let mut tmp = tempfile()?;
    let mut writer = mcap::WriteOptions::new()
        .compression(comp)
        .profile("fooey")
        .create(BufWriter::new(&mut tmp))?;

    for m in mcap::MessageStream::new(&mapped)? {
        // IRL, we'd add channels, then write messages to known channels,
        // which skips having to re-hash the channel and its schema each time.
        // But since here we'd need to do the same anyways...
        writer.write(&m?)?;
    }
    drop(writer);

    let ours = unsafe { Mmap::map(&tmp) }?;

    // Compare the message stream of our MCAP to the reference one.
    for (theirs, ours) in
        mcap::MessageStream::new(&mapped)?.zip_eq(mcap::MessageStream::new(&ours)?)
    {
        assert_eq!(ours?, theirs?)
    }

    Ok(())
}

#[test]
fn uncompressed_round_trip() -> Result<()> {
    round_trip(None)
}

#[cfg(feature = "zstd")]
#[test]
fn zstd_round_trip() -> Result<()> {
    round_trip(Some(mcap::Compression::Zstd))
}

#[test]
fn lz4_round_trip() -> Result<()> {
    round_trip(Some(mcap::Compression::Lz4))
}
