mod common;

use common::*;

use std::io::BufWriter;

use anyhow::Result;
use itertools::Itertools;
use memmap::Mmap;
use tempfile::tempfile;

#[test]
fn flush_and_cut_chunks() -> Result<()> {
    let mapped = map_mcap("../testdata/mcap/demo.mcap")?;

    let messages = mcap::MessageStream::new(&mapped)?;

    let mut tmp = tempfile()?;
    let mut writer = mcap::Writer::new(BufWriter::new(&mut tmp))?;

    for (i, m) in messages.enumerate() {
        writer.write(&m?)?;
        // Cut a new chunk every other message
        if i % 2 == 0 {
            writer.flush()?;
        }
    }
    drop(writer);

    let ours = unsafe { Mmap::map(&tmp) }?;

    // Compare the message stream of our MCAP to the reference one.
    // Regardless of the chunk boundaries, they should be the same.
    for (theirs, ours) in
        mcap::MessageStream::new(&mapped)?.zip_eq(mcap::MessageStream::new(&ours)?)
    {
        assert_eq!(ours?, theirs?)
    }

    Ok(())
}
