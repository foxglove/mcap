mod common;

use common::*;

use std::io::Cursor;

use anyhow::Result;
use itertools::Itertools;

#[test]
fn flush_and_cut_chunks() -> Result<()> {
    let mapped = mcap_test_file()?;

    let messages = mcap::MessageStream::new(&mapped)?;

    let mut tmp: Vec<u8> = Vec::new();
    let mut writer = mcap::WriteOptions::new()
        .chunk_size(None)
        .create(Cursor::new(&mut tmp))?;

    for (i, m) in messages.enumerate() {
        writer.write(&m?)?;
        // Cut a new chunk every other message
        if i % 2 == 0 {
            writer.flush()?;
        }
    }
    drop(writer);

    // Compare the message stream of our MCAP to the reference one.
    // Regardless of the chunk boundaries, they should be the same.
    for (theirs, ours) in mcap::MessageStream::new(&mapped)?.zip_eq(mcap::MessageStream::new(&tmp)?)
    {
        assert_eq!(ours?, theirs?)
    }

    Ok(())
}
