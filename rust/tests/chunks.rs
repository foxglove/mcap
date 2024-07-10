mod common;

use common::*;

use std::io::Cursor;

use anyhow::Result;
use itertools::Itertools;

/// Checks that the writer will automatically close chunks when they hit a target size.
#[test]
fn auto_cut_chunks() -> Result<()> {
    let mapped = mcap_test_file()?;

    let messages = mcap::MappedMessageStream::new(&mapped)?;

    let mut tmp: Vec<u8> = Vec::new();
    // Setting chunk size to 0 ensures that each message gets written to a new chunk.
    {
        let mut writer = mcap::WriteOptions::new()
            .chunk_size(Some(0))
            .create(Cursor::new(&mut tmp))?;

        for m in messages {
            writer.write(&m?)?;
        }
    }

    // ensure that all messages can be read in the new MCAP
    for (theirs, ours) in mcap::MappedMessageStream::new(&tmp)?.zip_eq(mcap::MappedMessageStream::new(&tmp)?) {
        assert_eq!(ours?, theirs?)
    }

    // ensure that more than one chunk is present in the new MCAP
    let num_chunks = mcap::read::MappedLinearReader::new(&mapped)?
        .filter(|r| matches!(r, Ok(mcap::records::Record::Chunk { .. })))
        .count();
    assert!(num_chunks > 1);

    Ok(())
}
