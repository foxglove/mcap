mod common;

use common::*;
use mcap::WriteOptions;

use std::io::BufWriter;

use anyhow::Result;
use itertools::Itertools;
use memmap::Mmap;
use rayon::prelude::*;
use tempfile::tempfile;

fn demo_round_trip_for_opts(opts: WriteOptions) -> Result<()> {
    use mcap::records::op;

    let mapped = mcap_test_file()?;

    let messages = mcap::MessageStream::new(&mapped)?;

    let mut tmp = tempfile()?;
    let mut writer = opts.create(BufWriter::new(&mut tmp))?;

    for m in messages {
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

    // We don't use them, but verify the summary offsets.
    let footer = mcap::read::footer(&ours)?;
    assert_ne!(footer.summary_offset_start, 0);

    const FOOTER_LEN: usize = 20 + 8 + 1; // 20 bytes + 8 byte len + 1 byte opcode
    let summary_offset_end = ours.len() - FOOTER_LEN - mcap::MAGIC.len();

    for (i, rec) in mcap::read::LinearReader::sans_magic(
        &ours[footer.summary_offset_start as usize..summary_offset_end],
    )
    .enumerate()
    {
        let offset = match rec {
            Ok(mcap::records::Record::SummaryOffset(sos)) => sos,
            wut => panic!("Expected summary offset, got {:?}", wut),
        };

        // We expect these offsets in this (arbitrary) order:
        match (i, offset.group_opcode) {
            (0, op::SCHEMA) => (),
            (1, op::CHANNEL) => (),
            (2, op::CHUNK_INDEX) => (),
            (3, op::STATISTICS) => (),
            _ => panic!("Summary offset {i} was {offset:?}"),
        };

        // We should be able to read each group from start to finish,
        // and the records should be the expected type.
        let group_start = offset.group_start as usize;
        let group_end = (offset.group_start + offset.group_length) as usize;
        for group_rec in mcap::read::LinearReader::sans_magic(&ours[group_start..group_end]) {
            match group_rec {
                Ok(rec) => assert_eq!(offset.group_opcode, rec.opcode()),
                wut => panic!("Expected op {}, got {:?}", offset.group_opcode, wut),
            }
        }
    }

    // Verify the summary and its connectivity.

    let summary = mcap::Summary::read(&ours)?.unwrap();
    assert!(summary.attachment_indexes.is_empty());
    assert!(summary.metadata_indexes.is_empty());

    // EZ mode: Streamed chunks should match up with a file-level message stream.
    for (whole, by_chunk) in mcap::MessageStream::new(&ours)?.zip_eq(
        summary
            .chunk_indexes
            .iter()
            .flat_map(|ci| summary.stream_chunk(&ours, ci).unwrap()),
    ) {
        assert_eq!(whole?, by_chunk?);
    }

    // Hard mode: randomly access every message in the MCAP.
    // Yes, this is dumb and O(n^2).
    let mut messages = Vec::new();

    for ci in &summary.chunk_indexes {
        let mut offsets_and_messages = summary
            .read_message_indexes(&ours, ci)
            .unwrap()
            // At least parallelize the dumb.
            .into_par_iter()
            .flat_map(|(_k, v)| v)
            .map(|e| (e.offset, summary.seek_message(&ours, ci, &e).unwrap()))
            .collect::<Vec<(u64, mcap::Message)>>();

        offsets_and_messages.sort_unstable_by_key(|im| im.0);

        for om in offsets_and_messages {
            messages.push(om.1);
        }
    }

    for (streamed, seeked) in mcap::MessageStream::new(&ours)?.zip_eq(messages.into_iter()) {
        assert_eq!(streamed?, seeked);
    }

    Ok(())
}

#[test]
fn demo_round_trip() -> Result<()> {
    demo_round_trip_for_opts(Default::default())
}

#[test]
fn demo_round_trip_buffered() -> Result<()> {
    demo_round_trip_for_opts(WriteOptions::default().use_buffered_chunks(true))
}

#[test]
fn demo_random_chunk_access() -> Result<()> {
    let mapped = mcap_test_file()?;

    let summary = mcap::Summary::read(&mapped)?.unwrap();

    // Random access of the second chunk should match the stream of the whole file.
    let messages_in_first_chunk: usize = summary
        .read_message_indexes(&mapped, &summary.chunk_indexes[0])?
        .values()
        .map(|entries| entries.len())
        .sum();
    let messages_in_second_chunk: usize = summary
        .read_message_indexes(&mapped, &summary.chunk_indexes[1])?
        .values()
        .map(|entries| entries.len())
        .sum();

    for (whole, random) in mcap::MessageStream::new(&mapped)?
        .skip(messages_in_first_chunk)
        .take(messages_in_second_chunk)
        .zip_eq(summary.stream_chunk(&mapped, &summary.chunk_indexes[1])?)
    {
        assert_eq!(whole?, random?);
    }
    // Let's poke around the message indexes
    let mut index_entries = summary
        .read_message_indexes(&mapped, &summary.chunk_indexes[1])?
        .values()
        .flatten()
        .copied()
        .collect::<Vec<mcap::records::MessageIndexEntry>>();

    index_entries.sort_unstable_by_key(|e| e.offset);

    // Do a big dumb n^2 seek of each message (dear god, don't ever actually do this)
    for (entry, message) in index_entries
        .iter()
        .zip_eq(summary.stream_chunk(&mapped, &summary.chunk_indexes[1])?)
    {
        let seeked = summary.seek_message(&mapped, &summary.chunk_indexes[1], entry)?;
        assert_eq!(seeked, message?);
    }

    Ok(())
}
