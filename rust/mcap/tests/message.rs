mod common;

use common::*;

use std::{borrow::Cow, io::BufWriter, sync::Arc};

use anyhow::Result;
use tempfile::tempfile;

#[test]
fn smoke() -> Result<()> {
    let mapped = read_mcap("../../tests/conformance/data/OneMessage/OneMessage.mcap")?;
    let messages = mcap::MessageStream::new(&mapped)?.collect::<mcap::McapResult<Vec<_>>>()?;

    assert_eq!(messages.len(), 1);

    let expected = mcap::Message {
        channel: Arc::new(mcap::Channel {
            id: 1,
            schema: Some(Arc::new(mcap::Schema {
                id: 1,
                name: String::from("Example"),
                encoding: String::from("c"),
                data: Cow::Borrowed(&[4, 5, 6]),
            })),
            topic: String::from("example"),
            message_encoding: String::from("a"),
            metadata: [(String::from("foo"), String::from("bar"))].into(),
        }),
        sequence: 10,
        log_time: 2,
        publish_time: 1,
        data: Cow::Borrowed(&[1, 2, 3]),
    };

    assert_eq!(messages[0], expected);

    Ok(())
}

#[test]
fn round_trip() -> Result<()> {
    run_round_trip(true)
}

#[test]
fn round_trip_no_chunks() -> Result<()> {
    run_round_trip(false)
}

fn run_round_trip(use_chunks: bool) -> Result<()> {
    let mapped = read_mcap("../../tests/conformance/data/OneMessage/OneMessage.mcap")?;
    let messages = mcap::MessageStream::new(&mapped)?;

    let mut tmp = tempfile()?;
    let mut writer = mcap::WriteOptions::default()
        .use_chunks(use_chunks)
        .create(BufWriter::new(&mut tmp))?;

    for m in messages {
        writer.write(&m?)?;
    }
    drop(writer);

    let ours = read_back(&mut tmp)?;
    let summary = mcap::Summary::read(&ours)?.unwrap();

    let schema = Arc::new(mcap::Schema {
        id: 1,
        name: String::from("Example"),
        encoding: String::from("c"),
        data: Cow::Borrowed(&[4, 5, 6]),
    });

    let channel = Arc::new(mcap::Channel {
        id: 1,
        schema: Some(schema.clone()),
        topic: String::from("example"),
        message_encoding: String::from("a"),
        metadata: [(String::from("foo"), String::from("bar"))].into(),
    });

    let expected_summary = mcap::Summary {
        stats: Some(mcap::records::Statistics {
            message_count: 1,
            schema_count: 1,
            channel_count: 1,
            chunk_count: if use_chunks { 1 } else { 0 },
            message_start_time: 2,
            message_end_time: 2,
            channel_message_counts: [(1, 1)].into(),
            ..Default::default()
        }),
        channels: [(1, channel.clone())].into(),
        schemas: [(1, schema.clone())].into(),
        ..Default::default()
    };
    // Don't assert the chunk indexes - their size is at the whim of compressors.
    assert_eq!(summary.stats, expected_summary.stats);
    assert_eq!(summary.channels, expected_summary.channels);
    assert_eq!(summary.schemas, expected_summary.schemas);
    assert_eq!(
        summary.attachment_indexes,
        expected_summary.attachment_indexes
    );
    assert_eq!(summary.metadata_indexes, expected_summary.metadata_indexes);

    let expected = mcap::Message {
        channel,
        sequence: 10,
        log_time: 2,
        publish_time: 1,
        data: Cow::Borrowed(&[1, 2, 3]),
    };

    assert_eq!(
        mcap::MessageStream::new(&ours)?.collect::<mcap::McapResult<Vec<_>>>()?,
        &[expected]
    );

    Ok(())
}

/// Four messages on one channel, in uncompressed chunks with a summary.
fn chunked_fixture() -> Result<Vec<u8>> {
    let mut buffer = Vec::new();
    let mut writer = mcap::WriteOptions::default()
        .use_chunks(true)
        .compression(None)
        .create(std::io::Cursor::new(&mut buffer))?;
    let schema_id = writer.add_schema("Example", "c", &[4, 5, 6])?;
    let channel_id = writer.add_channel(schema_id, "example", "a", &Default::default())?;
    for sequence in 0..4 {
        writer.write_to_known_channel(
            &mcap::records::MessageHeader {
                channel_id,
                sequence,
                log_time: u64::from(sequence),
                publish_time: u64::from(sequence),
            },
            &[sequence as u8; 8],
        )?;
    }
    writer.finish()?;
    drop(writer);
    Ok(buffer)
}

#[test]
fn reads_through_the_summary_to_the_end_magic() -> Result<()> {
    let buffer = chunked_fixture()?;
    let summary_start = mcap::read::footer(&buffer)?.summary_start as usize;
    assert!(summary_start > 0, "fixture needs a summary section");
    // The summary opens with the schema: opcode, length, id (2), name length (4), name.
    assert_eq!(buffer[summary_start], mcap::records::op::SCHEMA);
    let name_at = summary_start + 9 + 2 + 4;

    // A summary that disagrees with the data (a renamed schema) is not an error.
    let mut damaged = buffer.clone();
    damaged[name_at] = b'X';
    let messages = mcap::MessageStream::new(&damaged)?.collect::<mcap::McapResult<Vec<_>>>()?;
    assert_eq!(messages.len(), 4);
    let raw = mcap::read::RawMessageStream::new(&damaged)?.collect::<mcap::McapResult<Vec<_>>>()?;
    assert_eq!(raw.len(), 4);

    // Truncated summary: every message, then the error.
    let truncated = &buffer[..summary_start + 20];
    let items = mcap::MessageStream::new(truncated)?.collect::<Vec<_>>();
    assert_eq!(items.len(), 5, "{items:?}");
    assert!(items[..4].iter().all(|item| item.is_ok()));
    assert!(matches!(items[4], Err(mcap::McapError::UnexpectedEof)));

    // Bad end magic: the same.
    let mut bad_magic = buffer.clone();
    *bad_magic.last_mut().unwrap() ^= 0xFF;
    let items = mcap::MessageStream::new(&bad_magic)?.collect::<Vec<_>>();
    assert_eq!(items.len(), 5, "{items:?}");
    assert!(matches!(items[4], Err(mcap::McapError::BadMagic)));

    // With IgnoreEndMagic the stream ends at the data end record.
    let lenient = enumset::enum_set!(mcap::read::Options::IgnoreEndMagic);
    for file in [truncated, &bad_magic[..]] {
        let messages = mcap::MessageStream::new_with_options(file, lenient)?
            .collect::<mcap::McapResult<Vec<_>>>()?;
        assert_eq!(messages.len(), 4);
        let raw = mcap::read::RawMessageStream::new_with_options(file, lenient)?
            .collect::<mcap::McapResult<Vec<_>>>()?;
        assert_eq!(raw.len(), 4);
    }
    Ok(())
}

#[test]
fn validates_chunk_crcs_only_with_the_option() -> Result<()> {
    let mut buffer = chunked_fixture()?;

    // Flip the last byte of the last chunk. It ends with a message, so this lands in payload:
    // every record still parses and only the CRC disagrees.
    let summary = mcap::read::Summary::read(&buffer)?.expect("fixture has a summary");
    let index = summary.chunk_indexes.last().expect("fixture has a chunk");
    let last_byte = usize::try_from(index.chunk_start_offset + index.chunk_length)? - 1;
    buffer[last_byte] ^= 0xFF;

    // Default: no CRC check.
    let messages = mcap::MessageStream::new(&buffer)?.collect::<mcap::McapResult<Vec<_>>>()?;
    assert_eq!(messages.len(), 4);

    // Opt in: the mismatch is reported.
    let items = mcap::MessageStream::new_with_options(
        &buffer,
        enumset::enum_set!(mcap::read::Options::ValidateChunkCrcs),
    )?
    .collect::<Vec<_>>();
    assert!(
        items
            .iter()
            .any(|item| matches!(item, Err(mcap::McapError::BadChunkCrc { .. }))),
        "{items:?}"
    );
    Ok(())
}
