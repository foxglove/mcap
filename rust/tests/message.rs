mod common;

use common::*;

use std::{borrow::Cow, io::BufWriter, sync::Arc};

use anyhow::Result;
use memmap::Mmap;
use tempfile::tempfile;

#[test]
fn smoke() -> Result<()> {
    let mapped = map_mcap("../tests/conformance/data/OneMessage/OneMessage.mcap")?;
    let messages = mcap::MappedMessageStream::new(&mapped)?.collect::<mcap::McapResult<Vec<_>>>()?;

    assert_eq!(messages.len(), 1);

    let expected = mcap::Message {
        channel: Arc::new(mcap::Channel {
            schema: Some(Arc::new(mcap::Schema {
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
    let mapped = map_mcap("../tests/conformance/data/OneMessage/OneMessage.mcap")?;
    let messages = mcap::MappedMessageStream::new(&mapped)?;

    let mut tmp = tempfile()?;
    let mut writer = mcap::Writer::new(BufWriter::new(&mut tmp))?;

    for m in messages {
        writer.write(&m?)?;
    }
    drop(writer);

    let ours = unsafe { Mmap::map(&tmp) }?;
    let summary = mcap::Summary::read(&ours)?.unwrap();

    let schema = Arc::new(mcap::Schema {
        name: String::from("Example"),
        encoding: String::from("c"),
        data: Cow::Borrowed(&[4, 5, 6]),
    });

    let channel = Arc::new(mcap::Channel {
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
            chunk_count: 1,
            message_start_time: 2,
            message_end_time: 2,
            channel_message_counts: [(0, 1)].into(),
            ..Default::default()
        }),
        channels: [(0, channel.clone())].into(),
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
        mcap::MappedMessageStream::new(&ours)?.collect::<mcap::McapResult<Vec<_>>>()?,
        &[expected]
    );

    Ok(())
}
