use std::io::Cursor;

use anyhow::Result;

/// Check that chunks and statistics properly handle messages with log_time = 0
/// and don't ignore it, using the next time as the minimum.
#[test]
fn handles_time0_messages() -> Result<()> {
    let mut buf = Vec::new();
    let mut out = mcap::Writer::new(Cursor::new(&mut buf))?;

    let my_channel = mcap::Channel {
        topic: String::from("time"),
        message_encoding: String::from("text/plain"),
        metadata: Default::default(),
        schema: None,
    };

    let channel_id = out.add_channel(&my_channel)?;

    out.write_to_known_channel(
        &mcap::records::MessageHeader {
            channel_id,
            sequence: 1,
            log_time: 0,
            publish_time: 0,
        },
        b"Time, Dr. Freeman?",
    )?;
    out.write_to_known_channel(
        &mcap::records::MessageHeader {
            channel_id,
            sequence: 2,
            log_time: 42,
            publish_time: 42,
        },
        b"Is it really that time agian?",
    )?;

    out.finish()?;
    drop(out);

    let summary = mcap::read::Summary::read(&buf)?.expect("no summary");

    let the_chunk = &summary.chunk_indexes[0];
    assert_eq!(the_chunk.message_start_time, 0);
    assert_eq!(the_chunk.message_end_time, 42);

    let stats = &summary.stats.expect("no stats");
    assert_eq!(stats.message_start_time, 0);
    assert_eq!(stats.message_end_time, 42);

    Ok(())
}
