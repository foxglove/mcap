use std::io::Write as _;

use anyhow::Result;
use mcap::sans_io::indexed_reader::ReadOrder;

use crate::cli::CatCommand;
use crate::commands::common;
use crate::context::CommandContext;

const MESSAGE_PREVIEW_LEN: usize = 10;

pub fn run(_ctx: &CommandContext, args: CatCommand) -> Result<()> {
    let stdout = std::io::stdout();
    let mut writer = std::io::BufWriter::new(stdout.lock());

    for file in args.files {
        let mcap = common::map_file(&file)?;
        if cat_mcap(&mut writer, &mcap)? {
            return Ok(());
        }
    }

    if let Err(err) = writer.flush() {
        if err.kind() == std::io::ErrorKind::BrokenPipe {
            return Ok(());
        }
        return Err(err.into());
    }

    Ok(())
}

fn cat_mcap(writer: &mut impl std::io::Write, mcap: &[u8]) -> Result<bool> {
    if let Some(broken_pipe) = cat_indexed(writer, mcap)? {
        return Ok(broken_pipe);
    }
    cat_linear(writer, mcap)
}

fn cat_indexed(writer: &mut impl std::io::Write, mcap: &[u8]) -> Result<Option<bool>> {
    let Some(summary) = mcap::Summary::read(mcap)? else {
        return Ok(None);
    };
    if summary.chunk_indexes.is_empty() {
        return Ok(None);
    }

    let mut reader = mcap::sans_io::IndexedReader::new_with_options(
        &summary,
        mcap::sans_io::IndexedReaderOptions::new().with_order(ReadOrder::LogTime),
    )?;

    while let Some(event) = reader.next_event() {
        match event? {
            mcap::sans_io::IndexedReadEvent::ReadChunkRequest { offset, length } => {
                let start = offset as usize;
                let end = start
                    .checked_add(length)
                    .ok_or_else(|| anyhow::anyhow!("chunk read overflow at offset {offset}"))?;
                if end > mcap.len() {
                    anyhow::bail!("chunk read out of bounds at offset {offset} length {length}");
                }
                reader.insert_chunk_record_data(offset, &mcap[start..end])?;
            }
            mcap::sans_io::IndexedReadEvent::Message { header, data } => {
                let channel = summary
                    .channels
                    .get(&header.channel_id)
                    .ok_or_else(|| anyhow::anyhow!("unknown channel {}", header.channel_id))?;
                let schema_name = channel
                    .schema
                    .as_ref()
                    .map(|schema| schema.name.as_str())
                    .unwrap_or("no schema");
                if write_message_fields(
                    writer,
                    header.log_time,
                    &channel.topic,
                    schema_name,
                    data,
                    MESSAGE_PREVIEW_LEN,
                )? {
                    return Ok(Some(true));
                }
            }
        }
    }

    Ok(Some(false))
}

fn cat_linear(writer: &mut impl std::io::Write, mcap: &[u8]) -> Result<bool> {
    for message in mcap::MessageStream::new(mcap)? {
        let message = message?;
        let schema_name = message
            .channel
            .schema
            .as_ref()
            .map(|schema| schema.name.as_str())
            .unwrap_or("no schema");
        if write_message_fields(
            writer,
            message.log_time,
            &message.channel.topic,
            schema_name,
            message.data.as_ref(),
            MESSAGE_PREVIEW_LEN,
        )? {
            return Ok(true);
        }
    }
    Ok(false)
}

fn write_message_fields(
    writer: &mut impl std::io::Write,
    log_time: u64,
    topic: &str,
    schema_name: &str,
    data: &[u8],
    max_preview_bytes: usize,
) -> Result<bool> {
    if let Err(err) = write!(writer, "{} {} [{}] ", log_time, topic, schema_name) {
        if err.kind() == std::io::ErrorKind::BrokenPipe {
            return Ok(true);
        }
        return Err(err.into());
    }

    if let Err(err) = write_payload_preview(writer, data, max_preview_bytes) {
        if err.kind() == std::io::ErrorKind::BrokenPipe {
            return Ok(true);
        }
        return Err(err.into());
    }

    if let Err(err) = writeln!(writer) {
        if err.kind() == std::io::ErrorKind::BrokenPipe {
            return Ok(true);
        }
        return Err(err.into());
    }

    Ok(false)
}

fn write_payload_preview(
    writer: &mut impl std::io::Write,
    data: &[u8],
    max_bytes: usize,
) -> std::io::Result<()> {
    let preview = if data.len() > max_bytes {
        &data[..max_bytes]
    } else {
        data
    };

    write!(writer, "[")?;
    for (idx, byte) in preview.iter().enumerate() {
        if idx > 0 {
            write!(writer, " ")?;
        }
        write!(writer, "{byte}")?;
    }
    write!(writer, "]")?;

    if data.len() > max_bytes {
        write!(writer, "...")?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{borrow::Cow, collections::BTreeMap, io::Cursor, sync::Arc};

    use super::{cat_mcap, write_payload_preview};

    fn sample_message(schema_name: Option<&str>, data: Vec<u8>) -> mcap::Message<'static> {
        let schema = schema_name.map(|name| {
            Arc::new(mcap::Schema {
                id: 1,
                name: name.to_string(),
                encoding: "jsonschema".to_string(),
                data: Cow::Owned(Vec::new()),
            })
        });
        mcap::Message {
            channel: Arc::new(mcap::Channel {
                id: 1,
                topic: "/demo".to_string(),
                schema,
                message_encoding: "json".to_string(),
                metadata: BTreeMap::new(),
            }),
            sequence: 1,
            log_time: 42,
            publish_time: 43,
            data: Cow::Owned(data),
        }
    }

    fn payload_preview_string(data: &[u8], max_bytes: usize) -> String {
        let mut out = Vec::new();
        write_payload_preview(&mut out, data, max_bytes).expect("payload preview should serialize");
        String::from_utf8(out).expect("payload preview should be utf8")
    }

    fn message_line_string(message: &mcap::Message<'_>, max_preview_bytes: usize) -> String {
        let mut out = Vec::new();
        let schema_name = message
            .channel
            .schema
            .as_ref()
            .map(|schema| schema.name.as_str())
            .unwrap_or("no schema");
        let broken_pipe = super::write_message_fields(
            &mut out,
            message.log_time,
            &message.channel.topic,
            schema_name,
            message.data.as_ref(),
            max_preview_bytes,
        )
        .expect("message line should write");
        assert!(!broken_pipe);
        String::from_utf8(out)
            .expect("message line should be utf8")
            .trim_end_matches('\n')
            .to_string()
    }

    fn build_out_of_order_chunked_mcap() -> Vec<u8> {
        let mut cursor = Cursor::new(Vec::new());
        {
            let mut writer = mcap::WriteOptions::new()
                .chunk_size(Some(1024))
                .create(&mut cursor)
                .expect("writer");

            let schema_id = writer
                .add_schema("Example", "jsonschema", br#"{"type":"object"}"#)
                .expect("schema");
            let channel_id = writer
                .add_channel(schema_id, "/demo", "json", &BTreeMap::new())
                .expect("channel");

            writer
                .write_to_known_channel(
                    &mcap::records::MessageHeader {
                        channel_id,
                        sequence: 1,
                        log_time: 30,
                        publish_time: 30,
                    },
                    &[1],
                )
                .expect("write message 1");
            writer.flush().expect("flush chunk 1");

            writer
                .write_to_known_channel(
                    &mcap::records::MessageHeader {
                        channel_id,
                        sequence: 2,
                        log_time: 10,
                        publish_time: 10,
                    },
                    &[2],
                )
                .expect("write message 2");
            writer.flush().expect("flush chunk 2");

            writer.finish().expect("finish");
        }
        cursor.into_inner()
    }

    #[test]
    fn payload_preview_includes_full_message_when_short() {
        assert_eq!(payload_preview_string(&[1, 2, 3], 10), "[1 2 3]");
    }

    #[test]
    fn payload_preview_truncates_with_ellipsis() {
        let data: Vec<u8> = (0..12).collect();
        assert_eq!(
            payload_preview_string(&data, 10),
            "[0 1 2 3 4 5 6 7 8 9]..."
        );
    }

    #[test]
    fn message_line_includes_schema_name_when_present() {
        let message = sample_message(Some("Example"), vec![1, 2, 3]);
        assert_eq!(
            message_line_string(&message, 10),
            "42 /demo [Example] [1 2 3]"
        );
    }

    #[test]
    fn message_line_uses_no_schema_for_schemaless_channel() {
        let message = sample_message(None, vec![1, 2, 3]);
        assert_eq!(
            message_line_string(&message, 10),
            "42 /demo [no schema] [1 2 3]"
        );
    }

    #[test]
    fn cat_prefers_log_time_order_when_index_available() {
        let mcap = build_out_of_order_chunked_mcap();
        let mut out = Vec::new();
        let broken_pipe = cat_mcap(&mut out, &mcap).expect("cat should succeed");
        assert!(!broken_pipe);

        let output = String::from_utf8(out).expect("valid utf8 output");
        let lines: Vec<&str> = output.lines().collect();
        assert_eq!(
            lines,
            vec!["10 /demo [Example] [2]", "30 /demo [Example] [1]"]
        );
    }
}
