use std::io::Write as _;

use anyhow::Result;

use crate::cli::CatCommand;
use crate::commands::common;
use crate::context::CommandContext;

const MESSAGE_PREVIEW_LEN: usize = 10;

pub fn run(_ctx: &CommandContext, args: CatCommand) -> Result<()> {
    let stdout = std::io::stdout();
    let mut writer = std::io::BufWriter::new(stdout.lock());

    for file in args.files {
        let mcap = common::map_file(&file)?;
        for message in mcap::MessageStream::new(&mcap)? {
            write_message_line(&mut writer, &message?, MESSAGE_PREVIEW_LEN)?;
            writeln!(&mut writer)?;
        }
    }

    writer.flush()?;
    Ok(())
}

fn write_message_line(
    writer: &mut impl std::io::Write,
    message: &mcap::Message<'_>,
    max_preview_bytes: usize,
) -> std::io::Result<()> {
    write!(
        writer,
        "{} {} [{}] ",
        message.log_time,
        message.channel.topic,
        message
            .channel
            .schema
            .as_ref()
            .map(|schema| schema.name.as_str())
            .unwrap_or("no schema"),
    )?;
    write_payload_preview(writer, message.data.as_ref(), max_preview_bytes)
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
    use std::{borrow::Cow, collections::BTreeMap, sync::Arc};

    use super::{write_message_line, write_payload_preview};

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
        write_message_line(&mut out, message, max_preview_bytes)
            .expect("message line should write");
        String::from_utf8(out).expect("message line should be utf8")
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
}
