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
            writeln!(&mut writer, "{}", format_message_line(&message?))?;
        }
    }

    writer.flush()?;
    Ok(())
}

fn format_message_line(message: &mcap::Message<'_>) -> String {
    format!(
        "{} {} [{}] {}",
        message.log_time,
        message.channel.topic,
        message
            .channel
            .schema
            .as_ref()
            .map(|schema| schema.name.as_str())
            .unwrap_or("no schema"),
        format_payload_preview(message.data.as_ref(), MESSAGE_PREVIEW_LEN),
    )
}

fn format_payload_preview(data: &[u8], max_bytes: usize) -> String {
    let preview = if data.len() > max_bytes {
        &data[..max_bytes]
    } else {
        data
    };

    let body = preview
        .iter()
        .map(u8::to_string)
        .collect::<Vec<_>>()
        .join(" ");

    if data.len() > max_bytes {
        format!("[{body}]...")
    } else {
        format!("[{body}]")
    }
}

#[cfg(test)]
mod tests {
    use std::{borrow::Cow, collections::BTreeMap, sync::Arc};

    use super::{format_message_line, format_payload_preview};

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

    #[test]
    fn payload_preview_includes_full_message_when_short() {
        assert_eq!(format_payload_preview(&[1, 2, 3], 10), "[1 2 3]");
    }

    #[test]
    fn payload_preview_truncates_with_ellipsis() {
        let data: Vec<u8> = (0..12).collect();
        assert_eq!(format_payload_preview(&data, 10), "[0 1 2 3 4 5 6 7 8 9]...");
    }

    #[test]
    fn message_line_includes_schema_name_when_present() {
        let message = sample_message(Some("Example"), vec![1, 2, 3]);
        assert_eq!(format_message_line(&message), "42 /demo [Example] [1 2 3]");
    }

    #[test]
    fn message_line_uses_no_schema_for_schemaless_channel() {
        let message = sample_message(None, vec![1, 2, 3]);
        assert_eq!(format_message_line(&message), "42 /demo [no schema] [1 2 3]");
    }
}
