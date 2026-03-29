use std::{
    collections::{BTreeMap, HashSet},
    io::{self, Read},
    sync::Arc,
};

use anyhow::{Context, Result};
use mcap::{read::MessageStream, Channel};
use serde::Serialize;

use crate::{
    cli::CatArgs,
    cli_io::{open_local_mcap, reading_stdin},
    time::parse_date_or_nanos,
};

pub fn run(args: CatArgs) -> Result<()> {
    let topic_filter = parse_topic_filter(args.topics.as_deref());
    let start = parse_optional_time(args.start.as_deref())?.unwrap_or(0);
    let end = parse_optional_time(args.end.as_deref())?.unwrap_or(u64::MAX);

    if end < start {
        anyhow::bail!("invalid time range query, end-time is before start-time");
    }

    let mut inputs = Vec::new();
    if args.files.is_empty() {
        if !reading_stdin()? {
            anyhow::bail!("supply a file");
        }
        let mut stdin_bytes = Vec::new();
        io::stdin()
            .read_to_end(&mut stdin_bytes)
            .context("failed to read stdin")?;
        inputs.push(("<stdin>".to_string(), stdin_bytes));
    } else {
        for path in args.files {
            let bytes = open_local_mcap(&path)?;
            inputs.push((path.display().to_string(), bytes));
        }
    }

    for (name, bytes) in inputs {
        let stream = MessageStream::new(&bytes)
            .with_context(|| format!("failed to read messages from {name}"))?;
        if args.json {
            print_messages_json(stream, &topic_filter, start, end)?;
        } else {
            print_messages_text(stream, &topic_filter, start, end)?;
        }
    }

    Ok(())
}

fn parse_topic_filter(topics: Option<&str>) -> Option<HashSet<String>> {
    topics.map(|v| {
        v.split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(ToOwned::to_owned)
            .collect::<HashSet<_>>()
    })
}

fn parse_optional_time(value: Option<&str>) -> Result<Option<u64>> {
    value.map(parse_date_or_nanos).transpose()
}

fn should_include(
    channel: &Arc<Channel<'static>>,
    topic_filter: &Option<HashSet<String>>,
    log_time: u64,
    start: u64,
    end: u64,
) -> bool {
    if log_time < start || log_time >= end {
        return false;
    }
    if let Some(topics) = topic_filter {
        topics.contains(&channel.topic)
    } else {
        true
    }
}

fn print_messages_text(
    stream: MessageStream<'_>,
    topic_filter: &Option<HashSet<String>>,
    start: u64,
    end: u64,
) -> Result<()> {
    for message in stream {
        let message = message.context("failed reading message")?;
        if !should_include(&message.channel, topic_filter, message.log_time, start, end) {
            continue;
        }

        let schema_name = message
            .channel
            .schema
            .as_ref()
            .map(|s| s.name.as_str())
            .unwrap_or("no schema");
        println!(
            "{} {} [{}] {} bytes",
            message.log_time,
            message.channel.topic,
            schema_name,
            message.data.len()
        );
    }
    Ok(())
}

fn print_messages_json(
    stream: MessageStream<'_>,
    topic_filter: &Option<HashSet<String>>,
    start: u64,
    end: u64,
) -> Result<()> {
    for message in stream {
        let message = message.context("failed reading message")?;
        if !should_include(&message.channel, topic_filter, message.log_time, start, end) {
            continue;
        }

        let schema_name = message
            .channel
            .schema
            .as_ref()
            .map(|s| s.name.clone())
            .unwrap_or_else(|| "no schema".to_string());

        let row = JsonMessage {
            topic: message.channel.topic.clone(),
            sequence: message.sequence,
            log_time: format_decimal_time(message.log_time),
            publish_time: format_decimal_time(message.publish_time),
            schema: schema_name,
            data_len: message.data.len(),
            metadata: message.channel.metadata.clone(),
        };
        println!(
            "{}",
            serde_json::to_string(&row).context("failed serializing message JSON")?
        );
    }
    Ok(())
}

fn format_decimal_time(nanos: u64) -> String {
    let seconds = nanos / 1_000_000_000;
    let subnanos = nanos % 1_000_000_000;
    format!("{seconds}.{subnanos:09}")
}

#[derive(Serialize)]
struct JsonMessage {
    topic: String,
    sequence: u32,
    log_time: String,
    publish_time: String,
    schema: String,
    data_len: usize,
    metadata: BTreeMap<String, String>,
}

#[cfg(test)]
mod tests {
    use super::parse_topic_filter;

    #[test]
    fn parses_topic_filters() {
        let filters = parse_topic_filter(Some("/a,/b,, /c")).expect("filters should parse");
        assert!(filters.contains("/a"));
        assert!(filters.contains("/b"));
        assert!(filters.contains("/c"));
    }
}
