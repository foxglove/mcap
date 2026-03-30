use std::fmt::Write as _;
use std::path::Path;

use anyhow::{bail, Context, Result};
use mcap::records::{self, Record};
use memmap2::Mmap;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedSchema {
    pub header: records::SchemaHeader,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ParsedMcap {
    pub header: Option<records::Header>,
    pub statistics: Option<records::Statistics>,
    pub channels: std::collections::BTreeMap<u16, records::Channel>,
    pub schemas: std::collections::BTreeMap<u16, ParsedSchema>,
    pub chunk_indexes: Vec<records::ChunkIndex>,
    pub attachment_indexes: Vec<records::AttachmentIndex>,
    pub metadata_indexes: Vec<records::MetadataIndex>,
}

pub fn map_file(path: &Path) -> anyhow::Result<Mmap> {
    let file =
        std::fs::File::open(path).with_context(|| format!("couldn't open '{}'", path.display()))?;
    unsafe { Mmap::map(&file) }.with_context(|| format!("couldn't map '{}'", path.display()))
}

pub fn parse_mcap(mcap: &[u8]) -> Result<ParsedMcap> {
    let header = read_header(mcap)?;
    if let Some(parsed_from_summary) = parse_mcap_from_summary(mcap, header.clone())? {
        return Ok(parsed_from_summary);
    }

    eprintln!(
        "Warning: summary section not available; full scan may be slow. Run `mcap doctor` for details."
    );
    parse_mcap_linear(mcap, header)
}

fn read_header(mcap: &[u8]) -> Result<Option<records::Header>> {
    let mut reader = mcap::read::LinearReader::new(mcap)?;
    match reader.next() {
        Some(Ok(Record::Header(header))) => Ok(Some(header)),
        Some(Ok(_)) | None => Ok(None),
        Some(Err(err)) => Err(err.into()),
    }
}

fn parse_mcap_from_summary(
    mcap: &[u8],
    header: Option<records::Header>,
) -> Result<Option<ParsedMcap>> {
    let Some(summary) = mcap::Summary::read(mcap)? else {
        return Ok(None);
    };

    let mut out = ParsedMcap {
        header,
        statistics: summary.stats,
        channels: std::collections::BTreeMap::new(),
        schemas: std::collections::BTreeMap::new(),
        chunk_indexes: summary.chunk_indexes,
        attachment_indexes: summary.attachment_indexes,
        metadata_indexes: summary.metadata_indexes,
    };

    for schema in summary.schemas.values() {
        let schema = schema.as_ref();
        out.schemas.insert(
            schema.id,
            ParsedSchema {
                header: records::SchemaHeader {
                    id: schema.id,
                    name: schema.name.clone(),
                    encoding: schema.encoding.clone(),
                },
                data: schema.data.clone().into_owned(),
            },
        );
    }

    for channel in summary.channels.values() {
        let channel = channel.as_ref();
        out.channels.insert(
            channel.id,
            records::Channel {
                id: channel.id,
                schema_id: channel.schema.as_ref().map(|schema| schema.id).unwrap_or(0),
                topic: channel.topic.clone(),
                message_encoding: channel.message_encoding.clone(),
                metadata: channel.metadata.clone(),
            },
        );
    }

    Ok(Some(out))
}

fn parse_mcap_linear(mcap: &[u8], header: Option<records::Header>) -> Result<ParsedMcap> {
    let mut out = ParsedMcap {
        header,
        ..ParsedMcap::default()
    };
    for record in mcap::read::LinearReader::new(mcap)? {
        let record = record?;
        if let Record::Chunk { header, data } = record {
            for nested_record in mcap::read::ChunkReader::new(header, data.as_ref())? {
                collect_record(&mut out, nested_record?)?;
            }
        } else {
            collect_record(&mut out, record)?;
        }
    }

    Ok(out)
}

fn collect_record(out: &mut ParsedMcap, record: Record<'_>) -> Result<()> {
    match record {
        Record::Header(header) => {
            if let Some(existing) = &out.header {
                if existing != &header {
                    bail!("conflicting MCAP header records");
                }
            } else {
                out.header = Some(header);
            }
        }
        Record::Statistics(statistics) => {
            out.statistics = Some(statistics);
        }
        Record::Channel(channel) => {
            if let Some(existing) = out.channels.get(&channel.id) {
                if existing != &channel {
                    bail!("conflicting channel definition for id {}", channel.id);
                }
            } else {
                out.channels.insert(channel.id, channel);
            }
        }
        Record::Schema { header, data } => {
            let schema = ParsedSchema {
                header,
                data: data.into_owned(),
            };
            if let Some(existing) = out.schemas.get(&schema.header.id) {
                if existing != &schema {
                    bail!("conflicting schema definition for id {}", schema.header.id);
                }
            } else {
                out.schemas.insert(schema.header.id, schema);
            }
        }
        Record::ChunkIndex(index) => out.chunk_indexes.push(index),
        Record::AttachmentIndex(index) => out.attachment_indexes.push(index),
        Record::MetadataIndex(index) => out.metadata_indexes.push(index),
        _ => {}
    }
    Ok(())
}

pub fn decimal_time(t: u64) -> String {
    format!("{}.{:09}", t / 1_000_000_000, t % 1_000_000_000)
}

pub fn formatted_time(t: u64) -> String {
    let seconds = (t / 1_000_000_000) as i64;
    let nanos = (t % 1_000_000_000) as u32;
    match chrono::DateTime::from_timestamp(seconds, nanos) {
        Some(dt) => format!(
            "{} ({})",
            format_rfc3339_trimmed(dt),
            decimal_time(t)
        ),
        None => decimal_time(t),
    }
}

fn format_rfc3339_trimmed(dt: chrono::DateTime<chrono::Utc>) -> String {
    let rendered = dt.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true);
    let Some(without_z) = rendered.strip_suffix('Z') else {
        return rendered;
    };

    let Some((prefix, fractional)) = without_z.split_once('.') else {
        return rendered;
    };

    let trimmed = fractional.trim_end_matches('0');
    if trimmed.is_empty() {
        format!("{prefix}Z")
    } else {
        format!("{prefix}.{trimmed}Z")
    }
}

pub fn format_table(rows: &[Vec<String>]) -> String {
    if rows.is_empty() {
        return String::new();
    }

    let mut widths = vec![0usize; rows[0].len()];
    for row in rows {
        for (idx, value) in row.iter().enumerate() {
            widths[idx] = widths[idx].max(value.chars().count());
        }
    }

    let mut out = String::new();
    let last_col_idx = rows[0].len().saturating_sub(1);
    for row in rows {
        let mut line = String::new();
        for (idx, value) in row.iter().enumerate() {
            if idx > 0 {
                line.push('\t');
            }
            if idx == last_col_idx {
                line.push_str(value);
            } else {
                let width = widths[idx];
                let _ = write!(&mut line, "{value:<width$}");
            }
        }
        let _ = writeln!(&mut out, "{line}");
    }
    out
}

pub fn print_table(rows: &[Vec<String>]) {
    let rendered = format_table(rows);
    if rendered.is_empty() {
        return;
    }
    print!("{rendered}");
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::{
        decimal_time, format_table, formatted_time, parse_mcap, parse_mcap_from_summary,
        print_table,
    };
    use mcap::records;

    #[test]
    fn table_printer_handles_empty_input() {
        print_table(&[]);
        assert!(format_table(&[]).is_empty());
    }

    #[test]
    fn table_formatter_aligns_columns() {
        let rows = vec![
            vec!["id".to_string(), "topic".to_string()],
            vec!["7".to_string(), "/foo".to_string()],
            vec!["12".to_string(), "/barbaz".to_string()],
        ];
        let rendered = format_table(&rows);
        let lines: Vec<&str> = rendered.lines().collect();
        assert_eq!(lines.len(), 3);
        assert!(lines[0].starts_with("id"));
        assert!(lines[1].contains('\t'));
        assert!(lines[2].contains("/barbaz"));
    }

    #[test]
    fn formatted_time_includes_rfc3339_and_decimal() {
        assert_eq!(
            formatted_time(1_000_000_000),
            "1970-01-01T00:00:01Z (1.000000000)"
        );
        assert_eq!(decimal_time(1_234_567_890), "1.234567890");
        assert_eq!(
            formatted_time(1_234_567_890),
            "1970-01-01T00:00:01.23456789Z (1.234567890)"
        );
    }

    #[test]
    fn table_formatter_omits_trailing_whitespace() {
        let rows = vec![
            vec!["col1".to_string(), "col2".to_string()],
            vec!["a".to_string(), "b".to_string()],
        ];
        let rendered = format_table(&rows);
        for line in rendered.lines() {
            assert!(!line.ends_with(' '));
            assert!(!line.ends_with('\t'));
        }
    }

    #[test]
    fn parse_mcap_collects_channels_and_schemas() {
        let mut buffer = Vec::new();
        let (schema_id, channel_id) = {
            let mut writer = mcap::Writer::new(std::io::Cursor::new(&mut buffer)).expect("writer");
            let schema_id = writer
                .add_schema("demo_schema", "jsonschema", br#"{"type":"object"}"#)
                .expect("schema");
            let channel_id = writer
                .add_channel(schema_id, "/demo", "json", &BTreeMap::new())
                .expect("channel");
            writer
                .write_to_known_channel(
                    &records::MessageHeader {
                        channel_id,
                        sequence: 1,
                        log_time: 10,
                        publish_time: 11,
                    },
                    br#"{"k":"v"}"#,
                )
                .expect("write message");
            writer.finish().expect("finish writer");
            (schema_id, channel_id)
        };

        let parsed = parse_mcap(&buffer).expect("parse mcap");
        assert!(parsed.header.is_some());
        assert!(parsed.channels.contains_key(&channel_id));
        assert!(parsed.schemas.contains_key(&schema_id));
    }

    #[test]
    fn parse_mcap_falls_back_for_summaryless_files() {
        let mut buffer = Vec::new();
        let (schema_id, channel_id) = {
            let mut writer = mcap::WriteOptions::new()
                .emit_summary_records(false)
                .emit_summary_offsets(false)
                .create(std::io::Cursor::new(&mut buffer))
                .expect("writer");
            let schema_id = writer
                .add_schema("demo_schema", "jsonschema", br#"{"type":"object"}"#)
                .expect("schema");
            let channel_id = writer
                .add_channel(schema_id, "/demo", "json", &BTreeMap::new())
                .expect("channel");
            writer
                .write_to_known_channel(
                    &records::MessageHeader {
                        channel_id,
                        sequence: 1,
                        log_time: 10,
                        publish_time: 11,
                    },
                    br#"{"k":"v"}"#,
                )
                .expect("write message");
            writer.finish().expect("finish writer");
            (schema_id, channel_id)
        };

        let parsed = parse_mcap(&buffer).expect("parse mcap");
        assert!(parsed.header.is_some());
        assert!(parsed.channels.contains_key(&channel_id));
        assert!(parsed.schemas.contains_key(&schema_id));
    }

    #[test]
    fn parse_mcap_from_summary_accepts_empty_summary() {
        let mut buffer = Vec::new();
        {
            let mut writer = mcap::Writer::new(std::io::Cursor::new(&mut buffer)).expect("writer");
            writer.finish().expect("finish writer");
        }

        let parsed = parse_mcap_from_summary(&buffer, None).expect("parse from summary");
        assert!(parsed.is_some());
        let parsed = parsed.expect("parsed summary output");
        if let Some(stats) = &parsed.statistics {
            assert_eq!(stats.message_count, 0);
            assert_eq!(stats.channel_count, 0);
            assert_eq!(stats.attachment_count, 0);
            assert_eq!(stats.metadata_count, 0);
        }
        assert!(parsed.channels.is_empty());
        assert!(parsed.schemas.is_empty());
        assert!(parsed.chunk_indexes.is_empty());
        assert!(parsed.attachment_indexes.is_empty());
        assert!(parsed.metadata_indexes.is_empty());
    }
}
