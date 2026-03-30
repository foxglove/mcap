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
    let mut out = ParsedMcap::default();

    for record in mcap::read::ChunkFlattener::new(mcap)? {
        match record? {
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
    }

    Ok(out)
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
    for row in rows {
        let mut line = String::new();
        for (idx, value) in row.iter().enumerate() {
            if idx > 0 {
                line.push('\t');
            }
            let width = widths[idx];
            let _ = write!(&mut line, "{value:<width$}");
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

    use super::{format_table, parse_mcap, print_table};
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
}
