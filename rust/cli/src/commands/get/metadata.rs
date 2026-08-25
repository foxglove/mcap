use std::collections::BTreeMap;

use anyhow::{Context, Result};

use crate::byte_source::{self, ByteSource};
use crate::cli::GetMetadataCommand;
use crate::context::CommandContext;
use crate::{parse, source};

pub fn run(ctx: &CommandContext, args: GetMetadataCommand) -> Result<()> {
    let source_options = source::SourceOptions::new(ctx.allow_remote_scan());
    let mut input = byte_source::open_byte_source(Some(&args.file), source_options)?;
    let indexes = metadata_indexes(input.as_mut(), &args.name, source_options)?;
    let metadata = merged_metadata_for_name(input.as_mut(), &indexes, &args.name, source_options)?;
    let pretty =
        serde_json::to_string_pretty(&metadata).context("failed to serialize metadata to JSON")?;
    println!("{pretty}");
    Ok(())
}

fn metadata_indexes(
    source: &mut dyn ByteSource,
    name: &str,
    source_options: source::SourceOptions,
) -> Result<Vec<mcap::records::MetadataIndex>> {
    let header = byte_source::read_header(source)?;
    let parsed = match parse::try_parsed_mcap_from_summary(source, header.clone())? {
        Some(parsed) => parsed,
        None => {
            source::require_remote_scan_for_linear(source, source_options)?;
            return Ok(
                parse::parse_mcap_linear_from_byte_source(source, header)?.metadata_indexes,
            );
        }
    };
    let missing_requested_name = !parsed
        .metadata_indexes
        .iter()
        .any(|index| index.name == name);
    if parse::metadata_indexes_need_scan(&parsed)
        || (missing_requested_name && parsed.summary_available && parsed.statistics.is_none())
    {
        parse::warn_index_scan("metadata");
        source::require_remote_scan_for_linear(source, source_options)?;
        return parse::collect_metadata_indexes_from_byte_source(source);
    }
    Ok(parsed.metadata_indexes)
}

fn merged_metadata_for_name(
    source: &mut dyn ByteSource,
    indexes: &[mcap::records::MetadataIndex],
    name: &str,
    source_options: source::SourceOptions,
) -> Result<BTreeMap<String, String>> {
    let mut matching_indexes: Vec<&mcap::records::MetadataIndex> =
        indexes.iter().filter(|index| index.name == name).collect();
    if matching_indexes.is_empty() {
        anyhow::bail!("metadata {name} does not exist");
    }
    matching_indexes.sort_by_key(|index| index.offset);

    let total_bytes = matching_indexes
        .iter()
        .fold(0u64, |total, index| total.saturating_add(index.length));
    if matching_indexes.len() > 1 || source.is_remote() {
        source::require_remote_indexed_read_budget(
            total_bytes,
            source_options,
            "remote metadata records",
        )?;
    }

    let mut output = BTreeMap::new();
    for index in matching_indexes {
        let length = usize::try_from(index.length)
            .context("indexed record is too large to read on this platform")?;
        let bytes = source.read_at(index.offset, length)?;
        let record = parse::parse_metadata_record(&bytes)
            .with_context(|| format!("failed to read metadata at offset {}", index.offset))?;
        for (key, value) in record.metadata {
            output.insert(key, value);
        }
    }
    Ok(output)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use mcap::records::{MetadataIndex, Statistics};

    use super::{merged_metadata_for_name, metadata_indexes};
    use crate::byte_source::MemorySource;
    use crate::parse;
    use crate::source::SourceOptions;

    fn metadata_index(name: &str, offset: u64, length: u64) -> MetadataIndex {
        MetadataIndex {
            offset,
            length,
            name: name.to_string(),
        }
    }

    #[test]
    fn errors_when_metadata_name_missing() {
        let mut source = MemorySource::new(Vec::new());
        let err = merged_metadata_for_name(
            &mut source,
            &[metadata_index("demo", 0, 0)],
            "other",
            SourceOptions::default(),
        )
        .expect_err("missing metadata should fail");
        assert_eq!(err.to_string(), "metadata other does not exist");
    }

    #[test]
    fn merges_metadata_records_by_offset_order() {
        let mut mcap_bytes = Vec::new();
        let (first, second) = {
            let mut writer = mcap::WriteOptions::new()
                .emit_metadata_indexes(true)
                .emit_summary_records(true)
                .emit_summary_offsets(true)
                .create(std::io::Cursor::new(&mut mcap_bytes))
                .expect("writer");
            writer
                .write_metadata(&mcap::records::Metadata {
                    name: "config".to_string(),
                    metadata: BTreeMap::from([
                        ("a".to_string(), "1".to_string()),
                        ("b".to_string(), "1".to_string()),
                    ]),
                })
                .expect("first metadata");
            writer
                .write_metadata(&mcap::records::Metadata {
                    name: "config".to_string(),
                    metadata: BTreeMap::from([
                        ("b".to_string(), "2".to_string()),
                        ("c".to_string(), "3".to_string()),
                    ]),
                })
                .expect("second metadata");
            let summary = writer.finish().expect("finish");
            let mut indexes: Vec<MetadataIndex> = summary.metadata_indexes;
            indexes.sort_by_key(|index| index.offset);
            (indexes[0].clone(), indexes[1].clone())
        };

        let mut source = MemorySource::new(mcap_bytes);
        let latest = merged_metadata_for_name(
            &mut source,
            &[second.clone(), first.clone()],
            "config",
            SourceOptions::default(),
        )
        .expect("metadata should merge");
        assert_eq!(
            latest,
            BTreeMap::from([
                ("a".to_string(), "1".to_string()),
                ("b".to_string(), "2".to_string()),
                ("c".to_string(), "3".to_string()),
            ])
        );
    }

    #[test]
    fn missing_name_does_not_scan_when_metadata_indexes_are_complete() {
        let mut mcap_bytes = Vec::new();
        {
            let mut writer = mcap::WriteOptions::new()
                .emit_metadata_indexes(true)
                .emit_summary_records(true)
                .emit_summary_offsets(true)
                .create(std::io::Cursor::new(&mut mcap_bytes))
                .expect("writer");
            writer
                .write_metadata(&mcap::records::Metadata {
                    name: "demo".to_string(),
                    metadata: BTreeMap::from([("foo".to_string(), "bar".to_string())]),
                })
                .expect("metadata");
            writer.finish().expect("finish");
        }
        let mut source = MemorySource::new(mcap_bytes);
        let indexes =
            metadata_indexes(&mut source, "missing", SourceOptions::default()).expect("indexes");
        assert_eq!(indexes.len(), 1);
        assert_eq!(indexes[0].name, "demo");
        let _ = parse::ParsedMcap {
            summary_available: true,
            statistics: Some(Statistics {
                metadata_count: 1,
                ..Default::default()
            }),
            metadata_indexes: vec![metadata_index("demo", 10, 20)],
            ..Default::default()
        };
    }

    #[test]
    fn missing_name_does_not_rescan_summaryless_input() {
        let parsed = parse::ParsedMcap {
            metadata_indexes: vec![metadata_index("demo", 10, 20)],
            ..Default::default()
        };
        assert!(!parse::metadata_indexes_need_scan(&parsed));
        let missing_requested_name = !parsed
            .metadata_indexes
            .iter()
            .any(|index| index.name == "missing");
        assert!(missing_requested_name);
        assert!(!(missing_requested_name && parsed.summary_available && parsed.statistics.is_none()));
    }

    #[test]
    fn missing_name_scans_when_metadata_index_completeness_is_unknown() {
        let mut mcap_bytes = Vec::new();
        {
            let mut writer = mcap::WriteOptions::new()
                .emit_summary_records(false)
                .emit_summary_offsets(false)
                .emit_metadata_indexes(false)
                .create(std::io::Cursor::new(&mut mcap_bytes))
                .expect("writer");
            writer
                .write_metadata(&mcap::records::Metadata {
                    name: "demo".to_string(),
                    metadata: BTreeMap::from([("foo".to_string(), "bar".to_string())]),
                })
                .expect("metadata");
            writer.finish().expect("finish");
        }
        let mut source = MemorySource::new(mcap_bytes);
        let indexes =
            metadata_indexes(&mut source, "missing", SourceOptions::default()).expect("scan");
        assert_eq!(indexes.len(), 1);
        assert_eq!(indexes[0].name, "demo");
    }
}
