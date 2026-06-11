use std::collections::BTreeMap;

use anyhow::{Context, Result};

use crate::cli::GetMetadataCommand;
use crate::context::CommandContext;
use crate::{parse, source};

pub fn run(ctx: &CommandContext, args: GetMetadataCommand) -> Result<()> {
    let source_options = source::SourceOptions::new(ctx.allow_remote_scan());
    let metadata = if let Some(remote) = source::try_open_remote_mcap(&args.file, source_options)? {
        merged_remote_metadata_for_name(&remote, &args.name, source_options)?
    } else {
        let mcap = source::load_path(&args.file, source_options)?;
        let parsed = parse::parse_mcap(&mcap)?;
        let indexes = local_metadata_indexes(&mcap, parsed, &args.name)?;
        merged_metadata_for_name(&mcap, &indexes, &args.name)?
    };
    let pretty =
        serde_json::to_string_pretty(&metadata).context("failed to serialize metadata to JSON")?;
    println!("{pretty}");
    Ok(())
}

fn merged_remote_metadata_for_name(
    remote: &source::RemoteMcap,
    name: &str,
    source_options: source::SourceOptions,
) -> Result<BTreeMap<String, String>> {
    let mut matching_indexes: Vec<&mcap::records::MetadataIndex> = remote
        .summary()
        .metadata_indexes
        .iter()
        .filter(|index| index.name == name)
        .collect();
    if matching_indexes.is_empty() {
        anyhow::bail!("metadata {name} does not exist");
    }
    matching_indexes.sort_by_key(|index| index.offset);
    if matching_indexes.len() > 1 {
        let total_bytes = matching_indexes
            .iter()
            .map(|index| index.length)
            .sum::<u64>();
        source::require_remote_metadata_budget(total_bytes, source_options, "metadata records")?;
    }

    let mut output = BTreeMap::new();
    for index in matching_indexes {
        let bytes = remote.read_range(
            index.offset,
            usize::try_from(index.length)
                .context("indexed record is too large to read on this platform")?,
        )?;
        let record = parse::parse_metadata_record(&bytes)
            .with_context(|| format!("failed to read metadata at offset {}", index.offset))?;
        for (key, value) in record.metadata {
            output.insert(key, value);
        }
    }
    Ok(output)
}

fn local_metadata_indexes(
    mcap: &[u8],
    parsed: parse::ParsedMcap,
    name: &str,
) -> Result<Vec<mcap::records::MetadataIndex>> {
    let missing_requested_name = !parsed
        .metadata_indexes
        .iter()
        .any(|index| index.name == name);
    if parse::metadata_indexes_need_scan(&parsed)
        || (missing_requested_name && parsed.statistics.is_none())
    {
        return parse::collect_metadata_indexes_linear(mcap);
    }
    Ok(parsed.metadata_indexes)
}

fn merged_metadata_for_name(
    mcap: &[u8],
    indexes: &[mcap::records::MetadataIndex],
    name: &str,
) -> Result<BTreeMap<String, String>> {
    let mut matching_indexes: Vec<&mcap::records::MetadataIndex> =
        indexes.iter().filter(|index| index.name == name).collect();
    if matching_indexes.is_empty() {
        anyhow::bail!("metadata {name} does not exist");
    }
    matching_indexes.sort_by_key(|index| index.offset);

    let mut output = BTreeMap::new();
    for index in matching_indexes {
        let record = mcap::read::metadata(mcap, index)
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

    use super::{local_metadata_indexes, merged_metadata_for_name};
    use crate::parse;

    fn metadata_index(name: &str, offset: u64, length: u64) -> MetadataIndex {
        MetadataIndex {
            offset,
            length,
            name: name.to_string(),
        }
    }

    #[test]
    fn errors_when_metadata_name_missing() {
        let err = merged_metadata_for_name(&[], &[metadata_index("demo", 0, 0)], "other")
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

        let latest =
            merged_metadata_for_name(&mcap_bytes, &[second.clone(), first.clone()], "config")
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
        let parsed = parse::ParsedMcap {
            statistics: Some(Statistics {
                metadata_count: 1,
                ..Default::default()
            }),
            metadata_indexes: vec![metadata_index("demo", 10, 20)],
            ..Default::default()
        };

        let indexes =
            local_metadata_indexes(&[], parsed, "missing").expect("complete index is enough");
        assert_eq!(indexes.len(), 1);
        assert_eq!(indexes[0].name, "demo");
    }
}
