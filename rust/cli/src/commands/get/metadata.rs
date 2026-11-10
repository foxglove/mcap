use std::collections::BTreeMap;

use anyhow::{Context, Result};

use crate::byte_source::{self, ByteSource};
use crate::cli::GetMetadataCommand;
use crate::context::CommandContext;
use crate::{parse, source};

pub fn run(ctx: &CommandContext, args: GetMetadataCommand) -> Result<()> {
    let source_options = source::SourceOptions::new(ctx.allow_remote_scan());
    let mut input = byte_source::open_byte_source(&args.file, source_options)?;
    let indexes: Vec<mcap::records::MetadataIndex> =
        parse::top_level_indexes(input.as_mut(), Some(&args.name))?;
    let metadata = merged_metadata_for_name(input.as_mut(), &indexes, &args.name)?;
    let pretty =
        serde_json::to_string_pretty(&metadata).context("failed to serialize metadata to JSON")?;
    println!("{pretty}");
    Ok(())
}

fn merged_metadata_for_name(
    source: &mut dyn ByteSource,
    indexes: &[mcap::records::MetadataIndex],
    name: &str,
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
    source::require_remote_indexed_read_budget(&*source, total_bytes, "remote metadata records")?;

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

    use mcap::records::MetadataIndex;

    use super::merged_metadata_for_name;
    use crate::byte_source::MemorySource;

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
        let err = merged_metadata_for_name(&mut source, &[metadata_index("demo", 0, 0)], "other")
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
        let latest =
            merged_metadata_for_name(&mut source, &[second.clone(), first.clone()], "config")
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
}
