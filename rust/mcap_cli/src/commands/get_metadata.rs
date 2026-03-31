use std::collections::BTreeMap;

use anyhow::{Context, Result};

use crate::cli::GetMetadataCommand;
use crate::commands::common;
use crate::context::CommandContext;

pub fn run(_ctx: &CommandContext, args: GetMetadataCommand) -> Result<()> {
    let mcap = common::map_file(&args.file)?;
    let parsed = common::parse_mcap(&mcap)?;
    let metadata = latest_metadata_for_name(&mcap, &parsed.metadata_indexes, &args.name)?;
    let pretty =
        serde_json::to_string_pretty(&metadata).context("failed to serialize metadata to JSON")?;
    println!("{pretty}");
    Ok(())
}

fn latest_metadata_for_name(
    mcap: &[u8],
    indexes: &[mcap::records::MetadataIndex],
    name: &str,
) -> Result<BTreeMap<String, String>> {
    let index = indexes
        .iter()
        .filter(|index| index.name == name)
        .max_by_key(|index| index.offset)
        .ok_or_else(|| anyhow::anyhow!("metadata {name} does not exist"))?;

    let record = mcap::read::metadata(mcap, index)
        .with_context(|| format!("failed to read metadata at offset {}", index.offset))?;
    Ok(record.metadata)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use mcap::records::MetadataIndex;

    use super::latest_metadata_for_name;

    fn metadata_index(name: &str, offset: u64, length: u64) -> MetadataIndex {
        MetadataIndex {
            offset,
            length,
            name: name.to_string(),
        }
    }

    #[test]
    fn errors_when_metadata_name_missing() {
        let err = latest_metadata_for_name(
            &[],
            &[metadata_index("demo", 0, 0)],
            "other",
        )
        .expect_err("missing metadata should fail");
        assert_eq!(err.to_string(), "metadata other does not exist");
    }

    #[test]
    fn returns_latest_metadata_record_by_offset() {
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

        let latest = latest_metadata_for_name(
            &mcap_bytes,
            &[second.clone(), first.clone()],
            "config",
        )
        .expect("metadata should resolve to latest");
        assert_eq!(
            latest,
            BTreeMap::from([
                ("b".to_string(), "2".to_string()),
                ("c".to_string(), "3".to_string()),
            ])
        );
    }
}
