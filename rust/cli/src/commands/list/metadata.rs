use anyhow::{Context, Result};

use crate::cli::ListMetadataCommand;
use crate::context::CommandContext;
use crate::{parse, render, source};

pub fn run(ctx: &CommandContext, args: ListMetadataCommand) -> Result<()> {
    let source_options = source::SourceOptions::new(ctx.allow_remote_scan());
    let records = if let Some(remote) = source::try_open_remote_mcap(&args.file, source_options)? {
        collect_remote_metadata_records(&remote, source_options)?
    } else {
        let mcap = source::load_path(&args.file, source_options)?;
        collect_metadata_records(&mcap)?
    };
    render::print_table(&render_metadata_rows(&records)?);
    Ok(())
}

fn collect_remote_metadata_records(
    remote: &source::RemoteMcap,
    source_options: source::SourceOptions,
) -> Result<Vec<(mcap::records::MetadataIndex, mcap::records::Metadata)>> {
    let total_bytes = remote
        .summary()
        .metadata_indexes
        .iter()
        .map(|index| index.length)
        .sum::<u64>();
    source::require_remote_metadata_budget(total_bytes, source_options, "metadata records")?;

    let mut records = Vec::new();
    for index in &remote.summary().metadata_indexes {
        let bytes = remote.read_range(
            index.offset,
            usize::try_from(index.length)
                .context("indexed record is too large to read on this platform")?,
        )?;
        let metadata = parse::parse_metadata_record(&bytes)
            .with_context(|| format!("failed to read metadata at offset {}", index.offset))?;
        records.push((index.clone(), metadata));
    }
    Ok(records)
}

fn collect_metadata_records(
    mcap: &[u8],
) -> Result<Vec<(mcap::records::MetadataIndex, mcap::records::Metadata)>> {
    let mut records = Vec::new();
    let parsed = parse::parse_mcap(mcap)?;
    let indexes = if metadata_indexes_need_scan(&parsed) {
        parse::collect_metadata_indexes_linear(mcap)?
    } else {
        parsed.metadata_indexes
    };
    for index in indexes {
        let metadata = mcap::read::metadata(mcap, &index)
            .with_context(|| format!("failed to read metadata at offset {}", index.offset))?;
        records.push((index, metadata));
    }
    Ok(records)
}

fn metadata_indexes_need_scan(parsed: &parse::ParsedMcap) -> bool {
    match &parsed.statistics {
        Some(statistics) => statistics.metadata_count as usize > parsed.metadata_indexes.len(),
        None => parsed.metadata_indexes.is_empty(),
    }
}

fn render_metadata_rows(
    records: &[(mcap::records::MetadataIndex, mcap::records::Metadata)],
) -> Result<Vec<Vec<String>>> {
    let mut rows = vec![vec![
        "name".to_string(),
        "offset".to_string(),
        "length".to_string(),
        "metadata".to_string(),
    ]];

    for (index, metadata) in records {
        let metadata_json = serde_json::to_string(&metadata.metadata)
            .context("failed to serialize metadata map")?;
        rows.push(vec![
            metadata.name.clone(),
            index.offset.to_string(),
            index.length.to_string(),
            metadata_json,
        ]);
    }
    Ok(rows)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::render_metadata_rows;
    use mcap::records::{Metadata, MetadataIndex};

    #[test]
    fn render_rows_includes_metadata_json() {
        let rows = render_metadata_rows(&[(
            MetadataIndex {
                offset: 7,
                length: 42,
                name: "demo".to_string(),
            },
            Metadata {
                name: "demo".to_string(),
                metadata: BTreeMap::from([
                    ("a".to_string(), "1".to_string()),
                    ("b".to_string(), "2".to_string()),
                ]),
            },
        )])
        .expect("rows");

        assert_eq!(rows[0], ["name", "offset", "length", "metadata"]);
        assert_eq!(rows[1][0], "demo");
        assert_eq!(rows[1][1], "7");
        assert_eq!(rows[1][2], "42");
        assert!(rows[1][3].contains(r#""a":"1""#));
    }
}
