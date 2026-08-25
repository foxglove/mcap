use anyhow::{Context, Result};

use crate::byte_source::{self, ByteSource};
use crate::cli::ListMetadataCommand;
use crate::context::CommandContext;
use crate::{parse, render, source};

pub fn run(ctx: &CommandContext, args: ListMetadataCommand) -> Result<()> {
    let source_options = source::SourceOptions::new(ctx.allow_remote_scan());
    let mut input = byte_source::open_byte_source(Some(&args.file), source_options)?;
    let records = collect_metadata_records(input.as_mut(), source_options)?;
    render::print_table(&render_metadata_rows(&records)?);
    Ok(())
}

fn collect_metadata_records(
    source: &mut dyn ByteSource,
    source_options: source::SourceOptions,
) -> Result<Vec<(mcap::records::MetadataIndex, mcap::records::Metadata)>> {
    let header = byte_source::read_header(source)?;
    let indexes = match parse::try_parsed_mcap_from_summary(source, header.clone())? {
        Some(parsed) if parse::metadata_indexes_need_scan(&parsed) => {
            parse::warn_index_scan("metadata");
            source::require_remote_scan_for_linear(source, source_options)?;
            parse::collect_metadata_indexes_from_byte_source(source)?
        }
        Some(parsed) => parsed.metadata_indexes,
        None => {
            source::require_remote_scan_for_linear(source, source_options)?;
            parse::parse_mcap_linear_from_byte_source(source, header)?.metadata_indexes
        }
    };

    let total_bytes = indexes
        .iter()
        .fold(0u64, |total, index| total.saturating_add(index.length));
    if source.is_remote() {
        source::require_remote_indexed_read_budget(
            total_bytes,
            source_options,
            "remote metadata records",
        )?;
    }

    let mut records = Vec::new();
    for index in indexes {
        let length = usize::try_from(index.length)
            .context("indexed record is too large to read on this platform")?;
        let bytes = source.read_at(index.offset, length)?;
        let metadata = parse::parse_metadata_record(&bytes)
            .with_context(|| format!("failed to read metadata at offset {}", index.offset))?;
        records.push((index, metadata));
    }
    Ok(records)
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
