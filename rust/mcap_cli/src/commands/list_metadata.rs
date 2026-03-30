use anyhow::{Context, Result};

use crate::cli::ListMetadataCommand;
use crate::commands::common;
use crate::context::CommandContext;

pub fn run(_ctx: &CommandContext, args: ListMetadataCommand) -> Result<()> {
    let mcap = common::map_file(&args.file)?;
    let records = collect_metadata_records(&mcap)?;
    common::print_table(&render_metadata_rows(&records)?);
    Ok(())
}

fn collect_metadata_records(
    mcap: &[u8],
) -> Result<Vec<(mcap::records::MetadataIndex, mcap::records::Metadata)>> {
    let mut records = Vec::new();
    let parsed = common::parse_mcap(mcap)?;
    for index in parsed.metadata_indexes {
        let metadata = mcap::read::metadata(mcap, &index)
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
