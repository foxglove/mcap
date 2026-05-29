use anyhow::{Context, Result};

use crate::cli::ListMetadataCommand;
use crate::commands::common;
use crate::context::CommandContext;

pub fn run(ctx: &CommandContext, args: ListMetadataCommand) -> Result<()> {
    let records = if let Some(remote) = common::try_open_remote_mcap(&args.file)? {
        collect_remote_metadata_records(&remote)?
    } else {
        let mcap = common::load_path(
            &args.file,
            common::SourceOptions::new(ctx.allow_remote_scan()),
        )?;
        collect_metadata_records(&mcap)?
    };
    common::print_table(&render_metadata_rows(&records)?);
    Ok(())
}


fn collect_remote_metadata_records(
    remote: &common::RemoteMcap,
) -> Result<Vec<(mcap::records::MetadataIndex, mcap::records::Metadata)>> {
    let mut records = Vec::new();
    for index in remote.summary().metadata_indexes.clone() {
        let bytes = remote.read_range(
            index.offset,
            usize::try_from(index.length).context("metadata record is too large to read on this platform")?,
        )?;
        let metadata = parse_metadata_record(&bytes)
            .with_context(|| format!("failed to read metadata at offset {}", index.offset))?;
        records.push((index, metadata));
    }
    Ok(records)
}

fn parse_metadata_record(bytes: &[u8]) -> Result<mcap::records::Metadata> {
    let mut reader = mcap::read::LinearReader::sans_magic(bytes);
    let metadata = match reader.next().ok_or(mcap::McapError::BadIndex)?? {
        mcap::records::Record::Metadata(metadata) => metadata,
        _ => return Err(mcap::McapError::BadIndex.into()),
    };
    if reader.next().is_some() {
        return Err(mcap::McapError::BadIndex.into());
    }
    Ok(metadata)
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
