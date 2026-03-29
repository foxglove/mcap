use std::collections::BTreeMap;

use anyhow::{Context, Result};
use mcap::Summary;

use crate::{cli::GetMetadataArgs, cli_io::open_local_mcap, output::print_rows};

pub fn run(args: GetMetadataArgs) -> Result<()> {
    let bytes = open_local_mcap(&args.file)?;
    let summary = Summary::read(&bytes)?
        .ok_or_else(|| anyhow::anyhow!("failed to read metadata list: file has no summary"))?;

    let matching_indexes = summary
        .metadata_indexes
        .into_iter()
        .filter(|idx| idx.name == args.name)
        .collect::<Vec<_>>();
    if matching_indexes.is_empty() {
        anyhow::bail!("metadata '{}' does not exist", args.name);
    }

    let mut merged = BTreeMap::new();
    for index in matching_indexes {
        let metadata = mcap::read::metadata(&bytes, &index)
            .with_context(|| format!("failed to read metadata at offset {}", index.offset))?;
        for (k, v) in metadata.metadata {
            merged.insert(k, v);
        }
    }

    let mut rows = Vec::with_capacity(merged.len() + 1);
    rows.push(vec!["key".to_string(), "value".to_string()]);
    for (k, v) in merged {
        rows.push(vec![k, v]);
    }
    print_rows(&rows)
}

#[cfg(test)]
mod tests {
    use super::run;
    use crate::cli::GetMetadataArgs;
    use std::path::PathBuf;

    #[test]
    fn get_metadata_returns_missing_file_error() {
        let err = run(GetMetadataArgs {
            file: PathBuf::from("missing-file.mcap"),
            name: "test".to_string(),
        })
        .expect_err("missing file should fail");
        assert!(err.to_string().contains("failed to read file"));
    }
}
