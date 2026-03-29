use anyhow::Result;
use mcap::Summary;

use crate::{cli::InputFile, cli_io, output};

pub fn run(args: InputFile) -> Result<()> {
    let bytes = cli_io::open_local_mcap(&args.file)?;
    let summary = Summary::read(&bytes)?
        .ok_or_else(|| anyhow::anyhow!("failed to read metadata list: file has no summary"))?;

    let mut indexes = summary.metadata_indexes;
    indexes.sort_by_key(|m| (m.offset, m.name.clone()));

    let mut rows: Vec<Vec<String>> = Vec::with_capacity(indexes.len() + 1);
    rows.push(vec![
        "name".to_string(),
        "offset".to_string(),
        "length".to_string(),
        "metadata".to_string(),
    ]);

    for idx in indexes {
        let metadata = mcap::read::metadata(&bytes, &idx)
            .map(|m| {
                m.metadata
                    .iter()
                    .map(|(k, v)| format!("{k}={v}"))
                    .collect::<Vec<_>>()
                    .join(";")
            })
            .unwrap_or_else(|_| "<unreadable>".to_string());
        rows.push(vec![
            idx.name,
            idx.offset.to_string(),
            idx.length.to_string(),
            metadata,
        ]);
    }

    output::print_rows(&rows)
}
