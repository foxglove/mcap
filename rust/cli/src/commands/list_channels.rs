use std::collections::BTreeMap;

use anyhow::Result;
use mcap::Summary;

use crate::{cli::InputFile, cli_io, output};

pub fn run(args: InputFile) -> Result<()> {
    let data = cli_io::open_local_mcap(&args.file)?;
    let summary = Summary::read(&data)?;

    let mut rows = Vec::new();
    rows.push(vec![
        "id".to_string(),
        "schemaId".to_string(),
        "topic".to_string(),
        "messageEncoding".to_string(),
        "metadata".to_string(),
    ]);

    if let Some(summary) = summary {
        let ordered = summary.channels.iter().collect::<BTreeMap<_, _>>();
        for (id, channel) in ordered {
            let metadata = channel
                .metadata
                .iter()
                .map(|(k, v)| format!("{k}={v}"))
                .collect::<Vec<_>>()
                .join(";");
            let schema_id = channel.schema.as_ref().map(|s| s.id).unwrap_or(0);
            rows.push(vec![
                id.to_string(),
                schema_id.to_string(),
                channel.topic.clone(),
                channel.message_encoding.clone(),
                metadata,
            ]);
        }
    }

    output::print_rows(&rows)
}
