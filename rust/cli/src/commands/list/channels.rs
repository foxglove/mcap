use anyhow::Result;
use clap::Args;
use serde::Serialize;
use serde_json;
use std::collections::BTreeMap;
use std::path::PathBuf;

use crate::utils::{io::read_mcap_summary, table::format_table};

#[derive(Args)]
pub struct ChannelsArgs {
    /// MCAP file to analyze
    pub file: PathBuf,

    /// Output as JSON
    #[arg(long)]
    pub json: bool,
}

#[derive(Serialize)]
struct SerializableChannel {
    id: u16,
    #[serde(rename = "schemaId")]
    schema_id: u16,
    topic: String,
    #[serde(rename = "messageEncoding")]
    message_encoding: String,
    metadata: BTreeMap<String, String>,
}

pub async fn run(args: ChannelsArgs) -> Result<()> {
    let path = args.file.to_string_lossy();
    let summary_opt = read_mcap_summary(&path)?;

    let summary = summary_opt.ok_or_else(|| anyhow::anyhow!("File has no summary section"))?;

    let mut channels: Vec<_> = summary.channels.values().collect();
    channels.sort_by_key(|channel| channel.id);

    if args.json {
        let serializable_channels: Vec<SerializableChannel> = channels
            .iter()
            .map(|channel| SerializableChannel {
                id: channel.id,
                schema_id: channel.schema.as_ref().map(|s| s.id).unwrap_or(0),
                topic: channel.topic.clone(),
                message_encoding: channel.message_encoding.clone(),
                metadata: channel.metadata.clone(),
            })
            .collect();

        let json_output = serde_json::to_string_pretty(&serializable_channels)?;
        println!("{}", json_output);
    } else {
        // Create table rows in the format expected by Go CLI
        let mut rows = vec![vec![
            "id".to_string(),
            "schemaId".to_string(),
            "topic".to_string(),
            "messageEncoding".to_string(),
            "metadata".to_string(),
        ]];

        for channel in channels {
            let metadata_json = serde_json::to_string(&channel.metadata)?;
            let schema_id = channel
                .schema
                .as_ref()
                .map(|schema| schema.id.to_string())
                .unwrap_or_else(|| "0".to_string());

            rows.push(vec![
                channel.id.to_string(),
                schema_id,
                channel.topic.clone(),
                channel.message_encoding.clone(),
                metadata_json,
            ]);
        }

        let mut stdout = std::io::stdout();
        format_table(&mut stdout, &rows)?;
    }

    Ok(())
}
