use anyhow::Result;
use clap::Args;
use serde::Serialize;
use serde_json;
use std::collections::BTreeMap;
use std::path::PathBuf;

use crate::utils::{io::read_mcap_summary, table::format_table};

#[derive(Args)]
pub struct ChunksArgs {
    /// MCAP file to analyze
    pub file: PathBuf,

    /// Output as JSON
    #[arg(long)]
    pub json: bool,
}

#[derive(Serialize)]
struct SerializableChunk {
    #[serde(rename = "messageStartTime")]
    message_start_time: u64,
    #[serde(rename = "messageEndTime")]
    message_end_time: u64,
    #[serde(rename = "chunkStartOffset")]
    chunk_start_offset: u64,
    #[serde(rename = "chunkLength")]
    chunk_length: u64,
    #[serde(rename = "messageIndexOffsets")]
    message_index_offsets: BTreeMap<u16, u64>,
    #[serde(rename = "messageIndexLength")]
    message_index_length: u64,
    compression: String,
    #[serde(rename = "compressedSize")]
    compressed_size: u64,
    #[serde(rename = "uncompressedSize")]
    uncompressed_size: u64,
}

pub async fn run(args: ChunksArgs) -> Result<()> {
    let path = args.file.to_string_lossy();
    let summary_opt = read_mcap_summary(&path)?;

    let summary = summary_opt.ok_or_else(|| anyhow::anyhow!("File has no summary section"))?;

    if args.json {
        let serializable_chunks: Vec<SerializableChunk> = summary
            .chunk_indexes
            .iter()
            .map(|chunk| SerializableChunk {
                message_start_time: chunk.message_start_time,
                message_end_time: chunk.message_end_time,
                chunk_start_offset: chunk.chunk_start_offset,
                chunk_length: chunk.chunk_length,
                message_index_offsets: chunk.message_index_offsets.clone(),
                message_index_length: chunk.message_index_length,
                compression: chunk.compression.clone(),
                compressed_size: chunk.compressed_size,
                uncompressed_size: chunk.uncompressed_size,
            })
            .collect();

        let json_output = serde_json::to_string_pretty(&serializable_chunks)?;
        println!("{}", json_output);
    } else {
        // Create table rows in the format expected by Go CLI
        let mut rows = vec![vec![
            "offset".to_string(),
            "length".to_string(),
            "start".to_string(),
            "end".to_string(),
            "compression".to_string(),
            "compressed size".to_string(),
            "uncompressed size".to_string(),
            "compression ratio".to_string(),
            "message index length".to_string(),
        ]];

        for chunk in &summary.chunk_indexes {
            let compression_name = if chunk.compression.is_empty() {
                "none"
            } else {
                &chunk.compression
            };

            let ratio = chunk.compressed_size as f64 / chunk.uncompressed_size as f64;

            rows.push(vec![
                chunk.chunk_start_offset.to_string(),
                chunk.chunk_length.to_string(),
                chunk.message_start_time.to_string(),
                chunk.message_end_time.to_string(),
                compression_name.to_string(),
                chunk.compressed_size.to_string(),
                chunk.uncompressed_size.to_string(),
                format!("{:.6}", ratio),
                chunk.message_index_length.to_string(),
            ]);
        }

        let mut stdout = std::io::stdout();
        format_table(&mut stdout, &rows)?;
    }

    Ok(())
}
