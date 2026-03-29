use std::collections::BTreeMap;

use anyhow::Result;
use mcap::Summary;

use crate::{cli::InputFile, cli_io, output};

pub fn run(args: InputFile) -> Result<()> {
    let bytes = cli_io::open_local_mcap(&args.file)?;
    let summary = Summary::read(&bytes)?
        .ok_or_else(|| anyhow::anyhow!("failed to read summary from {}", args.file.display()))?;

    let mut rows = vec![
        vec!["file".to_string(), args.file.display().to_string()],
        vec![
            "library".to_string(),
            extract_library(&bytes).unwrap_or_else(|| "unknown".to_string()),
        ],
        vec![
            "messages".to_string(),
            summary
                .stats
                .as_ref()
                .map(|s| s.message_count.to_string())
                .unwrap_or_else(|| "unknown".to_string()),
        ],
        vec!["channels".to_string(), summary.channels.len().to_string()],
        vec!["schemas".to_string(), summary.schemas.len().to_string()],
        vec![
            "chunks".to_string(),
            summary.chunk_indexes.len().to_string(),
        ],
        vec![
            "attachments".to_string(),
            summary.attachment_indexes.len().to_string(),
        ],
        vec![
            "metadata".to_string(),
            summary.metadata_indexes.len().to_string(),
        ],
    ];

    if let Some(stats) = &summary.stats {
        rows.push(vec![
            "start".to_string(),
            format_decimal_time(stats.message_start_time),
        ]);
        rows.push(vec![
            "end".to_string(),
            format_decimal_time(stats.message_end_time),
        ]);
    }

    output::print_rows(&rows)
}

fn extract_library(bytes: &[u8]) -> Option<String> {
    let mut reader = mcap::read::LinearReader::new(bytes).ok()?;
    match reader.next()? {
        Ok(mcap::records::Record::Header(header)) => Some(header.library),
        _ => None,
    }
}

fn format_decimal_time(nanos: u64) -> String {
    let seconds = nanos / 1_000_000_000;
    let subnanos = nanos % 1_000_000_000;
    format!("{seconds}.{subnanos:09}")
}

#[allow(dead_code)]
pub fn channel_rows(summary: &Summary) -> Vec<Vec<String>> {
    let mut channels: BTreeMap<u16, _> = BTreeMap::new();
    for (id, channel) in &summary.channels {
        channels.insert(*id, channel);
    }

    let mut rows = vec![vec![
        "id".to_string(),
        "schemaId".to_string(),
        "topic".to_string(),
        "messageEncoding".to_string(),
    ]];

    for (id, channel) in channels {
        let schema_id = channel.schema.as_ref().map(|s| s.id).unwrap_or(0);
        rows.push(vec![
            id.to_string(),
            schema_id.to_string(),
            channel.topic.clone(),
            channel.message_encoding.clone(),
        ]);
    }
    rows
}

#[allow(dead_code)]
pub fn schema_rows(summary: &Summary) -> Vec<Vec<String>> {
    let mut schemas: BTreeMap<u16, _> = BTreeMap::new();
    for (id, schema) in &summary.schemas {
        schemas.insert(*id, schema);
    }

    let mut rows = vec![vec![
        "id".to_string(),
        "name".to_string(),
        "encoding".to_string(),
        "data_len".to_string(),
    ]];

    for (id, schema) in schemas {
        rows.push(vec![
            id.to_string(),
            schema.name.clone(),
            schema.encoding.clone(),
            schema.data.len().to_string(),
        ]);
    }
    rows
}

#[allow(dead_code)]
pub fn chunk_rows(summary: &Summary) -> Vec<Vec<String>> {
    let mut rows = vec![vec![
        "offset".to_string(),
        "length".to_string(),
        "start".to_string(),
        "end".to_string(),
        "compression".to_string(),
        "compressed_size".to_string(),
        "uncompressed_size".to_string(),
        "message_index_length".to_string(),
    ]];
    for chunk in &summary.chunk_indexes {
        rows.push(vec![
            chunk.chunk_start_offset.to_string(),
            chunk.chunk_length.to_string(),
            chunk.message_start_time.to_string(),
            chunk.message_end_time.to_string(),
            chunk.compression.clone(),
            chunk.compressed_size.to_string(),
            chunk.uncompressed_size.to_string(),
            chunk.message_index_length.to_string(),
        ]);
    }
    rows
}

#[allow(dead_code)]
pub fn attachment_rows(summary: &Summary) -> Vec<Vec<String>> {
    let mut rows = vec![vec![
        "name".to_string(),
        "media_type".to_string(),
        "log_time".to_string(),
        "create_time".to_string(),
        "content_length".to_string(),
        "offset".to_string(),
    ]];

    for att in &summary.attachment_indexes {
        rows.push(vec![
            att.name.clone(),
            att.media_type.clone(),
            att.log_time.to_string(),
            att.create_time.to_string(),
            att.data_size.to_string(),
            att.offset.to_string(),
        ]);
    }
    rows
}

#[allow(dead_code)]
pub fn metadata_rows(summary: &Summary) -> Vec<Vec<String>> {
    let mut rows = vec![vec![
        "name".to_string(),
        "offset".to_string(),
        "length".to_string(),
    ]];

    for metadata in &summary.metadata_indexes {
        rows.push(vec![
            metadata.name.clone(),
            metadata.offset.to_string(),
            metadata.length.to_string(),
        ]);
    }
    rows
}

#[cfg(test)]
mod tests {
    use super::{format_decimal_time, metadata_rows};

    #[test]
    fn decimal_time_formatting() {
        assert_eq!(format_decimal_time(0), "0.000000000");
        assert_eq!(format_decimal_time(1_234_567_890), "1.234567890");
    }

    #[test]
    fn metadata_rows_have_header() {
        let summary = mcap::Summary::default();
        let rows = metadata_rows(&summary);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0], vec!["name", "offset", "length"]);
    }
}
