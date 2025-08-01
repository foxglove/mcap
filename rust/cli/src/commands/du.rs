use crate::error::CliResult;
use crate::utils::{format_bytes, format_table, validation::validate_input_file};
use clap::Args;
use mcap::{MessageStream, Summary};
use memmap2::Mmap;
use std::collections::HashMap;
use std::fs::File;
use std::io::Write;

#[derive(Args)]
pub struct DuArgs {
    /// Path to MCAP file
    file: String,

    /// Show disk usage by topic
    #[arg(long)]
    by_topic: bool,

    /// Show disk usage by schema
    #[arg(long)]
    by_schema: bool,

    /// Show detailed breakdown
    #[arg(long)]
    detailed: bool,
}

struct DiskUsage {
    total_size: u64,
    compressed_size: u64,
    uncompressed_size: u64,
    message_count: u64,
    chunk_count: usize,
    attachment_count: usize,
    metadata_count: usize,
    schema_count: usize,
    channel_count: usize,
}

pub async fn execute(args: DuArgs) -> CliResult<()> {
    // Validate input file
    validate_input_file(&args.file)?;

    // Get file metadata
    let file_metadata = std::fs::metadata(&args.file)?;
    let file_size = file_metadata.len();

    // Memory map the file
    let file = File::open(&args.file)?;
    let mapped = unsafe { Mmap::map(&file)? };

    // Read the summary from the file
    let summary = Summary::read(&mapped)?;

    // Analyze disk usage
    let usage = analyze_usage(&summary, file_size)?;

    // Print results
    print_usage(&mut std::io::stdout(), &usage, &args)?;

    // If requested, show usage by topic or schema
    if args.by_topic {
        analyze_by_topic(&mapped).await?;
    }

    if args.by_schema {
        analyze_by_schema(&mapped, &summary).await?;
    }

    Ok(())
}

fn analyze_usage(summary: &Option<Summary>, file_size: u64) -> CliResult<DiskUsage> {
    if let Some(info) = summary {
        let compressed_size = info
            .chunk_indexes
            .iter()
            .map(|chunk| chunk.compressed_size)
            .sum::<u64>();

        let uncompressed_size = info
            .chunk_indexes
            .iter()
            .map(|chunk| chunk.uncompressed_size)
            .sum::<u64>();

        let message_count = if let Some(stats) = &info.stats {
            stats.message_count
        } else {
            0
        };

        Ok(DiskUsage {
            total_size: file_size,
            compressed_size,
            uncompressed_size,
            message_count,
            chunk_count: info.chunk_indexes.len(),
            attachment_count: info.attachment_indexes.len(),
            metadata_count: info.metadata_indexes.len(),
            schema_count: info.schemas.len(),
            channel_count: info.channels.len(),
        })
    } else {
        Ok(DiskUsage {
            total_size: file_size,
            compressed_size: 0,
            uncompressed_size: 0,
            message_count: 0,
            chunk_count: 0,
            attachment_count: 0,
            metadata_count: 0,
            schema_count: 0,
            channel_count: 0,
        })
    }
}

fn print_usage<W: Write>(writer: &mut W, usage: &DiskUsage, args: &DuArgs) -> std::io::Result<()> {
    writeln!(writer, "MCAP File Disk Usage Analysis")?;
    writeln!(writer, "==============================")?;
    writeln!(writer)?;

    writeln!(
        writer,
        "Total file size: {}",
        format_bytes(usage.total_size)
    )?;

    if usage.compressed_size > 0 && usage.uncompressed_size > 0 {
        let compression_ratio = usage.compressed_size as f64 / usage.uncompressed_size as f64;
        writeln!(
            writer,
            "Compressed data: {}",
            format_bytes(usage.compressed_size)
        )?;
        writeln!(
            writer,
            "Uncompressed data: {}",
            format_bytes(usage.uncompressed_size)
        )?;
        writeln!(
            writer,
            "Compression ratio: {:.1}%",
            compression_ratio * 100.0
        )?;
        writeln!(
            writer,
            "Space saved: {}",
            format_bytes(usage.uncompressed_size - usage.compressed_size)
        )?;
    }

    writeln!(writer)?;
    writeln!(writer, "Record counts:")?;
    writeln!(writer, "  Messages: {}", usage.message_count)?;
    writeln!(writer, "  Schemas: {}", usage.schema_count)?;
    writeln!(writer, "  Channels: {}", usage.channel_count)?;
    writeln!(writer, "  Chunks: {}", usage.chunk_count)?;
    writeln!(writer, "  Attachments: {}", usage.attachment_count)?;
    writeln!(writer, "  Metadata: {}", usage.metadata_count)?;

    if args.detailed {
        writeln!(writer)?;
        writeln!(writer, "Detailed breakdown:")?;

        if usage.message_count > 0 {
            let avg_message_size = usage.uncompressed_size / usage.message_count;
            writeln!(
                writer,
                "  Average message size: {}",
                format_bytes(avg_message_size)
            )?;
        }

        if usage.chunk_count > 0 {
            let avg_chunk_size = usage.compressed_size / usage.chunk_count as u64;
            writeln!(
                writer,
                "  Average chunk size: {}",
                format_bytes(avg_chunk_size)
            )?;
        }

        // Calculate overhead (file size minus compressed data)
        let overhead = usage.total_size.saturating_sub(usage.compressed_size);
        let overhead_percentage = if usage.total_size > 0 {
            (overhead as f64 / usage.total_size as f64) * 100.0
        } else {
            0.0
        };
        writeln!(
            writer,
            "  File overhead: {} ({:.1}%)",
            format_bytes(overhead),
            overhead_percentage
        )?;
    }

    Ok(())
}

async fn analyze_by_topic(mapped: &[u8]) -> CliResult<()> {
    println!("\nDisk usage by topic:");
    println!("===================");

    let mut topic_usage = HashMap::new();
    let message_stream = MessageStream::new(mapped)?;

    for message_result in message_stream {
        let message = message_result?;
        let entry = topic_usage
            .entry(message.channel.topic.clone())
            .or_insert((0u64, 0u64));
        entry.0 += 1; // message count
        entry.1 += message.data.len() as u64; // data size
    }

    // Convert to sorted vector
    let mut topics: Vec<_> = topic_usage.into_iter().collect();
    topics.sort_by_key(|(_, (_, size))| std::cmp::Reverse(*size));

    let headers = vec!["Topic", "Messages", "Data Size"];
    let rows: Vec<Vec<String>> = topics
        .into_iter()
        .map(|(topic, (count, size))| vec![topic, count.to_string(), format_bytes(size)])
        .collect();

    format_table(&mut std::io::stdout(), headers, rows)?;

    Ok(())
}

async fn analyze_by_schema(mapped: &[u8], summary: &Option<Summary>) -> CliResult<()> {
    println!("\nDisk usage by schema:");
    println!("====================");

    if let Some(info) = summary {
        let mut schema_usage = HashMap::new();
        let message_stream = MessageStream::new(mapped)?;

        for message_result in message_stream {
            let message = message_result?;
            let schema_name = if let Some(schema) = &message.channel.schema {
                schema.name.clone()
            } else {
                "No schema".to_string()
            };

            let entry = schema_usage.entry(schema_name).or_insert((0u64, 0u64));
            entry.0 += 1; // message count
            entry.1 += message.data.len() as u64; // data size
        }

        // Convert to sorted vector
        let mut schemas: Vec<_> = schema_usage.into_iter().collect();
        schemas.sort_by_key(|(_, (_, size))| std::cmp::Reverse(*size));

        let headers = vec!["Schema", "Messages", "Data Size"];
        let rows: Vec<Vec<String>> = schemas
            .into_iter()
            .map(|(schema, (count, size))| vec![schema, count.to_string(), format_bytes(size)])
            .collect();

        format_table(&mut std::io::stdout(), headers, rows)?;
    } else {
        println!("No schema information available");
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_du_args() {
        let args = DuArgs {
            file: "test.mcap".to_string(),
            by_topic: false,
            by_schema: false,
            detailed: false,
        };
        assert_eq!(args.file, "test.mcap");
        assert!(!args.by_topic);
        assert!(!args.by_schema);
        assert!(!args.detailed);
    }
}
