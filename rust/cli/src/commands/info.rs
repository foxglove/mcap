use anyhow::Result;
use chrono::{DateTime, Utc};
use clap::Args;

use std::collections::HashMap;
use std::path::PathBuf;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::utils::{format::human_bytes, io::read_mcap_summary, table::format_summary_rows};

#[derive(Args)]
pub struct InfoArgs {
    /// MCAP file to analyze
    pub file: PathBuf,
}

fn decimal_time(time_nanos: u64) -> String {
    let seconds = time_nanos / 1_000_000_000;
    let nanoseconds = time_nanos % 1_000_000_000;
    format!("{}.{:09}", seconds, nanoseconds)
}

fn format_time(time_nanos: u64) -> String {
    let system_time = UNIX_EPOCH + Duration::from_nanos(time_nanos);
    let datetime: DateTime<Utc> = system_time.into();

    // Check if time is reasonably recent (not "long ago")
    let long_ago = SystemTime::now() - Duration::from_secs(20 * 365 * 24 * 3600); // 20 years ago

    if system_time > long_ago {
        format!(
            "{} ({})",
            datetime.format("%Y-%m-%dT%H:%M:%S%.9fZ"),
            decimal_time(time_nanos)
        )
    } else {
        decimal_time(time_nanos)
    }
}

fn get_duration_ns(start: u64, end: u64) -> f64 {
    if end >= start {
        (end - start) as f64
    } else {
        -((start - end) as f64)
    }
}

struct CompressionStats {
    count: usize,
    compressed_size: u64,
    uncompressed_size: u64,
}

pub async fn run(args: InfoArgs) -> Result<()> {
    let path = args.file.to_string_lossy();
    let summary_opt = read_mcap_summary(&path)?;

    let summary = summary_opt.ok_or_else(|| anyhow::anyhow!("File has no summary section"))?;

    let mut rows = vec![
        ("library:".to_string(), "".to_string()), // Header not available in summary
        ("profile:".to_string(), "".to_string()), // Header not available in summary
    ];

    let mut start_time = 0u64;
    let mut end_time = 0u64;
    let mut duration_seconds = 0.0;

    if let Some(stats) = &summary.stats {
        rows.push(("messages:".to_string(), stats.message_count.to_string()));

        start_time = stats.message_start_time;
        end_time = stats.message_end_time;
        let duration_ns = get_duration_ns(start_time, end_time);
        duration_seconds = duration_ns / 1e9;

        if duration_ns.abs() > i64::MAX as f64 {
            rows.push(("duration:".to_string(), format!("{:.3}s", duration_seconds)));
        } else {
            let duration = Duration::from_nanos(duration_ns.abs() as u64);
            rows.push(("duration:".to_string(), format!("{:?}", duration)));
        }

        rows.push(("start:".to_string(), format_time(start_time)));
        rows.push(("end:".to_string(), format_time(end_time)));
    }

    // Print basic info
    format_summary_rows(&rows);

    // Print compression information
    if !summary.chunk_indexes.is_empty() {
        let mut compression_stats: HashMap<String, CompressionStats> = HashMap::new();
        let mut largest_chunk_compressed = 0u64;
        let mut largest_chunk_uncompressed = 0u64;

        for chunk in &summary.chunk_indexes {
            let compression_name = if chunk.compression.is_empty() {
                "none".to_string()
            } else {
                chunk.compression.clone()
            };

            let stats = compression_stats
                .entry(compression_name)
                .or_insert(CompressionStats {
                    count: 0,
                    compressed_size: 0,
                    uncompressed_size: 0,
                });

            stats.count += 1;
            stats.compressed_size += chunk.compressed_size;
            stats.uncompressed_size += chunk.uncompressed_size;

            if chunk.compressed_size > largest_chunk_compressed {
                largest_chunk_compressed = chunk.compressed_size;
            }
            if chunk.uncompressed_size > largest_chunk_uncompressed {
                largest_chunk_uncompressed = chunk.uncompressed_size;
            }
        }

        println!("compression:");
        let chunk_count = summary.chunk_indexes.len();

        for (compression, stats) in compression_stats {
            let compression_ratio =
                100.0 * (1.0 - stats.compressed_size as f64 / stats.uncompressed_size as f64);
            print!(
                "\t{}: [{}/{}] chunks] ",
                compression, stats.count, chunk_count
            );
            print!(
                "[{}/{} ({:.2}%)] ",
                human_bytes(stats.uncompressed_size),
                human_bytes(stats.compressed_size),
                compression_ratio
            );

            if duration_seconds > 0.0 {
                print!(
                    "[{}/sec] ",
                    human_bytes((stats.compressed_size as f64 / duration_seconds) as u64)
                );
            }
            println!();
        }

        println!("chunks:");
        println!(
            "\tmax uncompressed size: {}",
            human_bytes(largest_chunk_uncompressed)
        );
        println!(
            "\tmax compressed size: {}",
            human_bytes(largest_chunk_compressed)
        );
        println!("\toverlaps: no"); // TODO: Implement overlap detection
    }

    // Print channel information
    println!("channels:");

    if summary.channels.is_empty() {
        println!("\t(no channels)");
    } else {
        let mut channel_ids: Vec<_> = summary.channels.keys().collect();
        channel_ids.sort();

        for &channel_id in &channel_ids {
            let channel = &summary.channels[channel_id];
            let schema = channel.schema.as_ref();

            print!("\t({})\t{}", channel.id, channel.topic);

            if let Some(stats) = &summary.stats {
                if let Some(&message_count) = stats.channel_message_counts.get(channel_id) {
                    if message_count > 1 && duration_seconds > 0.0 {
                        let frequency =
                            (message_count as f64) * 1e9 / get_duration_ns(start_time, end_time);
                        print!(" {} msgs ({:.2} Hz)", message_count, frequency);
                    } else {
                        print!(" {} msgs", message_count);
                    }
                }
            }

            match schema {
                Some(schema) => {
                    println!(" : {} [{}]", schema.name, schema.encoding);
                }
                None => {
                    println!(" : <no schema>");
                }
            }
        }
    }

    // Print summary stats
    if let Some(stats) = &summary.stats {
        println!("channels: {}", stats.channel_count);
        println!("attachments: {}", stats.attachment_count);
        println!("metadata: {}", stats.metadata_count);
    } else {
        println!("channels: unknown");
        println!("attachments: unknown");
        println!("metadata: unknown");
    }

    Ok(())
}
