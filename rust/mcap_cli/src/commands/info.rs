use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::time::Duration;

use anyhow::Result;

use crate::cli::InfoCommand;
use crate::commands::common;
use crate::context::CommandContext;

pub fn run(_ctx: &CommandContext, args: InfoCommand) -> Result<()> {
    let mcap = common::map_file(&args.file)?;
    let parsed = common::parse_mcap(&mcap)?;
    print!("{}", render_info(&parsed));
    Ok(())
}

fn render_info(parsed: &common::ParsedMcap) -> String {
    let mut out = String::new();

    if let Some(header) = &parsed.header {
        let _ = writeln!(&mut out, "library: {}", header.library);
        let _ = writeln!(&mut out, "profile: {}", header.profile);
    } else {
        let _ = writeln!(&mut out, "library: unknown");
        let _ = writeln!(&mut out, "profile: unknown");
    }

    let mut duration_seconds = 0.0f64;
    if let Some(stats) = &parsed.statistics {
        let _ = writeln!(&mut out, "messages: {}", stats.message_count);
        let (duration_ns, signed_duration) =
            format_duration(stats.message_start_time, stats.message_end_time);
        duration_seconds = duration_ns / 1e9;
        let _ = writeln!(&mut out, "duration: {signed_duration}");
        let _ = writeln!(
            &mut out,
            "start: {}",
            common::formatted_time(stats.message_start_time)
        );
        let _ = writeln!(
            &mut out,
            "end: {}",
            common::formatted_time(stats.message_end_time)
        );
    }

    if !parsed.chunk_indexes.is_empty() {
        let mut by_compression: BTreeMap<&str, CompressionStats> = BTreeMap::new();
        let mut largest_compressed = 0u64;
        let mut largest_uncompressed = 0u64;

        for chunk in &parsed.chunk_indexes {
            let compression = chunk.compression.as_str();
            let stats = by_compression.entry(compression).or_default();
            stats.count += 1;
            stats.compressed_size += chunk.compressed_size;
            stats.uncompressed_size += chunk.uncompressed_size;

            largest_compressed = largest_compressed.max(chunk.compressed_size);
            largest_uncompressed = largest_uncompressed.max(chunk.uncompressed_size);
        }

        let (has_overlaps, max_active_chunks, max_total_uncompressed_size) =
            count_chunk_overlaps(&parsed.chunk_indexes);

        let _ = writeln!(&mut out, "compression:");
        let chunk_count = parsed.chunk_indexes.len();
        for (compression, stats) in by_compression {
            let ratio = if stats.uncompressed_size == 0 {
                0.0
            } else {
                100.0 * (1.0 - (stats.compressed_size as f64 / stats.uncompressed_size as f64))
            };

            let _ = write!(
                &mut out,
                "\t{compression}: [{}/{} chunks] [{}/{} ({ratio:.2}%)]",
                stats.count,
                chunk_count,
                human_bytes(stats.uncompressed_size),
                human_bytes(stats.compressed_size),
            );
            if duration_seconds > 0.0 {
                let throughput = (stats.compressed_size as f64 / duration_seconds).max(0.0);
                let _ = write!(&mut out, " [{}/sec]", human_bytes(throughput as u64));
            }
            let _ = writeln!(&mut out);
        }

        let _ = writeln!(&mut out, "chunks:");
        let _ = writeln!(
            &mut out,
            "\tmax uncompressed size: {}",
            human_bytes(largest_uncompressed)
        );
        let _ = writeln!(
            &mut out,
            "\tmax compressed size: {}",
            human_bytes(largest_compressed)
        );
        if has_overlaps {
            let _ = writeln!(
                &mut out,
                "\toverlaps: [max concurrent: {max_active_chunks}, decompressed: {}]",
                human_bytes(max_total_uncompressed_size)
            );
        } else {
            let _ = writeln!(&mut out, "\toverlaps: no");
        }
    }

    let _ = writeln!(&mut out, "channels:");
    let rows = render_channel_summary_rows(parsed, duration_seconds);
    out.push_str(&common::format_table(&rows));

    if let Some(stats) = &parsed.statistics {
        let _ = writeln!(&mut out, "channels: {}", stats.channel_count);
        let _ = writeln!(&mut out, "attachments: {}", stats.attachment_count);
        let _ = writeln!(&mut out, "metadata: {}", stats.metadata_count);
    } else {
        let _ = writeln!(&mut out, "channels: unknown");
        let _ = writeln!(&mut out, "attachments: unknown");
        let _ = writeln!(&mut out, "metadata: unknown");
    }

    out
}

fn render_channel_summary_rows(
    parsed: &common::ParsedMcap,
    duration_seconds: f64,
) -> Vec<Vec<String>> {
    let mut rows = Vec::new();
    for channel in parsed.channels.values() {
        let msg_col = if let Some(stats) = &parsed.statistics {
            let count = stats
                .channel_message_counts
                .get(&channel.id)
                .copied()
                .unwrap_or_default();
            if count > 1 && duration_seconds > 0.0 {
                let max_hz = count as f64 / duration_seconds;
                let min_hz = (count - 1) as f64 / duration_seconds;
                let delta = (max_hz - min_hz).max(f64::EPSILON);
                let precision = ((-delta.log10()).ceil() as i32).max(0) as usize;
                if precision > 2 {
                    format!("{count} msgs ({max_hz:.2}Hz)")
                } else {
                    format!("{count} msgs ({min_hz:.precision$}..{max_hz:.precision$}Hz)")
                }
            } else {
                format!("{count} msgs")
            }
        } else {
            String::new()
        };

        let schema_col = match channel.schema_id {
            0 => " : <no schema>".to_string(),
            id => match parsed.schemas.get(&id) {
                Some(schema) => {
                    format!(" : {} [{}]", schema.header.name, schema.header.encoding)
                }
                None => format!(" : <missing schema {id}>"),
            },
        };

        rows.push(vec![
            format!("\t({}) {}", channel.id, channel.topic),
            msg_col,
            schema_col,
        ]);
    }
    rows
}

fn format_duration(start: u64, end: u64) -> (f64, String) {
    let (diff, sign) = if end >= start {
        (end - start, "")
    } else {
        (start - end, "-")
    };
    let duration = Duration::from_nanos(diff);
    (diff as f64, format!("{sign}{duration:?}"))
}

fn human_bytes(num_bytes: u64) -> String {
    let prefixes = ["B", "KiB", "MiB", "GiB"];
    for (index, prefix) in prefixes.iter().enumerate() {
        let displayed = num_bytes as f64 / 1024f64.powi(index as i32);
        if displayed <= 1024.0 {
            return format!("{displayed:.2} {prefix}");
        }
    }

    let last = prefixes.len() - 1;
    let displayed = num_bytes as f64 / 1024f64.powi(last as i32);
    format!("{displayed:.2} {}", prefixes[last])
}

#[derive(Default)]
struct CompressionStats {
    count: usize,
    compressed_size: u64,
    uncompressed_size: u64,
}

#[derive(Debug, Clone, Copy)]
struct ChunkEvent {
    time: u64,
    is_start: bool,
    uncompressed_size: u64,
}

fn count_chunk_overlaps(chunks: &[mcap::records::ChunkIndex]) -> (bool, usize, u64) {
    if chunks.len() < 2 {
        if let Some(chunk) = chunks.first() {
            return (false, 1, chunk.uncompressed_size);
        }
        return (false, 0, 0);
    }

    let mut events = Vec::with_capacity(chunks.len() * 2);
    for chunk in chunks {
        events.push(ChunkEvent {
            time: chunk.message_start_time,
            is_start: true,
            uncompressed_size: chunk.uncompressed_size,
        });
        events.push(ChunkEvent {
            time: chunk.message_end_time,
            is_start: false,
            uncompressed_size: chunk.uncompressed_size,
        });
    }

    events.sort_by(|a, b| {
        a.time
            .cmp(&b.time)
            .then_with(|| b.is_start.cmp(&a.is_start))
    });

    let mut active = 0usize;
    let mut max_active = 0usize;
    let mut current_size = 0u64;
    let mut max_size = 0u64;

    for event in events {
        if event.is_start {
            active += 1;
            current_size += event.uncompressed_size;
            if active > max_active || (active == max_active && current_size > max_size) {
                max_active = active;
                max_size = current_size;
            }
        } else {
            active = active.saturating_sub(1);
            current_size = current_size.saturating_sub(event.uncompressed_size);
        }
    }

    (max_active > 1, max_active, max_size)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::{count_chunk_overlaps, human_bytes, render_info};
    use crate::commands::common;
    use crate::commands::common::{ParsedMcap, ParsedSchema};
    use mcap::records::{self, ChunkIndex, Header, Statistics};

    #[test]
    fn decimal_time_formats_nanos() {
        assert_eq!(common::decimal_time(1_234_567_890), "1.234567890");
    }

    #[test]
    fn formatted_time_includes_rfc3339_and_decimal() {
        assert_eq!(
            common::formatted_time(1_000_000_000),
            "1970-01-01T00:00:01.000000000Z (1.000000000)"
        );
    }

    #[test]
    fn human_bytes_scales_units() {
        assert_eq!(human_bytes(2), "2.00 B");
        assert_eq!(human_bytes(2 * 1024), "2.00 KiB");
    }

    #[test]
    fn overlaps_counts_concurrent_chunks() {
        let chunks = vec![
            ChunkIndex {
                message_start_time: 10,
                message_end_time: 20,
                chunk_start_offset: 0,
                chunk_length: 0,
                message_index_offsets: BTreeMap::new(),
                message_index_length: 0,
                compression: "zstd".to_string(),
                compressed_size: 10,
                uncompressed_size: 100,
            },
            ChunkIndex {
                message_start_time: 15,
                message_end_time: 25,
                chunk_start_offset: 0,
                chunk_length: 0,
                message_index_offsets: BTreeMap::new(),
                message_index_length: 0,
                compression: "zstd".to_string(),
                compressed_size: 15,
                uncompressed_size: 80,
            },
        ];
        let (has_overlaps, max_active, max_size) = count_chunk_overlaps(&chunks);
        assert!(has_overlaps);
        assert_eq!(max_active, 2);
        assert_eq!(max_size, 180);
    }

    #[test]
    fn info_render_includes_core_sections() {
        let mut parsed = ParsedMcap {
            header: Some(Header {
                profile: "demo".to_string(),
                library: "mcap-rust".to_string(),
            }),
            statistics: Some(Statistics {
                message_count: 2,
                channel_count: 1,
                attachment_count: 0,
                metadata_count: 0,
                chunk_count: 1,
                message_start_time: 1_000_000_000,
                message_end_time: 2_500_000_000,
                ..Statistics::default()
            }),
            ..ParsedMcap::default()
        };
        parsed.schemas.insert(
            1,
            ParsedSchema {
                header: records::SchemaHeader {
                    id: 1,
                    name: "demo_schema".to_string(),
                    encoding: "jsonschema".to_string(),
                },
                data: br#"{"type":"object"}"#.to_vec(),
            },
        );
        parsed.channels.insert(
            7,
            records::Channel {
                id: 7,
                schema_id: 1,
                topic: "/demo".to_string(),
                message_encoding: "json".to_string(),
                metadata: BTreeMap::new(),
            },
        );

        let rendered = render_info(&parsed);
        assert!(rendered.contains("library: mcap-rust"));
        assert!(rendered.contains("profile: demo"));
        assert!(rendered.contains("messages: 2"));
        assert!(rendered.contains("channels:"));
        assert!(rendered.contains("/demo"));
    }
}
