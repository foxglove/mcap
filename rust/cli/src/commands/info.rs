use std::collections::BTreeMap;
use std::fmt::Write as _;

use anyhow::Result;

use crate::cli::InfoCommand;
use crate::context::CommandContext;
use crate::{parse, render, source};

pub fn run(ctx: &CommandContext, args: InfoCommand) -> Result<()> {
    let parsed = source::parse_mcap_from_path(
        &args.file,
        source::SourceOptions::new(ctx.allow_remote_scan()).scan_data_without_statistics(true),
    )?;
    print!("{}", render_info(&parsed));
    Ok(())
}

fn render_info(parsed: &parse::ParsedMcap) -> String {
    let mut out = String::new();

    if let Some(header) = &parsed.header {
        let _ = writeln!(&mut out, "library:     {}", header.library);
        let _ = writeln!(&mut out, "profile:     {}", header.profile);
    } else {
        let _ = writeln!(&mut out, "library:     unknown");
        let _ = writeln!(&mut out, "profile:     unknown");
    }

    let message_count = message_count(parsed);
    let message_time_range = message_time_range(parsed);
    let mut duration_seconds = 0.0f64;
    if let Some(count) = message_count {
        let _ = writeln!(&mut out, "messages:    {count}");
    }
    if let Some((message_start_time, message_end_time)) = message_time_range {
        let (duration_ns, signed_duration) = format_duration(message_start_time, message_end_time);
        duration_seconds = duration_ns / 1e9;
        let _ = writeln!(&mut out, "duration:    {signed_duration}");
        let _ = writeln!(
            &mut out,
            "start:       {}",
            render::formatted_time(message_start_time)
        );
        let _ = writeln!(
            &mut out,
            "end:         {}",
            render::formatted_time(message_end_time)
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
                render::human_bytes(stats.uncompressed_size),
                render::human_bytes(stats.compressed_size),
            );
            if duration_seconds > 0.0 {
                let throughput = (stats.compressed_size as f64 / duration_seconds).max(0.0);
                let _ = write!(
                    &mut out,
                    " [{}/sec]",
                    render::human_bytes(throughput as u64)
                );
            }
            let _ = writeln!(&mut out);
        }

        let _ = writeln!(&mut out, "chunks:");
        let _ = writeln!(
            &mut out,
            "\tmax uncompressed size: {}",
            render::human_bytes(largest_uncompressed)
        );
        let _ = writeln!(
            &mut out,
            "\tmax compressed size: {}",
            render::human_bytes(largest_compressed)
        );
        if has_overlaps {
            let _ = writeln!(
                &mut out,
                "\toverlaps: [max concurrent: {max_active_chunks}, decompressed: {}]",
                render::human_bytes(max_total_uncompressed_size)
            );
        } else {
            let _ = writeln!(&mut out, "\toverlaps: no");
        }
    }

    let _ = writeln!(&mut out, "channels:");
    let rows = render_channel_summary_rows(parsed, duration_seconds);
    out.push_str(&render::format_table(&rows));

    let _ = writeln!(&mut out, "channels:    {}", channel_count(parsed));
    let _ = writeln!(&mut out, "attachments: {}", attachment_count(parsed));
    let _ = writeln!(&mut out, "metadata:    {}", metadata_count(parsed));

    out
}

fn message_count(parsed: &parse::ParsedMcap) -> Option<u64> {
    parsed
        .statistics
        .as_ref()
        .map(|stats| stats.message_count)
        .or(parsed.message_count)
}

fn message_time_range(parsed: &parse::ParsedMcap) -> Option<(u64, u64)> {
    if let Some(stats) = &parsed.statistics {
        return Some((stats.message_start_time, stats.message_end_time));
    }
    Some((parsed.message_start_time?, parsed.message_end_time?))
}

fn channel_count(parsed: &parse::ParsedMcap) -> u32 {
    parsed
        .statistics
        .as_ref()
        .map(|stats| stats.channel_count)
        .unwrap_or(parsed.channels.len() as u32)
}

fn attachment_count(parsed: &parse::ParsedMcap) -> u32 {
    parsed
        .statistics
        .as_ref()
        .map(|stats| stats.attachment_count)
        .or(parsed.attachment_count)
        .unwrap_or(parsed.attachment_indexes.len() as u32)
}

fn metadata_count(parsed: &parse::ParsedMcap) -> u32 {
    parsed
        .statistics
        .as_ref()
        .map(|stats| stats.metadata_count)
        .or(parsed.metadata_count)
        .unwrap_or(parsed.metadata_indexes.len() as u32)
}

fn render_channel_summary_rows(
    parsed: &parse::ParsedMcap,
    duration_seconds: f64,
) -> Vec<Vec<String>> {
    let mut rows = Vec::new();
    let max_channel_id_width = parsed
        .channels
        .keys()
        .max()
        .copied()
        .map(digits_u16)
        .unwrap_or(1);
    let channel_message_counts = channel_message_counts(parsed);
    let max_count_width = channel_message_counts
        .map(|counts| {
            parsed
                .channels
                .keys()
                .map(|channel_id| counts.get(channel_id).copied().unwrap_or_default())
                .max()
                .unwrap_or_default()
                .to_string()
                .len()
        })
        .unwrap_or(0);

    for channel in parsed.channels.values() {
        let msg_col = if let Some(counts) = channel_message_counts {
            let count = counts.get(&channel.id).copied().unwrap_or_default();
            if count > 1 && duration_seconds > 0.0 {
                let max_hz = count as f64 / duration_seconds;
                let min_hz = (count - 1) as f64 / duration_seconds;
                let delta = (max_hz - min_hz).max(f64::EPSILON);
                let precision = ((-delta.log10()).ceil() as i32).max(0) as usize;
                if precision > 2 {
                    format!("{count:>max_count_width$} msgs ({max_hz:.2}Hz)")
                } else {
                    format!(
                        "{count:>max_count_width$} msgs ({min_hz:.precision$}..{max_hz:.precision$}Hz)"
                    )
                }
            } else {
                format!("{count:>max_count_width$} msgs")
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

        let channel_id_width = digits_u16(channel.id);
        let channel_padding = " ".repeat(max_channel_id_width.saturating_sub(channel_id_width) + 1);
        rows.push(vec![
            format!("\t({}){}{}", channel.id, channel_padding, channel.topic),
            msg_col,
            schema_col,
        ]);
    }
    rows
}

fn channel_message_counts(parsed: &parse::ParsedMcap) -> Option<&BTreeMap<u16, u64>> {
    if let Some(stats) = &parsed.statistics {
        return Some(&stats.channel_message_counts);
    }
    parsed
        .message_count
        .is_some()
        .then_some(&parsed.channel_message_counts)
}

fn format_duration(start: u64, end: u64) -> (f64, String) {
    let (diff, sign) = if end >= start {
        (end - start, "")
    } else {
        (start - end, "-")
    };
    (
        diff as f64,
        format!("{sign}{}", format_duration_human(diff)),
    )
}

fn format_duration_human(nanos: u64) -> String {
    if nanos < 1_000 {
        return format!("{nanos}ns");
    }
    if nanos < 1_000_000 {
        return format_fractional_subsecond(nanos, 1_000, "µs");
    }
    if nanos < 1_000_000_000 {
        return format_fractional_subsecond(nanos, 1_000_000, "ms");
    }

    let total_seconds = nanos / 1_000_000_000;
    let fractional_nanos = nanos % 1_000_000_000;
    let hours = total_seconds / 3600;
    let minutes = (total_seconds % 3600) / 60;
    let seconds = total_seconds % 60;

    let seconds_with_fraction = if fractional_nanos == 0 {
        seconds.to_string()
    } else {
        let mut fractional = format!("{fractional_nanos:09}");
        while fractional.ends_with('0') {
            fractional.pop();
        }
        format!("{seconds}.{fractional}")
    };

    if hours > 0 {
        format!("{hours}h{minutes}m{seconds_with_fraction}s")
    } else if minutes > 0 {
        format!("{minutes}m{seconds_with_fraction}s")
    } else {
        format!("{seconds_with_fraction}s")
    }
}

fn format_fractional_subsecond(nanos: u64, unit_nanos: u64, unit: &str) -> String {
    let whole = nanos / unit_nanos;
    let remainder = nanos % unit_nanos;
    if remainder == 0 {
        return format!("{whole}{unit}");
    }

    let width = unit_nanos.ilog10() as usize;
    let mut fractional = format!("{remainder:0width$}");
    while fractional.ends_with('0') {
        fractional.pop();
    }
    format!("{whole}.{fractional}{unit}")
}

fn digits_u16(value: u16) -> usize {
    if value == 0 {
        return 1;
    }
    value.ilog10() as usize + 1
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

    use super::{count_chunk_overlaps, format_duration, render_channel_summary_rows, render_info};
    use crate::parse::{ParsedMcap, ParsedSchema};
    use mcap::records::{self, AttachmentIndex, ChunkIndex, Header, MetadataIndex, Statistics};

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
                library: mcap::LIBRARY_IDENTIFIER.to_string(),
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
        assert!(rendered.contains(&format!("library:     {}", mcap::LIBRARY_IDENTIFIER)));
        assert!(rendered.contains("profile:     demo"));
        assert!(rendered.contains("messages:    2"));
        assert!(rendered.contains("channels:"));
        assert!(rendered.contains("/demo"));
    }

    #[test]
    fn info_render_uses_scanned_counts_without_statistics() {
        let mut parsed = ParsedMcap {
            message_count: Some(1),
            message_start_time: Some(10),
            message_end_time: Some(10),
            attachment_count: Some(0),
            metadata_count: Some(0),
            ..ParsedMcap::default()
        };
        parsed.channels.insert(
            7,
            records::Channel {
                id: 7,
                schema_id: 0,
                topic: "/demo".to_string(),
                message_encoding: "json".to_string(),
                metadata: BTreeMap::new(),
            },
        );
        parsed.channel_message_counts.insert(7, 1);

        let rendered = render_info(&parsed);
        assert!(rendered.contains("messages:    1"));
        assert!(rendered.contains("duration:    0ns"));
        assert!(rendered.contains("1 msgs"));
        assert!(rendered.contains("channels:    1"));
        assert!(rendered.contains("attachments: 0"));
        assert!(rendered.contains("metadata:    0"));
        assert!(!rendered.contains("channels:    unknown"));
        assert!(!rendered.contains("attachments: unknown"));
        assert!(!rendered.contains("metadata:    unknown"));
    }

    #[test]
    fn info_render_includes_zero_scanned_message_count() {
        let parsed = ParsedMcap {
            message_count: Some(0),
            attachment_count: Some(0),
            metadata_count: Some(0),
            ..ParsedMcap::default()
        };

        let rendered = render_info(&parsed);
        assert!(rendered.contains("messages:    0"));
        assert!(rendered.contains("channels:    0"));
        assert!(rendered.contains("attachments: 0"));
        assert!(rendered.contains("metadata:    0"));
    }

    #[test]
    fn info_render_uses_summary_record_counts_without_statistics() {
        let mut parsed = ParsedMcap::default();
        parsed.channels.insert(
            7,
            records::Channel {
                id: 7,
                schema_id: 0,
                topic: "/demo".to_string(),
                message_encoding: "json".to_string(),
                metadata: BTreeMap::new(),
            },
        );
        parsed.attachment_indexes.push(AttachmentIndex {
            offset: 22,
            length: 10,
            log_time: 2,
            create_time: 3,
            data_size: 44,
            name: "demo.bin".to_string(),
            media_type: "application/octet-stream".to_string(),
        });
        parsed.metadata_indexes.push(MetadataIndex {
            offset: 7,
            length: 42,
            name: "demo".to_string(),
        });

        let rendered = render_info(&parsed);
        assert!(rendered.contains("channels:    1"));
        assert!(rendered.contains("attachments: 1"));
        assert!(rendered.contains("metadata:    1"));
        assert!(!rendered.contains("channels:    unknown"));
        assert!(!rendered.contains("attachments: unknown"));
        assert!(!rendered.contains("metadata:    unknown"));
    }

    #[test]
    fn duration_format_is_human_readable() {
        assert_eq!(format_duration(0, 7_200_000_000_000).1, "2h0m0s");
        assert_eq!(format_duration(0, 1_500_000_000).1, "1.5s");
        assert_eq!(format_duration(2_000_000_000, 1_000_000_000).1, "-1s");
        assert_eq!(format_duration(0, 500_000_000).1, "500ms");
        assert_eq!(format_duration(0, 100_000).1, "100µs");
        assert_eq!(format_duration(0, 1_500).1, "1.5µs");
        assert_eq!(format_duration(0, 10).1, "10ns");
    }

    #[test]
    fn channel_summary_rows_align_columns() {
        let mut parsed = ParsedMcap {
            statistics: Some(Statistics::default()),
            ..ParsedMcap::default()
        };
        parsed.channels.insert(
            1,
            records::Channel {
                id: 1,
                schema_id: 0,
                topic: "/alpha".to_string(),
                message_encoding: "json".to_string(),
                metadata: BTreeMap::new(),
            },
        );
        parsed.channels.insert(
            12,
            records::Channel {
                id: 12,
                schema_id: 0,
                topic: "/beta".to_string(),
                message_encoding: "json".to_string(),
                metadata: BTreeMap::new(),
            },
        );
        if let Some(stats) = &mut parsed.statistics {
            stats.channel_message_counts.insert(1, 7);
            stats.channel_message_counts.insert(12, 123);
        }

        let rows = render_channel_summary_rows(&parsed, 0.0);
        assert_eq!(rows[0][0], "\t(1)  /alpha");
        assert_eq!(rows[1][0], "\t(12) /beta");
        assert_eq!(rows[0][1], "  7 msgs");
        assert_eq!(rows[1][1], "123 msgs");
    }
}
