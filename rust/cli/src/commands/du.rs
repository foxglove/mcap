use std::collections::BTreeMap;

use anyhow::{Context, Result};
use mcap::{
    read::Summary,
    records::{self, op},
    sans_io::{LinearReadEvent, LinearReader as SansIoReader, LinearReaderOptions},
};

use crate::{cli::DuArgs, cli_io, output};

const MESSAGE_RECORD_OVERHEAD: u64 = 1 + 8 + 2 + 4 + 8 + 8;
const FOOTER_RECORD_SIZE: u64 = 1 + 8 + 20;

#[derive(Default)]
struct UsageReport {
    record_kind_size: BTreeMap<String, u64>,
    topic_message_size: BTreeMap<String, u64>,
    total_message_size: u64,
    total_file_size: u64,
}

pub fn run(args: DuArgs) -> Result<()> {
    let bytes = cli_io::open_local_mcap(&args.file)?;

    let (report, approximate_used) = if args.approximate {
        match compute_approximate_usage(&bytes) {
            Ok(Some(report)) => (report, true),
            Ok(None) => {
                eprintln!(
                    "Warning: summary/chunk indexes unavailable for approximate mode; falling back to full scan"
                );
                (compute_exact_usage(&bytes)?, false)
            }
            Err(err) => {
                eprintln!(
                    "Warning: approximate usage scan failed ({err:#}); falling back to full scan"
                );
                (compute_exact_usage(&bytes)?, false)
            }
        }
    } else {
        (compute_exact_usage(&bytes)?, false)
    };

    print_usage_report(&report, approximate_used)
}

fn compute_exact_usage(bytes: &[u8]) -> Result<UsageReport> {
    let mut report = UsageReport {
        total_file_size: bytes.len() as u64,
        ..UsageReport::default()
    };
    let mut channels: BTreeMap<u16, String> = BTreeMap::new();

    scan_top_level_records(bytes, |opcode, data, record_size| {
        *report
            .record_kind_size
            .entry(record_name(opcode).to_string())
            .or_default() += record_size;

        let record = mcap::parse_record(opcode, data)
            .with_context(|| format!("failed parsing record opcode 0x{opcode:02x}"))?;

        match record {
            records::Record::Channel(channel) => {
                channels.insert(channel.id, channel.topic);
            }
            records::Record::Message { header, data } => {
                let topic = channels.get(&header.channel_id).ok_or_else(|| {
                    anyhow::anyhow!(
                        "message references unknown channel id {}",
                        header.channel_id
                    )
                })?;
                *report.topic_message_size.entry(topic.clone()).or_default() += data.len() as u64;
                report.total_message_size += data.len() as u64;
            }
            records::Record::Chunk { header, data } => {
                let mut chunk_reader = mcap::read::ChunkReader::new(header, data.as_ref())
                    .context("failed opening chunk while computing usage")?;
                while let Some(inner) = chunk_reader.next() {
                    match inner.context("failed reading record from chunk")? {
                        records::Record::Channel(channel) => {
                            channels.insert(channel.id, channel.topic);
                        }
                        records::Record::Message { header, data } => {
                            let topic = channels.get(&header.channel_id).ok_or_else(|| {
                                anyhow::anyhow!(
                                    "chunk message references unknown channel id {}",
                                    header.channel_id
                                )
                            })?;
                            *report.topic_message_size.entry(topic.clone()).or_default() +=
                                data.len() as u64;
                            report.total_message_size += data.len() as u64;
                        }
                        _ => {}
                    }
                }
            }
            _ => {}
        }
        Ok(())
    })?;

    *report
        .record_kind_size
        .entry("magic".to_string())
        .or_default() += (2 * mcap::MAGIC.len()) as u64;
    Ok(report)
}

fn compute_approximate_usage(bytes: &[u8]) -> Result<Option<UsageReport>> {
    let summary = match Summary::read(bytes)? {
        Some(summary) => summary,
        None => return Ok(None),
    };
    if summary.chunk_indexes.is_empty() {
        return Ok(None);
    }

    let mut report = UsageReport {
        total_file_size: bytes.len() as u64,
        ..UsageReport::default()
    };

    let chunk_bytes = summary
        .chunk_indexes
        .iter()
        .map(|chunk| chunk.chunk_length)
        .sum::<u64>();
    let message_index_bytes = summary
        .chunk_indexes
        .iter()
        .map(|chunk| chunk.message_index_length)
        .sum::<u64>();
    report
        .record_kind_size
        .insert("chunk".to_string(), chunk_bytes);
    report
        .record_kind_size
        .insert("message index".to_string(), message_index_bytes);

    let footer = mcap::read::footer(bytes).ok();
    if let Some(footer) = footer {
        let footer_start = report
            .total_file_size
            .saturating_sub(mcap::MAGIC.len() as u64 + FOOTER_RECORD_SIZE);
        if footer.summary_start > 0 && footer_start > footer.summary_start {
            report.record_kind_size.insert(
                "summary section".to_string(),
                footer_start - footer.summary_start,
            );
        }
    }

    let accounted = report.record_kind_size.values().copied().sum::<u64>();
    if report.total_file_size > accounted {
        report
            .record_kind_size
            .insert("other".to_string(), report.total_file_size - accounted);
    }

    for chunk in &summary.chunk_indexes {
        if chunk.message_index_offsets.is_empty() {
            continue;
        }
        let message_indexes = summary
            .read_message_indexes(bytes, chunk)
            .with_context(|| {
                format!(
                    "failed reading message indexes at chunk {}",
                    chunk.chunk_start_offset
                )
            })?;

        let mut offsets = Vec::new();
        for (channel, entries) in message_indexes {
            for entry in entries {
                offsets.push((entry.offset, channel.topic.clone()));
            }
        }
        offsets.sort_by_key(|(offset, _)| *offset);

        for (idx, (offset, topic)) in offsets.iter().enumerate() {
            if *offset > chunk.uncompressed_size {
                anyhow::bail!(
                    "message offset {} exceeds chunk uncompressed size {}",
                    offset,
                    chunk.uncompressed_size
                );
            }
            let next_offset = offsets
                .get(idx + 1)
                .map(|(next_offset, _)| *next_offset)
                .unwrap_or(chunk.uncompressed_size);
            if next_offset <= *offset {
                continue;
            }
            let span = next_offset - *offset;
            if span <= MESSAGE_RECORD_OVERHEAD {
                continue;
            }
            let payload = span - MESSAGE_RECORD_OVERHEAD;
            *report.topic_message_size.entry(topic.clone()).or_default() += payload;
            report.total_message_size += payload;
        }
    }

    Ok(Some(report))
}

fn scan_top_level_records<F>(bytes: &[u8], mut on_record: F) -> Result<()>
where
    F: FnMut(u8, &[u8], u64) -> Result<()>,
{
    let mut reader = SansIoReader::new_with_options(
        LinearReaderOptions::default()
            .with_emit_chunks(true)
            .with_validate_chunk_crcs(true),
    );
    let mut consumed = 0usize;

    while let Some(event) = reader.next_event() {
        match event.context("failed reading MCAP records")? {
            LinearReadEvent::ReadRequest(need) => {
                let remaining = bytes.len().saturating_sub(consumed);
                let len = remaining.min(need);
                reader
                    .insert(len)
                    .copy_from_slice(&bytes[consumed..consumed + len]);
                reader.notify_read(len);
                consumed += len;
            }
            LinearReadEvent::Record { data, opcode } => {
                on_record(opcode, data, (1 + 8 + data.len()) as u64)?;
            }
        }
    }

    Ok(())
}

fn print_usage_report(report: &UsageReport, approximate: bool) -> Result<()> {
    if approximate {
        println!("Top level record stats (approximate):");
    } else {
        println!("Top level record stats:");
    }
    println!();

    let mut record_rows = vec![vec![
        "record".to_string(),
        "sum bytes".to_string(),
        "% of total file bytes".to_string(),
    ]];
    let mut record_entries = report
        .record_kind_size
        .iter()
        .map(|(name, size)| (name.clone(), *size))
        .collect::<Vec<_>>();
    record_entries.sort_by(|(name_a, size_a), (name_b, size_b)| {
        size_b.cmp(size_a).then_with(|| name_a.cmp(name_b))
    });
    for (name, size) in record_entries {
        let pct = if report.total_file_size == 0 {
            0.0
        } else {
            size as f64 * 100.0 / report.total_file_size as f64
        };
        record_rows.push(vec![name, human_bytes(size), format!("{pct:.6}")]);
    }
    output::print_rows(&record_rows)?;

    println!();
    println!("Topic message stats:");
    println!();

    let mut topic_rows = vec![vec![
        "topic".to_string(),
        "sum message bytes".to_string(),
        "% of total message bytes".to_string(),
    ]];
    let mut topic_entries = report
        .topic_message_size
        .iter()
        .map(|(name, size)| (name.clone(), *size))
        .collect::<Vec<_>>();
    topic_entries.sort_by(|(name_a, size_a), (name_b, size_b)| {
        size_b.cmp(size_a).then_with(|| name_a.cmp(name_b))
    });
    for (topic, size) in topic_entries {
        let pct = if report.total_message_size == 0 {
            0.0
        } else {
            size as f64 * 100.0 / report.total_message_size as f64
        };
        topic_rows.push(vec![topic, human_bytes(size), format!("{pct:.6}")]);
    }
    output::print_rows(&topic_rows)?;
    Ok(())
}

fn human_bytes(size: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KB", "MB", "GB", "TB"];
    let mut value = size as f64;
    let mut unit = 0usize;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{size} {}", UNITS[unit])
    } else {
        format!("{value:.2} {}", UNITS[unit])
    }
}

fn record_name(opcode: u8) -> &'static str {
    match opcode {
        op::HEADER => "header",
        op::FOOTER => "footer",
        op::SCHEMA => "schema",
        op::CHANNEL => "channel",
        op::MESSAGE => "message",
        op::CHUNK => "chunk",
        op::MESSAGE_INDEX => "message index",
        op::CHUNK_INDEX => "chunk index",
        op::ATTACHMENT => "attachment",
        op::ATTACHMENT_INDEX => "attachment index",
        op::STATISTICS => "statistics",
        op::METADATA => "metadata",
        op::METADATA_INDEX => "metadata index",
        op::SUMMARY_OFFSET => "summary offset",
        op::DATA_END => "data end",
        _ => "unknown",
    }
}
