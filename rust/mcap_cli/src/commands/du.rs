use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Mutex;

use anyhow::{anyhow, bail, Result};
use mcap::records::{self, op, Record};
use mcap::sans_io::{LinearReadEvent, LinearReader, LinearReaderOptions};

use crate::cli::DuCommand;
use crate::commands::common;
use crate::context::CommandContext;

const MCAP_MAGIC_SIZE: u64 = mcap::MAGIC.len() as u64;
const FOOTER_RECORD_SIZE: u64 = 29;
const RECORD_ENVELOPE_SIZE: usize = 9;
const MESSAGE_HEADER_SIZE: u64 = 22;
const MESSAGE_OVERHEAD: u64 = RECORD_ENVELOPE_SIZE as u64 + MESSAGE_HEADER_SIZE;
const MAX_APPROX_WORKERS: usize = 16;

#[derive(Debug, Default, Clone, PartialEq, Eq)]
struct Usage {
    total_size: u64,
    total_message_size: u64,
    record_kind_size: BTreeMap<String, u64>,
    topic_message_size: BTreeMap<String, u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct OffsetEntry {
    offset: u64,
    channel_id: u16,
}

pub fn run(_ctx: &CommandContext, args: DuCommand) -> Result<()> {
    let mcap = common::map_file(&args.file)?;

    let (usage, used_approximate) = if args.approximate {
        match collect_usage_approximate(&mcap)? {
            Some(usage) => (usage, true),
            None => {
                eprintln!(
                    "Warning: summary/chunk indexes unavailable; falling back to exact du scan."
                );
                (collect_usage_exact(&mcap)?, false)
            }
        }
    } else {
        (collect_usage_exact(&mcap)?, false)
    };

    print_record_table(&usage.record_kind_size, usage.total_size, used_approximate);
    print_topic_table(&usage.topic_message_size, usage.total_message_size);

    Ok(())
}

fn collect_usage_exact(mcap: &[u8]) -> Result<Usage> {
    let mut usage = Usage {
        total_size: 2 * MCAP_MAGIC_SIZE,
        ..Usage::default()
    };
    let mut channels = BTreeMap::<u16, String>::new();
    scan_top_level_records(mcap, |opcode, data| {
        let kind = record_kind_name(opcode);
        let record_size = (RECORD_ENVELOPE_SIZE + data.len()) as u64;
        usage.total_size += record_size;
        *usage.record_kind_size.entry(kind.to_string()).or_default() += record_size;

        match mcap::parse_record(opcode, data)? {
            Record::Channel(channel) => {
                channels.insert(channel.id, channel.topic);
            }
            Record::Message { header, data } => {
                process_message(&mut usage, &channels, header.channel_id, data.len() as u64)?;
            }
            Record::Chunk { header, data } => {
                process_chunk(&mut usage, &mut channels, header, data.as_ref())?;
            }
            _ => {}
        }
        Ok(())
    })?;

    Ok(usage)
}

fn collect_usage_approximate(mcap: &[u8]) -> Result<Option<Usage>> {
    let summary = match mcap::Summary::read(mcap) {
        Ok(Some(summary)) => summary,
        Ok(None) | Err(_) => return Ok(None),
    };

    if summary.chunk_indexes.is_empty() {
        return Ok(None);
    }

    let footer = match mcap::read::footer(mcap) {
        Ok(footer) => footer,
        Err(_) => return Ok(None),
    };
    if footer.summary_start == 0 {
        return Ok(None);
    }

    let total_file_size = mcap.len() as u64;
    let mut usage = Usage {
        total_size: total_file_size,
        ..Usage::default()
    };

    let mut total_chunk_on_disk = 0u64;
    let mut total_message_indexes_on_disk = 0u64;

    for chunk in &summary.chunk_indexes {
        total_chunk_on_disk += chunk.chunk_length;
        total_message_indexes_on_disk += chunk.message_index_length;
    }

    usage
        .record_kind_size
        .insert("chunk".to_string(), total_chunk_on_disk);
    usage
        .record_kind_size
        .insert("message index".to_string(), total_message_indexes_on_disk);

    let minimum_file_size = MCAP_MAGIC_SIZE + FOOTER_RECORD_SIZE + MCAP_MAGIC_SIZE;
    if total_file_size >= minimum_file_size {
        let footer_start = total_file_size - MCAP_MAGIC_SIZE - FOOTER_RECORD_SIZE;
        if footer_start > footer.summary_start {
            usage.record_kind_size.insert(
                "summary section".to_string(),
                footer_start - footer.summary_start,
            );
        }
    }

    let mut accounted = total_chunk_on_disk + total_message_indexes_on_disk;
    if let Some(summary_section_size) = usage.record_kind_size.get("summary section") {
        accounted += *summary_section_size;
    }
    if accounted > total_file_size {
        bail!(
            "chunk index metadata exceeds file size ({} > {})",
            accounted,
            total_file_size
        );
    }
    let other = total_file_size - accounted;
    if other > 0 {
        usage.record_kind_size.insert("other".to_string(), other);
    }

    let channel_topics: BTreeMap<u16, String> = summary
        .channels
        .iter()
        .map(|(id, channel)| (*id, channel.topic.clone()))
        .collect();

    let (topic_message_size, total_message_size) =
        compute_topic_sizes_from_index(mcap, &summary.chunk_indexes, &channel_topics)?;

    usage.topic_message_size = topic_message_size;
    usage.total_message_size = total_message_size;

    Ok(Some(usage))
}

fn scan_top_level_records<F>(mcap: &[u8], mut process: F) -> Result<()>
where
    F: FnMut(u8, &[u8]) -> Result<()>,
{
    let mut reader = LinearReader::new_with_options(
        LinearReaderOptions::default()
            .with_emit_chunks(true)
            .with_validate_chunk_crcs(true)
            .with_record_length_limit(mcap.len()),
    );
    let mut remaining = mcap;

    while let Some(event) = reader.next_event() {
        match event? {
            LinearReadEvent::ReadRequest(need) => {
                let read = need.min(remaining.len());
                let dst = reader.insert(read);
                dst.copy_from_slice(&remaining[..read]);
                reader.notify_read(read);
                remaining = &remaining[read..];
            }
            LinearReadEvent::Record { opcode, data } => {
                process(opcode, data)?;
            }
        }
    }

    Ok(())
}

fn process_chunk(
    usage: &mut Usage,
    channels: &mut BTreeMap<u16, String>,
    header: records::ChunkHeader,
    data: &[u8],
) -> Result<()> {
    let chunk_reader = mcap::read::ChunkReader::new(header, data)?;
    for nested_record in chunk_reader {
        match nested_record? {
            Record::Channel(channel) => {
                channels.insert(channel.id, channel.topic);
            }
            Record::Message { header, data } => {
                process_message(usage, channels, header.channel_id, data.len() as u64)?;
            }
            _ => {}
        }
    }
    Ok(())
}

fn process_message(
    usage: &mut Usage,
    channels: &BTreeMap<u16, String>,
    channel_id: u16,
    data_size: u64,
) -> Result<()> {
    let Some(topic) = channels.get(&channel_id) else {
        bail!("got a Message record for unknown channel: {channel_id}");
    };
    usage.total_message_size += data_size;
    *usage.topic_message_size.entry(topic.clone()).or_default() += data_size;
    Ok(())
}

fn compute_topic_sizes_from_index(
    mcap: &[u8],
    chunk_indexes: &[records::ChunkIndex],
    channel_topics: &BTreeMap<u16, String>,
) -> Result<(BTreeMap<String, u64>, u64)> {
    let worker_count = std::thread::available_parallelism()
        .map(|parallelism| parallelism.get())
        .unwrap_or(1)
        .min(MAX_APPROX_WORKERS)
        .min(chunk_indexes.len());

    if worker_count <= 1 || chunk_indexes.len() <= 1 {
        return compute_topic_sizes_from_index_sequential(mcap, chunk_indexes, channel_topics);
    }

    compute_topic_sizes_from_index_parallel(mcap, chunk_indexes, channel_topics, worker_count)
}

fn compute_topic_sizes_from_index_sequential(
    mcap: &[u8],
    chunk_indexes: &[records::ChunkIndex],
    channel_topics: &BTreeMap<u16, String>,
) -> Result<(BTreeMap<String, u64>, u64)> {
    let mut topic_sizes = BTreeMap::<String, u64>::new();
    let mut total_size = 0u64;

    for chunk in chunk_indexes {
        let (chunk_topic_sizes, chunk_total_size) =
            compute_topic_sizes_for_chunk(mcap, chunk, channel_topics)?;

        for (topic, size) in chunk_topic_sizes {
            *topic_sizes.entry(topic).or_default() += size;
        }
        total_size += chunk_total_size;
    }

    Ok((topic_sizes, total_size))
}

fn compute_topic_sizes_from_index_parallel(
    mcap: &[u8],
    chunk_indexes: &[records::ChunkIndex],
    channel_topics: &BTreeMap<u16, String>,
    worker_count: usize,
) -> Result<(BTreeMap<String, u64>, u64)> {
    let next_index = AtomicUsize::new(0);
    let stop = AtomicBool::new(false);
    let first_error = Mutex::new(None::<anyhow::Error>);
    let partials = Mutex::new(Vec::<(BTreeMap<String, u64>, u64)>::new());

    std::thread::scope(|scope| {
        for _ in 0..worker_count {
            scope.spawn(|| {
                let mut local_sizes = BTreeMap::<String, u64>::new();
                let mut local_total = 0u64;

                loop {
                    if stop.load(Ordering::Relaxed) {
                        break;
                    }

                    let index = next_index.fetch_add(1, Ordering::Relaxed);
                    if index >= chunk_indexes.len() {
                        break;
                    }

                    match compute_topic_sizes_for_chunk(mcap, &chunk_indexes[index], channel_topics)
                    {
                        Ok((chunk_sizes, chunk_total)) => {
                            for (topic, size) in chunk_sizes {
                                *local_sizes.entry(topic).or_default() += size;
                            }
                            local_total += chunk_total;
                        }
                        Err(err) => {
                            stop.store(true, Ordering::Relaxed);
                            let mut guard = first_error
                                .lock()
                                .expect("failed to lock first_error mutex");
                            if guard.is_none() {
                                *guard = Some(err);
                            }
                            break;
                        }
                    }
                }

                if !local_sizes.is_empty() || local_total > 0 {
                    partials
                        .lock()
                        .expect("failed to lock partials mutex")
                        .push((local_sizes, local_total));
                }
            });
        }
    });

    if let Some(err) = first_error
        .lock()
        .expect("failed to lock first_error mutex")
        .take()
    {
        return Err(err);
    }

    let mut merged_sizes = BTreeMap::<String, u64>::new();
    let mut merged_total = 0u64;
    for (sizes, total) in partials
        .lock()
        .expect("failed to lock partials mutex")
        .drain(..)
    {
        for (topic, size) in sizes {
            *merged_sizes.entry(topic).or_default() += size;
        }
        merged_total += total;
    }

    Ok((merged_sizes, merged_total))
}

fn compute_topic_sizes_for_chunk(
    mcap: &[u8],
    chunk: &records::ChunkIndex,
    channel_topics: &BTreeMap<u16, String>,
) -> Result<(BTreeMap<String, u64>, u64)> {
    if chunk.message_index_length == 0 {
        return Ok((BTreeMap::new(), 0));
    }

    let message_index_offset = chunk.chunk_start_offset + chunk.chunk_length;
    let message_index_end = message_index_offset + chunk.message_index_length;
    let start = usize::try_from(message_index_offset)
        .map_err(|_| anyhow!("message index offset out of range: {message_index_offset}"))?;
    let end = usize::try_from(message_index_end)
        .map_err(|_| anyhow!("message index end out of range: {message_index_end}"))?;
    if end > mcap.len() {
        bail!(
            "message index section extends beyond file ({} > {})",
            end,
            mcap.len()
        );
    }

    parse_chunk_message_indexes(&mcap[start..end], chunk.uncompressed_size, channel_topics)
}

fn parse_chunk_message_indexes(
    buf: &[u8],
    uncompressed_size: u64,
    channel_topics: &BTreeMap<u16, String>,
) -> Result<(BTreeMap<String, u64>, u64)> {
    let mut entries = Vec::<OffsetEntry>::new();
    let mut pos = 0usize;

    while pos + RECORD_ENVELOPE_SIZE <= buf.len() {
        let opcode = buf[pos];
        if opcode != op::MESSAGE_INDEX {
            bail!(
                "unexpected opcode 0x{opcode:02x} at offset {pos}, expected MessageIndex (0x{:02x})",
                op::MESSAGE_INDEX
            );
        }

        let record_len = u64::from_le_bytes(
            buf[pos + 1..pos + RECORD_ENVELOPE_SIZE]
                .try_into()
                .expect("slice length"),
        );
        if record_len > buf.len() as u64 {
            bail!(
                "message index record length {} exceeds buffer size at offset {}",
                record_len,
                pos
            );
        }
        let record_end = pos
            + RECORD_ENVELOPE_SIZE
            + usize::try_from(record_len).map_err(|_| {
                anyhow!("message index record length out of range at offset {pos}: {record_len}")
            })?;
        if record_end > buf.len() {
            bail!("message index record extends beyond buffer at offset {pos}");
        }

        let body = &buf[pos + RECORD_ENVELOPE_SIZE..record_end];
        let record = mcap::parse_record(op::MESSAGE_INDEX, body)?;
        let Record::MessageIndex(message_index) = record else {
            bail!("failed to parse message index at offset {pos}");
        };
        for entry in message_index.records {
            entries.push(OffsetEntry {
                offset: entry.offset,
                channel_id: message_index.channel_id,
            });
        }

        pos = record_end;
    }

    if pos != buf.len() {
        bail!(
            "message index buffer has {} trailing bytes",
            buf.len() - pos
        );
    }

    if entries.is_empty() {
        return Ok((BTreeMap::new(), 0));
    }

    entries.sort_by_key(|entry| entry.offset);

    let mut topic_sizes = BTreeMap::<String, u64>::new();
    let mut total_size = 0u64;

    for (index, entry) in entries.iter().enumerate() {
        if entry.offset > uncompressed_size {
            bail!(
                "message offset {} exceeds chunk uncompressed size {}",
                entry.offset,
                uncompressed_size
            );
        }

        let record_size = if let Some(next) = entries.get(index + 1) {
            next.offset.saturating_sub(entry.offset)
        } else {
            uncompressed_size.saturating_sub(entry.offset)
        };

        if record_size <= MESSAGE_OVERHEAD {
            continue;
        }

        let data_size = record_size - MESSAGE_OVERHEAD;
        let Some(topic) = channel_topics.get(&entry.channel_id) else {
            bail!("message references unknown channel: {}", entry.channel_id);
        };
        *topic_sizes.entry(topic.clone()).or_default() += data_size;
        total_size += data_size;
    }

    Ok((topic_sizes, total_size))
}

fn print_record_table(
    record_kind_size: &BTreeMap<String, u64>,
    total_size: u64,
    approximate: bool,
) {
    if approximate {
        println!("Top level record stats (approximate):");
    } else {
        println!("Top level record stats:");
    }
    println!();

    let mut rows = vec![
        vec![
            "record".to_string(),
            "sum bytes".to_string(),
            "% of total file bytes".to_string(),
        ],
        vec![
            "------".to_string(),
            "---------".to_string(),
            "---------------------".to_string(),
        ],
    ];

    let mut records: Vec<(&str, u64)> = record_kind_size
        .iter()
        .map(|(kind, size)| (kind.as_str(), *size))
        .collect();
    records.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(b.0)));

    for (name, size) in records {
        let pct = if total_size > 0 {
            size as f64 / total_size as f64 * 100.0
        } else {
            0.0
        };
        rows.push(vec![
            name.to_string(),
            size.to_string(),
            format!("{pct:.6}"),
        ]);
    }

    common::print_table(&rows);
}

fn print_topic_table(topic_message_size: &BTreeMap<String, u64>, total_message_size: u64) {
    println!();
    println!("Message size stats:");
    println!();

    let mut rows = vec![
        vec![
            "topic".to_string(),
            "sum bytes (uncompressed)".to_string(),
            "% of total message bytes (uncompressed)".to_string(),
        ],
        vec![
            "-----".to_string(),
            "------------------------".to_string(),
            "---------------------------------------".to_string(),
        ],
    ];

    let mut topics: Vec<(&str, u64)> = topic_message_size
        .iter()
        .map(|(topic, size)| (topic.as_str(), *size))
        .collect();
    topics.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(b.0)));

    for (topic, size) in topics {
        let pct = if total_message_size > 0 {
            size as f64 / total_message_size as f64 * 100.0
        } else {
            0.0
        };
        rows.push(vec![
            topic.to_string(),
            common::human_bytes(size),
            format!("{pct:.6}"),
        ]);
    }

    common::print_table(&rows);
}

fn record_kind_name(opcode: u8) -> &'static str {
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
        // Keep unknown opcodes in size accounting so totals stay accurate if
        // newer MCAP versions introduce record types this CLI doesn't yet name.
        _ => "unknown",
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::{
        collect_usage_approximate, collect_usage_exact, parse_chunk_message_indexes,
        print_record_table, print_topic_table, record_kind_name, Usage,
    };
    use mcap::records::{op, MessageHeader};

    fn write_test_file(
        chunked: bool,
        chunk_size: Option<u64>,
        messages: &[(usize, u64, usize)],
        channels: &[&str],
    ) -> Vec<u8> {
        let mut buffer = Vec::new();
        {
            let mut writer = mcap::WriteOptions::new()
                .use_chunks(chunked)
                .chunk_size(chunk_size)
                .create(std::io::Cursor::new(&mut buffer))
                .expect("create writer");
            let schema_id = writer
                .add_schema("test_schema", "raw", b"{}")
                .expect("add schema");

            let mut channel_ids = Vec::with_capacity(channels.len());
            for topic in channels {
                let channel_id = writer
                    .add_channel(schema_id, topic, "raw", &BTreeMap::new())
                    .expect("add channel");
                channel_ids.push(channel_id);
            }

            for (channel_index, log_time, size) in messages {
                let channel_id = channel_ids
                    .get(*channel_index)
                    .copied()
                    .expect("message channel index should be in range");
                writer
                    .write_to_known_channel(
                        &MessageHeader {
                            channel_id,
                            sequence: 0,
                            log_time: *log_time,
                            publish_time: *log_time,
                        },
                        &vec![0u8; *size],
                    )
                    .expect("write message");
            }

            writer.finish().expect("finish writer");
        }
        buffer
    }

    fn first_chunk_message_index_bytes(mcap: &[u8]) -> (Vec<u8>, u64) {
        let summary = mcap::Summary::read(mcap)
            .expect("summary read should succeed")
            .expect("summary should exist");
        let chunk = summary
            .chunk_indexes
            .first()
            .expect("chunk index should be present");

        let start = usize::try_from(chunk.chunk_start_offset + chunk.chunk_length)
            .expect("message index start in range");
        let end = usize::try_from(
            chunk.chunk_start_offset + chunk.chunk_length + chunk.message_index_length,
        )
        .expect("message index end in range");
        (mcap[start..end].to_vec(), chunk.uncompressed_size)
    }

    #[test]
    fn exact_usage_counts_unchunked_message_payloads() {
        let mcap = write_test_file(
            false,
            None,
            &[(0, 0, 100), (0, 1, 50), (1, 2, 25)],
            &["/camera", "/imu"],
        );

        let usage = collect_usage_exact(&mcap).expect("collect exact usage");
        assert_eq!(usage.total_message_size, 175);
        assert_eq!(usage.topic_message_size["/camera"], 150);
        assert_eq!(usage.topic_message_size["/imu"], 25);
    }

    #[test]
    fn exact_usage_counts_chunked_message_payloads() {
        let mcap = write_test_file(
            true,
            Some(128),
            &[(0, 0, 100), (1, 1, 60), (0, 2, 40)],
            &["/alpha", "/beta"],
        );

        let usage = collect_usage_exact(&mcap).expect("collect exact usage");
        assert_eq!(usage.total_message_size, 200);
        assert_eq!(usage.topic_message_size["/alpha"], 140);
        assert_eq!(usage.topic_message_size["/beta"], 60);
    }

    #[test]
    fn approximate_usage_matches_exact_for_standard_chunking() {
        let mcap = write_test_file(
            true,
            Some(256),
            &[(0, 0, 90), (1, 1, 30), (0, 2, 10), (1, 3, 70)],
            &["/left", "/right"],
        );

        let exact = collect_usage_exact(&mcap).expect("exact");
        let approximate = collect_usage_approximate(&mcap)
            .expect("approximate")
            .expect("summary-backed approximate usage");
        assert_eq!(approximate.total_message_size, exact.total_message_size);
        assert_eq!(approximate.topic_message_size, exact.topic_message_size);
    }

    #[test]
    fn approximate_usage_falls_back_when_summary_is_missing() {
        let mut buffer = Vec::new();
        {
            let mut writer = mcap::WriteOptions::new()
                .emit_summary_records(false)
                .emit_summary_offsets(false)
                .create(std::io::Cursor::new(&mut buffer))
                .expect("create writer");
            let schema_id = writer
                .add_schema("test_schema", "raw", b"{}")
                .expect("add schema");
            let channel_id = writer
                .add_channel(schema_id, "/demo", "raw", &BTreeMap::new())
                .expect("add channel");
            writer
                .write_to_known_channel(
                    &MessageHeader {
                        channel_id,
                        sequence: 1,
                        log_time: 10,
                        publish_time: 10,
                    },
                    &[1, 2, 3],
                )
                .expect("write message");
            writer.finish().expect("finish writer");
        }

        let approximate = collect_usage_approximate(&buffer).expect("approximate");
        assert!(approximate.is_none());
    }

    #[test]
    fn approximate_usage_falls_back_when_no_chunk_indexes() {
        let mcap = write_test_file(false, None, &[(0, 0, 10), (0, 1, 10)], &["/data"]);
        let approximate = collect_usage_approximate(&mcap).expect("approximate");
        assert!(approximate.is_none());
    }

    #[test]
    fn parse_chunk_message_indexes_rejects_trailing_bytes() {
        let file = write_test_file(
            true,
            Some(1024 * 1024),
            &[(0, 0, 10), (0, 1, 10)],
            &["/data"],
        );
        let (mut buf, _) = first_chunk_message_index_bytes(&file);
        buf.push(0);

        let err =
            parse_chunk_message_indexes(&buf, 100, &BTreeMap::from([(1u16, "/demo".to_string())]))
                .expect_err("should fail on trailing bytes");
        assert!(err.to_string().contains("trailing bytes"));
    }

    #[test]
    fn parse_chunk_message_indexes_rejects_unknown_channel() {
        let file = write_test_file(
            true,
            Some(1024 * 1024),
            &[(0, 0, 10), (0, 1, 10)],
            &["/data"],
        );
        let (buf, uncompressed_size) = first_chunk_message_index_bytes(&file);

        let err = parse_chunk_message_indexes(&buf, uncompressed_size, &BTreeMap::new())
            .expect_err("should fail on unknown channel");
        assert!(err.to_string().contains("unknown channel"));
    }

    #[test]
    fn parse_chunk_message_indexes_rejects_offset_beyond_chunk_size() {
        let file = write_test_file(
            true,
            Some(1024 * 1024),
            &[(0, 0, 10), (0, 1, 10)],
            &["/data"],
        );
        let (buf, _) = first_chunk_message_index_bytes(&file);

        let err =
            parse_chunk_message_indexes(&buf, 0, &BTreeMap::from([(1u16, "/data".to_string())]))
                .expect_err("should fail on invalid offset");
        assert!(err.to_string().contains("exceeds chunk uncompressed size"));
    }

    #[test]
    fn record_kind_name_matches_go_token_strings() {
        assert_eq!(record_kind_name(op::HEADER), "header");
        assert_eq!(record_kind_name(op::MESSAGE_INDEX), "message index");
        assert_eq!(record_kind_name(op::ATTACHMENT), "attachment");
    }

    #[test]
    fn table_printers_accept_empty_maps() {
        print_record_table(&BTreeMap::new(), 0, false);
        print_topic_table(&BTreeMap::new(), 0);
    }

    #[test]
    fn usage_default_is_empty() {
        assert_eq!(Usage::default(), Usage::default());
    }

    #[test]
    fn human_bytes_matches_expected_units() {
        assert_eq!(crate::commands::common::human_bytes(2), "2.00 B");
        assert_eq!(crate::commands::common::human_bytes(2 * 1024), "2.00 KiB");
    }

    #[test]
    fn exact_usage_includes_magic_bytes_baseline() {
        let mcap = write_test_file(false, None, &[(0, 0, 10)], &["/data"]);
        let usage = collect_usage_exact(&mcap).expect("collect exact usage");
        assert_eq!(usage.total_size, mcap.len() as u64);
    }
}
