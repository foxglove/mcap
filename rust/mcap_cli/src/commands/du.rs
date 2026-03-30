use std::collections::BTreeMap;

use anyhow::{anyhow, bail, Result};
use mcap::records::{self, op, Record};
use mcap::sans_io::{LinearReadEvent, LinearReader, LinearReaderOptions};

use crate::cli::DuCommand;
use crate::commands::common;
use crate::context::CommandContext;

const MCAP_MAGIC_SIZE: u64 = mcap::MAGIC.len() as u64;
const FOOTER_RECORD_SIZE: u64 = 29;
const MESSAGE_OVERHEAD: u64 = 31;

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
            None => (collect_usage_exact(&mcap)?, false),
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

    for (opcode, data) in iter_top_level_records(mcap)? {
        let Some(kind) = record_kind_name(opcode) else {
            continue;
        };

        let record_size = data.len() as u64;
        usage.total_size += record_size;
        *usage.record_kind_size.entry(kind.to_string()).or_default() += record_size;

        match mcap::parse_record(opcode, &data)? {
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
    }

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
    usage.record_kind_size.insert(
        "message index".to_string(),
        total_message_indexes_on_disk,
    );

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

fn iter_top_level_records(mcap: &[u8]) -> Result<Vec<(u8, Vec<u8>)>> {
    let mut out = Vec::new();
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
                out.push((opcode, data.to_vec()));
            }
        }
    }

    Ok(out)
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
    let mut topic_sizes = BTreeMap::<String, u64>::new();
    let mut total_size = 0u64;

    for chunk in chunk_indexes {
        if chunk.message_index_length == 0 {
            continue;
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

        let (chunk_topic_sizes, chunk_total_size) =
            parse_chunk_message_indexes(&mcap[start..end], chunk.uncompressed_size, channel_topics)?;

        for (topic, size) in chunk_topic_sizes {
            *topic_sizes.entry(topic).or_default() += size;
        }
        total_size += chunk_total_size;
    }

    Ok((topic_sizes, total_size))
}

fn parse_chunk_message_indexes(
    buf: &[u8],
    uncompressed_size: u64,
    channel_topics: &BTreeMap<u16, String>,
) -> Result<(BTreeMap<String, u64>, u64)> {
    let mut entries = Vec::<OffsetEntry>::new();
    let mut pos = 0usize;

    while pos + 9 <= buf.len() {
        let opcode = buf[pos];
        if opcode != op::MESSAGE_INDEX {
            bail!(
                "unexpected opcode 0x{opcode:02x} at offset {pos}, expected MessageIndex (0x{:02x})",
                op::MESSAGE_INDEX
            );
        }

        let record_len =
            u64::from_le_bytes(buf[pos + 1..pos + 9].try_into().expect("slice length"));
        if record_len > buf.len() as u64 {
            bail!(
                "message index record length {} exceeds buffer size at offset {}",
                record_len,
                pos
            );
        }
        let record_end = pos + 9 + usize::try_from(record_len).map_err(|_| {
            anyhow!("message index record length out of range at offset {pos}: {record_len}")
        })?;
        if record_end > buf.len() {
            bail!("message index record extends beyond buffer at offset {pos}");
        }

        let body = &buf[pos + 9..record_end];
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
        bail!("message index buffer has {} trailing bytes", buf.len() - pos);
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

fn print_record_table(record_kind_size: &BTreeMap<String, u64>, total_size: u64, approximate: bool) {
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
            human_bytes(size),
            format!("{pct:.6}"),
        ]);
    }

    common::print_table(&rows);
}

fn record_kind_name(opcode: u8) -> Option<&'static str> {
    match opcode {
        op::HEADER => Some("header"),
        op::FOOTER => Some("footer"),
        op::SCHEMA => Some("schema"),
        op::CHANNEL => Some("channel"),
        op::MESSAGE => Some("message"),
        op::CHUNK => Some("chunk"),
        op::MESSAGE_INDEX => Some("message index"),
        op::CHUNK_INDEX => Some("chunk index"),
        op::ATTACHMENT_INDEX => Some("attachment index"),
        op::STATISTICS => Some("statistics"),
        op::METADATA => Some("metadata"),
        op::METADATA_INDEX => Some("metadata index"),
        op::SUMMARY_OFFSET => Some("summary offset"),
        op::DATA_END => Some("data end"),
        _ => None,
    }
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
        messages: &[(u16, u64, usize)],
        channels: &[(u16, &str)],
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

            for (_id, topic) in channels {
                writer
                    .add_channel(schema_id, *topic, "raw", &BTreeMap::new())
                    .expect("add channel");
            }

            for (channel_id, log_time, size) in messages {
                writer
                    .write_to_known_channel(
                        &MessageHeader {
                            channel_id: *channel_id,
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

    #[test]
    fn exact_usage_counts_unchunked_message_payloads() {
        let mcap = write_test_file(
            false,
            None,
            &[(1, 0, 100), (1, 1, 50), (2, 2, 25)],
            &[(1, "/camera"), (2, "/imu")],
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
            &[(1, 0, 100), (2, 1, 60), (1, 2, 40)],
            &[(1, "/alpha"), (2, "/beta")],
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
            &[(1, 0, 90), (2, 1, 30), (1, 2, 10), (2, 3, 70)],
            &[(1, "/left"), (2, "/right")],
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
    fn parse_chunk_message_indexes_rejects_trailing_bytes() {
        let mut file = write_test_file(
            true,
            Some(1024 * 1024),
            &[(1, 0, 10), (1, 1, 10)],
            &[(1, "/data")],
        );
        let summary = mcap::Summary::read(&file)
            .expect("summary read should succeed")
            .expect("summary should exist");
        let chunk = summary
            .chunk_indexes
            .first()
            .expect("chunk index should be present");

        let start = usize::try_from(chunk.chunk_start_offset + chunk.chunk_length)
            .expect("message index start in range");
        let end = usize::try_from(chunk.chunk_start_offset + chunk.chunk_length + chunk.message_index_length)
            .expect("message index end in range");
        let mut buf = file[start..end].to_vec();
        buf.push(0);
        file.clear();

        let err =
            parse_chunk_message_indexes(&buf, 100, &BTreeMap::from([(1u16, "/demo".to_string())]))
                .expect_err("should fail on trailing bytes");
        assert!(err.to_string().contains("trailing bytes"));
    }

    #[test]
    fn record_kind_name_matches_go_token_strings() {
        assert_eq!(record_kind_name(op::HEADER), Some("header"));
        assert_eq!(record_kind_name(op::MESSAGE_INDEX), Some("message index"));
        assert_eq!(record_kind_name(op::ATTACHMENT), None);
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
}
