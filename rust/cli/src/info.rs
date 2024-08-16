use std::{collections::HashMap, pin::Pin};

use chrono::{Duration, Local, TimeZone};
use mcap::records::ChunkIndex;
use tokio::fs::File;

use tabled::settings::{
    object::{Cell, Columns},
    Alignment, Margin, Padding, Style, Theme,
};

use crate::{
    error::{CliError, CliResult},
    gcs_reader::create_gcs_reader,
    mcap::read_info,
    traits::McapReader,
    utils::{format_decimal_nanos, format_human_nanos, format_human_bytes},
};

#[derive(Debug)]
enum McapFd {
    Gcs {
        bucket_name: String,
        object_name: String,
    },
    File(String),
}

impl McapFd {
    fn parse(path: String) -> CliResult<McapFd> {
        if path.starts_with("gs://") {
            let Some((bucket_name, object_name)) = path.trim_start_matches("gs://").split_once('/')
            else {
                return Err(CliError::UnexpectedInput(format!(
                    "The provided path '{path}' was not a valid GCS url."
                )));
            };

            Ok(McapFd::Gcs {
                bucket_name: bucket_name.into(),
                object_name: object_name.into(),
            })
        } else {
            Ok(McapFd::File(path))
        }
    }

    async fn create_reader(&self) -> CliResult<Pin<Box<dyn McapReader>>> {
        match self {
            Self::File(path) => Ok(Box::pin(File::open(path).await?)),
            Self::Gcs {
                bucket_name,
                object_name,
            } => Ok(Box::pin(create_gcs_reader(bucket_name, object_name).await?)),
        }
    }
}

const NANOSECONDS_IN_SECOND: f64 = 1e9;

#[derive(Default)]
struct CompressionInfo {
    compressed_size: u64,
    uncompressed_size: u64,
    chunk_count: u64,
}

fn get_compression_stats(info: Vec<ChunkIndex>) -> HashMap<String, CompressionInfo> {
    let mut compression_stats = HashMap::<String, CompressionInfo>::new();

    for chunk in info.into_iter() {
        let entry = compression_stats.entry(chunk.compression).or_default();

        entry.chunk_count += 1;
        entry.compressed_size += chunk.compressed_size;
        entry.uncompressed_size += chunk.uncompressed_size;
    }

    compression_stats
}

pub async fn print_info(path: String) -> CliResult<()> {
    let fd = McapFd::parse(path)?;
    let reader = fd.create_reader().await?;

    let info = read_info(reader).await?;

    let mut builder = tabled::builder::Builder::default();

    builder.push_record(["library:", &info.header.library]);
    builder.push_record(["profile:", &info.header.profile]);

    if let Some(stats) = &info.statistics {
        let message_count = stats.message_count.to_string();

        let long_ago = Local::now() - Duration::days(20 * 365);

        let start = Local.timestamp_nanos(stats.message_start_time as _);
        let end = Local.timestamp_nanos(stats.message_end_time as _);

        let start_time;
        let end_time;

        if start > long_ago {
            start_time = format!(
                "{start} ({})",
                format_decimal_nanos(stats.message_start_time)
            );
            end_time = format!("{end} ({})", format_decimal_nanos(stats.message_end_time));
        } else {
            start_time = format_decimal_nanos(stats.message_start_time);
            end_time = format_decimal_nanos(stats.message_end_time);
        }

        let duration = format_human_nanos(stats.message_end_time - stats.message_start_time);

        builder.push_record(["messages:", &message_count]);
        builder.push_record(["duration:", &duration]);
        builder.push_record(["start:", &start_time]);
        builder.push_record(["end:", &end_time]);
    }

    let mut indented_rows = vec![];

    let total_chunks = info.chunk_indexes.len();

    if total_chunks > 0 {
        builder.push_record(["compression:"]);

        for (kind, compression_info) in get_compression_stats(info.chunk_indexes).into_iter() {
            let CompressionInfo {
                compressed_size: total_compressed,
                uncompressed_size: total_uncompressed,
                chunk_count: total_count,
            } = compression_info;

            let mut throughput = String::new();

            if let Some(stats) = &info.statistics {
                let duration_seconds = (stats.message_end_time - stats.message_start_time) as f64
                    / NANOSECONDS_IN_SECOND;

                if duration_seconds > 0. {
                    throughput = format!(
                        "[{}/sec]",
                        format_human_bytes((total_compressed as f64 / duration_seconds) as _)
                    );
                }
            }

            let compression_ratio = format!(
                "{:.2}",
                (1. - total_compressed as f64 / total_uncompressed as f64) * 100.
            );
            let total_compressed = format_human_bytes(total_compressed);
            let total_uncompressed = format_human_bytes(total_uncompressed);

            indented_rows.push(builder.count_records());

            builder.push_record([
            format!("{kind}:"),
            format!("[{total_count}/{total_chunks} chunks] [{total_uncompressed}/{total_compressed} ({compression_ratio}%)] {throughput}"),
        ])
        }
    }

    builder.push_record(["channels:"]);

    let mut channels = info.channels;
    channels.sort_by_key(|x| x.id);

    let max_count_width = info
        .statistics
        .as_ref()
        .and_then(|stats| {
            stats
                .channel_message_counts
                .values()
                .map(|count| format!("{count}").len())
                .max()
        })
        .unwrap_or(0);

    for channel in channels {
        let id = format!("({})", channel.id);
        let mut row = vec![id, channel.topic];

        let schema = info
            .schemas
            .iter()
            .find(|schema| schema.id == channel.schema_id);

        if let Some(stats) = &info.statistics {
            let message_counts = *stats.channel_message_counts.get(&channel.id).unwrap_or(&0);

            if message_counts > 1 {
                let frequency = (NANOSECONDS_IN_SECOND * (message_counts as f64))
                    / (stats.message_end_time - stats.message_start_time) as f64;

                row.push(format!(
                    "{message_counts: >0$} msgs ({frequency:.2} Hz)",
                    max_count_width
                ));
            } else {
                row.push(format!("{message_counts: >0$} msgs", max_count_width));
            }
        } else {
            row.push("<no statistics>".to_string());
        }

        if let Some(schema) = schema {
            row.push(format!(" : {} [{}]", schema.name, schema.encoding));
        } else if channel.schema_id != 0 {
            row.push(format!(" : <missing schema {}>", channel.schema_id));
        } else {
            row.push(" : <no schema>".to_string());
        }

        indented_rows.push(builder.count_records());
        builder.push_record(row);
    }

    if let Some(stats) = &info.statistics {
        builder.push_record(["attachments:", &stats.attachment_count.to_string()]);

        println!("{}", &stats.metadata_count);

        builder.push_record(["metadata:", &stats.metadata_count.to_string()]);
    } else {
        builder.push_record(["attachments:", "<unknown>"]);
        builder.push_record(["metadata:", "<unknown>"]);
    }

    let mut style = Theme::from_style(Style::ascii());
    style.remove_borders();

    let mut table = builder.build();

    table
        .with(style)
        .with(Margin::new(0, 0, 0, 0))
        .with(Alignment::left())
        .modify(Columns::first(), Padding::new(0, 0, 0, 0));

    for row in indented_rows.into_iter() {
        table.modify(Cell::new(row, 0), Alignment::right());
    }

    println!("{table}");

    Ok(())
}
