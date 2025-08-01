use crate::error::CliResult;
use crate::utils::validation::{validate_input_file, validate_output_file};
use chrono::DateTime;
use clap::Args;
use mcap::{MessageStream, WriteOptions, Writer};
use memmap2::Mmap;
use regex::Regex;
use std::fs::File;
use std::io::{BufWriter, Seek, Write};

#[derive(Args)]
pub struct FilterArgs {
    /// Input MCAP file
    input: String,

    /// Output MCAP file
    #[arg(short, long)]
    output: String,

    /// Include messages with topic names matching this regex (can be used multiple times)
    #[arg(short = 'y', long = "include-topic-regex")]
    include_topics: Vec<String>,

    /// Exclude messages with topic names matching this regex (can be used multiple times)
    #[arg(short = 'n', long = "exclude-topic-regex")]
    exclude_topics: Vec<String>,

    /// Start time (RFC3339 format or nanoseconds)
    #[arg(short = 'S', long)]
    start: Option<String>,

    /// End time (RFC3339 format or nanoseconds)
    #[arg(short = 'E', long)]
    end: Option<String>,

    /// Start time in seconds
    #[arg(short = 's', long, conflicts_with = "start")]
    start_secs: Option<u64>,

    /// End time in seconds
    #[arg(short = 'e', long, conflicts_with = "end")]
    end_secs: Option<u64>,

    /// Start time in nanoseconds
    #[arg(long, conflicts_with = "start")]
    start_nsecs: Option<u64>,

    /// End time in nanoseconds
    #[arg(long, conflicts_with = "end")]
    end_nsecs: Option<u64>,

    /// Include metadata records
    #[arg(long, default_value = "true")]
    include_metadata: bool,

    /// Include attachment records
    #[arg(long, default_value = "true")]
    include_attachments: bool,

    /// Compression format for output (none, lz4, zstd)
    #[arg(long, default_value = "zstd")]
    compression: String,

    /// Chunk size for output file
    #[arg(long, default_value = "4194304")]
    chunk_size: u64,

    /// Don't chunk the output file
    #[arg(long)]
    unchunked: bool,
}

struct FilterOptions {
    include_topics: Vec<Regex>,
    exclude_topics: Vec<Regex>,
    start_time: Option<u64>,
    end_time: Option<u64>,
    include_metadata: bool,
    include_attachments: bool,
    compression: Option<mcap::Compression>,
    chunk_size: Option<u64>,
}

pub async fn execute(args: FilterArgs) -> CliResult<()> {
    // Validate input and output files
    validate_input_file(&args.input)?;
    validate_output_file(&args.output)?;

    // Parse filter options
    let filter_opts = parse_filter_options(&args)?;

    // Open input file
    let input_file = File::open(&args.input)?;
    let mapped = unsafe { Mmap::map(&input_file)? };

    // Create output file and writer
    let output_file = File::create(&args.output)?;
    let mut writer = create_writer(output_file, &filter_opts)?;

    // Process the file
    filter_mcap(&mapped, &mut writer, &filter_opts).await?;

    writer.finish()?;

    println!("Filtered MCAP file written to: {}", args.output);

    Ok(())
}

fn parse_filter_options(args: &FilterArgs) -> CliResult<FilterOptions> {
    // Parse include topic regexes
    let mut include_topics = Vec::new();
    for pattern in &args.include_topics {
        let regex = Regex::new(pattern).map_err(|e| {
            crate::error::CliError::invalid_argument(format!(
                "Invalid include topic regex '{}': {}",
                pattern, e
            ))
        })?;
        include_topics.push(regex);
    }

    // Parse exclude topic regexes
    let mut exclude_topics = Vec::new();
    for pattern in &args.exclude_topics {
        let regex = Regex::new(pattern).map_err(|e| {
            crate::error::CliError::invalid_argument(format!(
                "Invalid exclude topic regex '{}': {}",
                pattern, e
            ))
        })?;
        exclude_topics.push(regex);
    }

    // Parse start time
    let start_time =
        parse_timestamp_args(args.start.as_deref(), args.start_nsecs, args.start_secs)?;

    // Parse end time
    let end_time = parse_timestamp_args(args.end.as_deref(), args.end_nsecs, args.end_secs)?;

    // Parse compression
    let compression = match args.compression.as_str() {
        "none" => None,
        "lz4" => Some(mcap::Compression::Lz4),
        "zstd" => Some(mcap::Compression::Zstd),
        other => {
            return Err(crate::error::CliError::invalid_argument(format!(
                "Unknown compression format: {}",
                other
            )))
        }
    };

    // Set chunk size
    let chunk_size = if args.unchunked {
        None
    } else {
        Some(args.chunk_size)
    };

    Ok(FilterOptions {
        include_topics,
        exclude_topics,
        start_time,
        end_time,
        include_metadata: args.include_metadata,
        include_attachments: args.include_attachments,
        compression,
        chunk_size,
    })
}

fn parse_timestamp_args(
    date_or_nanos: Option<&str>,
    nanoseconds: Option<u64>,
    seconds: Option<u64>,
) -> CliResult<Option<u64>> {
    if let Some(date_str) = date_or_nanos {
        return Ok(Some(parse_date_or_nanos(date_str)?));
    }
    if let Some(ns) = nanoseconds {
        return Ok(Some(ns));
    }
    if let Some(s) = seconds {
        return Ok(Some(s * 1_000_000_000));
    }
    Ok(None)
}

fn parse_date_or_nanos(date_or_nanos: &str) -> CliResult<u64> {
    // Try parsing as nanoseconds first
    if let Ok(nanos) = date_or_nanos.parse::<u64>() {
        return Ok(nanos);
    }

    // Try parsing as RFC3339 date
    let dt = DateTime::parse_from_rfc3339(date_or_nanos).map_err(|e| {
        crate::error::CliError::invalid_argument(format!(
            "Invalid timestamp '{}': {}",
            date_or_nanos, e
        ))
    })?;

    Ok(dt.timestamp_nanos_opt().unwrap_or(0) as u64)
}

fn create_writer<W: Write + Seek>(
    writer: W,
    opts: &FilterOptions,
) -> CliResult<Writer<BufWriter<W>>> {
    let mut write_opts = WriteOptions::new();
    write_opts = write_opts.compression(opts.compression);

    if let Some(chunk_size) = opts.chunk_size {
        write_opts = write_opts.chunk_size(Some(chunk_size));
    } else {
        write_opts = write_opts.use_chunks(false);
    }

    Writer::with_options(BufWriter::new(writer), write_opts)
        .map_err(|e| crate::error::CliError::Mcap(e))
}

async fn filter_mcap<W: Write + Seek>(
    mapped: &[u8],
    writer: &mut Writer<W>,
    opts: &FilterOptions,
) -> CliResult<()> {
    // Create message stream
    let message_stream = MessageStream::new(mapped)?;

    // Track channels and schemas we've seen and their new IDs
    let mut schema_mapping = std::collections::HashMap::new();
    let mut channel_mapping = std::collections::HashMap::new();

    for message_result in message_stream {
        let message = message_result?;

        // Apply time filtering
        if let Some(start) = opts.start_time {
            if message.log_time < start {
                continue;
            }
        }
        if let Some(end) = opts.end_time {
            if message.log_time > end {
                continue;
            }
        }

        // Apply topic filtering
        if !should_include_topic(
            &message.channel.topic,
            &opts.include_topics,
            &opts.exclude_topics,
        ) {
            continue;
        }

        // Get or create schema ID
        let schema_id = if let Some(schema) = &message.channel.schema {
            if let Some(&existing_id) = schema_mapping.get(&schema.id) {
                existing_id
            } else {
                let new_id = writer.add_schema(&schema.name, &schema.encoding, &schema.data)?;
                schema_mapping.insert(schema.id, new_id);
                new_id
            }
        } else {
            0 // No schema
        };

        // Get or create channel ID
        let channel_id = if let Some(&existing_id) = channel_mapping.get(&message.channel.id) {
            existing_id
        } else {
            let new_id = writer.add_channel(
                schema_id,
                &message.channel.topic,
                &message.channel.message_encoding,
                &message.channel.metadata,
            )?;
            channel_mapping.insert(message.channel.id, new_id);
            new_id
        };

        // Write the message
        writer.write_to_known_channel(
            &mcap::records::MessageHeader {
                channel_id,
                sequence: message.sequence,
                log_time: message.log_time,
                publish_time: message.publish_time,
            },
            &message.data,
        )?;
    }

    Ok(())
}

fn should_include_topic(topic: &str, include_regexes: &[Regex], exclude_regexes: &[Regex]) -> bool {
    // If there are exclude patterns, check them first
    for regex in exclude_regexes {
        if regex.is_match(topic) {
            return false;
        }
    }

    // If there are include patterns, topic must match at least one
    if !include_regexes.is_empty() {
        for regex in include_regexes {
            if regex.is_match(topic) {
                return true;
            }
        }
        return false;
    }

    // No include patterns and not excluded - include it
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_date_or_nanos() {
        // Test nanoseconds
        assert_eq!(parse_date_or_nanos("1234567890").unwrap(), 1234567890);

        // Test RFC3339 date
        let result = parse_date_or_nanos("2023-01-01T00:00:00Z");
        assert!(result.is_ok());
    }

    #[test]
    fn test_should_include_topic() {
        let include_patterns = vec![Regex::new(r"/test.*").unwrap()];
        let exclude_patterns = vec![Regex::new(r".*_private").unwrap()];

        // Should include - matches include pattern
        assert!(should_include_topic(
            "/test/topic",
            &include_patterns,
            &exclude_patterns
        ));

        // Should exclude - matches exclude pattern
        assert!(!should_include_topic(
            "/test/topic_private",
            &include_patterns,
            &exclude_patterns
        ));

        // Should not include - doesn't match include pattern
        assert!(!should_include_topic(
            "/other/topic",
            &include_patterns,
            &exclude_patterns
        ));

        // No patterns - should include everything
        assert!(should_include_topic("/any/topic", &[], &[]));
    }

    #[test]
    fn test_parse_timestamp_args() {
        // Test RFC3339 date
        let result = parse_timestamp_args(Some("2023-01-01T00:00:00Z"), None, None);
        assert!(result.is_ok());

        // Test nanoseconds
        let result = parse_timestamp_args(None, Some(1234567890), None);
        assert_eq!(result.unwrap(), Some(1234567890));

        // Test seconds
        let result = parse_timestamp_args(None, None, Some(5));
        assert_eq!(result.unwrap(), Some(5_000_000_000));

        // Test none
        let result = parse_timestamp_args(None, None, None);
        assert_eq!(result.unwrap(), None);
    }
}
