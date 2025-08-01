use crate::error::CliResult;
use crate::utils::validation::{validate_input_file, validate_output_file};
use clap::Args;
use mcap::{MessageStream, WriteOptions, Writer};
use memmap2::Mmap;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufWriter, Seek, Write};

#[derive(Args)]
pub struct MergeArgs {
    /// Input MCAP files to merge
    inputs: Vec<String>,

    /// Output MCAP file
    #[arg(short, long)]
    output: String,

    /// Compression format for output (none, lz4, zstd)
    #[arg(long, default_value = "zstd")]
    compression: String,

    /// Chunk size for output file
    #[arg(long, default_value = "4194304")]
    chunk_size: u64,

    /// Don't chunk the output file
    #[arg(long)]
    unchunked: bool,

    /// Remove duplicate messages (same channel, timestamp, and sequence)
    #[arg(long)]
    deduplicate: bool,

    /// Only merge messages within time range (start time in nanoseconds)
    #[arg(long)]
    start_time: Option<u64>,

    /// Only merge messages within time range (end time in nanoseconds)
    #[arg(long)]
    end_time: Option<u64>,
}

// Message with source file information for merging
#[derive(Debug)]
struct SourcedMessage<'a> {
    message: mcap::Message<'a>,
    source_file: usize,
}

impl<'a> PartialEq for SourcedMessage<'a> {
    fn eq(&self, other: &Self) -> bool {
        self.message.log_time == other.message.log_time
    }
}

impl<'a> Eq for SourcedMessage<'a> {}

impl<'a> PartialOrd for SourcedMessage<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<'a> Ord for SourcedMessage<'a> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Normal ordering for timestamp comparison (earlier messages come first)
        self.message.log_time.cmp(&other.message.log_time)
    }
}

pub async fn execute(args: MergeArgs) -> CliResult<()> {
    // Validate arguments
    if args.inputs.is_empty() {
        return Err(crate::error::CliError::invalid_argument(
            "At least one input file must be provided".to_string(),
        ));
    }

    if args.inputs.len() < 2 {
        return Err(crate::error::CliError::invalid_argument(
            "At least two input files are required for merging".to_string(),
        ));
    }

    // Validate input files
    for input in &args.inputs {
        validate_input_file(input)?;
    }
    validate_output_file(&args.output)?;

    println!("Merging {} MCAP files...", args.inputs.len());

    // Create output file and writer
    let output_file = File::create(&args.output)?;
    let mut writer = create_writer(output_file, &args)?;

    // Perform merge
    merge_files(&args.inputs, &mut writer, &args).await?;

    writer.finish()?;

    println!("Merged MCAP file written to: {}", args.output);

    Ok(())
}

fn create_writer<W: Write + Seek>(writer: W, args: &MergeArgs) -> CliResult<Writer<BufWriter<W>>> {
    let mut write_opts = WriteOptions::new();

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

    write_opts = write_opts.compression(compression);

    if args.unchunked {
        write_opts = write_opts.use_chunks(false);
    } else {
        write_opts = write_opts.chunk_size(Some(args.chunk_size));
    }

    Writer::with_options(BufWriter::new(writer), write_opts)
        .map_err(|e| crate::error::CliError::Mcap(e))
}

async fn merge_files<W: Write + Seek>(
    input_files: &[String],
    writer: &mut Writer<W>,
    args: &MergeArgs,
) -> CliResult<()> {
    // Open and map all input files
    let mut files = Vec::new();
    let mut mapped_files = Vec::new();

    for input_file in input_files {
        let file = File::open(input_file)?;
        let mapped = unsafe { Mmap::map(&file)? };
        files.push(file);
        mapped_files.push(mapped);
    }

    // Track schemas and channels across all files for ID mapping
    let mut schema_mapping = HashMap::new(); // (source_file, original_id) -> new_id
    let mut channel_mapping = HashMap::new(); // (source_file, original_id) -> new_id

    // Collect all messages from all files into a sorted structure
    let mut all_messages = Vec::new();
    let mut total_messages = 0u64;

    for (file_index, mapped) in mapped_files.iter().enumerate() {
        let message_stream = MessageStream::new(mapped)?;

        for message_result in message_stream {
            let message = message_result?;

            // Apply time filtering if specified
            if let Some(start) = args.start_time {
                if message.log_time < start {
                    continue;
                }
            }
            if let Some(end) = args.end_time {
                if message.log_time > end {
                    continue;
                }
            }

            all_messages.push(SourcedMessage {
                message,
                source_file: file_index,
            });
            total_messages += 1;
        }
    }

    // Sort all messages by timestamp for proper merging
    all_messages.sort_by_key(|sm| sm.message.log_time);

    println!(
        "Processing {} messages from {} files...",
        total_messages,
        input_files.len()
    );

    // Track duplicates if deduplication is enabled
    let mut seen_messages = std::collections::HashSet::new();
    let mut deduplicated_count = 0;

    // Process sorted messages
    for sourced_message in all_messages {
        let message = &sourced_message.message;
        let source_file = sourced_message.source_file;

        // Check for duplicates if deduplication is enabled
        if args.deduplicate {
            let message_key = (message.channel.id, message.log_time, message.sequence);
            if !seen_messages.insert(message_key) {
                deduplicated_count += 1;
                continue;
            }
        }

        // Get or create schema ID for this file's schema
        let schema_id = if let Some(schema) = &message.channel.schema {
            let schema_key = (source_file, schema.id);
            if let Some(&existing_id) = schema_mapping.get(&schema_key) {
                existing_id
            } else {
                let new_id = writer.add_schema(&schema.name, &schema.encoding, &schema.data)?;
                schema_mapping.insert(schema_key, new_id);
                new_id
            }
        } else {
            0 // No schema
        };

        // Get or create channel ID for this file's channel
        let channel_key = (source_file, message.channel.id);
        let channel_id = if let Some(&existing_id) = channel_mapping.get(&channel_key) {
            existing_id
        } else {
            let new_id = writer.add_channel(
                schema_id,
                &message.channel.topic,
                &message.channel.message_encoding,
                &message.channel.metadata,
            )?;
            channel_mapping.insert(channel_key, new_id);
            new_id
        };

        // Write the message with the new channel ID
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

    if args.deduplicate && deduplicated_count > 0 {
        println!("Removed {} duplicate messages", deduplicated_count);
    }

    println!("Merge completed successfully");

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_merge_args() {
        let args = MergeArgs {
            inputs: vec!["file1.mcap".to_string(), "file2.mcap".to_string()],
            output: "merged.mcap".to_string(),
            compression: "zstd".to_string(),
            chunk_size: 4194304,
            unchunked: false,
            deduplicate: true,
            start_time: Some(1000),
            end_time: Some(2000),
        };
        assert_eq!(args.inputs.len(), 2);
        assert_eq!(args.output, "merged.mcap");
        assert!(args.deduplicate);
    }

    #[test]
    fn test_sourced_message_ordering() {
        // Test that SourcedMessage implements proper ordering for timestamp sorting
        use std::sync::Arc;

        let channel = Arc::new(mcap::Channel {
            id: 1,
            topic: "test".to_string(),
            message_encoding: "json".to_string(),
            metadata: Default::default(),
            schema: None,
        });

        let msg1 = SourcedMessage {
            message: mcap::Message {
                channel: channel.clone(),
                sequence: 1,
                log_time: 1000,
                publish_time: 1000,
                data: std::borrow::Cow::Borrowed(&[]),
            },
            source_file: 0,
        };

        let msg2 = SourcedMessage {
            message: mcap::Message {
                channel,
                sequence: 2,
                log_time: 2000,
                publish_time: 2000,
                data: std::borrow::Cow::Borrowed(&[]),
            },
            source_file: 0,
        };

        // Earlier timestamp should come first in normal ordering
        assert!(msg1 < msg2);
        assert_eq!(msg1.cmp(&msg2), std::cmp::Ordering::Less);
    }
}
