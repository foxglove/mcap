use crate::error::CliResult;
use crate::utils::validation::{validate_input_file, validate_output_file};
use clap::Args;
use mcap::{MessageStream, WriteOptions, Writer};
use memmap2::Mmap;
use std::fs::File;
use std::io::{BufWriter, Seek, Write};

#[derive(Args)]
pub struct DecompressArgs {
    /// Input MCAP file
    input: String,

    /// Output MCAP file
    #[arg(short, long)]
    output: String,

    /// Chunk size for output file
    #[arg(long, default_value = "4194304")]
    chunk_size: u64,
}

pub async fn execute(args: DecompressArgs) -> CliResult<()> {
    // Validate input and output files
    validate_input_file(&args.input)?;
    validate_output_file(&args.output)?;

    // Open input file
    let input_file = File::open(&args.input)?;
    let mapped = unsafe { Mmap::map(&input_file)? };

    // Create output file and writer (no compression)
    let output_file = File::create(&args.output)?;
    let mut writer = create_writer(output_file, &args)?;

    // Process all messages and write them without compression
    let message_stream = MessageStream::new(&mapped)?;

    // Track schemas and channels we've seen and their new IDs
    let mut schema_mapping = std::collections::HashMap::new();
    let mut channel_mapping = std::collections::HashMap::new();

    for message_result in message_stream {
        let message = message_result?;

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

    writer.finish()?;

    println!("Decompressed MCAP file written to: {}", args.output);

    Ok(())
}

fn create_writer<W: Write + Seek>(
    writer: W,
    args: &DecompressArgs,
) -> CliResult<Writer<BufWriter<W>>> {
    let mut write_opts = WriteOptions::new();

    // No compression for decompression
    write_opts = write_opts.compression(None);
    write_opts = write_opts.chunk_size(Some(args.chunk_size));

    Writer::with_options(BufWriter::new(writer), write_opts)
        .map_err(|e| crate::error::CliError::Mcap(e))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decompress_args() {
        let args = DecompressArgs {
            input: "input.mcap".to_string(),
            output: "output.mcap".to_string(),
            chunk_size: 4194304,
        };
        assert_eq!(args.input, "input.mcap");
        assert_eq!(args.output, "output.mcap");
    }
}
