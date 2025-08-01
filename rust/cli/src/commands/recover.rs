use crate::error::CliResult;
use crate::utils::validation::{validate_input_file, validate_output_file};
use clap::Args;
use mcap::{WriteOptions, Writer};
use std::fs::File;
use std::io::{BufWriter, Read, Seek, Write};

#[derive(Args)]
pub struct RecoverArgs {
    /// Input MCAP file (potentially corrupted)
    input: String,

    /// Output MCAP file (recovered)
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

    /// Skip attempting to recover corrupted chunks
    #[arg(long)]
    skip_corrupted_chunks: bool,

    /// Verbose output
    #[arg(short, long)]
    verbose: bool,
}

#[derive(Debug, Default)]
struct RecoveryStats {
    total_messages: u64,
    recovered_messages: u64,
    corrupted_messages: u64,
    recovered_attachments: u64,
    recovered_metadata: u64,
    recovered_schemas: u64,
    recovered_channels: u64,
    skipped_chunks: u64,
}

pub async fn execute(args: RecoverArgs) -> CliResult<()> {
    // Validate input and output files
    validate_input_file(&args.input)?;
    validate_output_file(&args.output)?;

    println!("🩹 MCAP File Recovery");
    println!("====================");
    println!("Input:  {}", args.input);
    println!("Output: {}", args.output);

    if args.verbose {
        println!("Options:");
        println!("  Skip corrupted chunks: {}", args.skip_corrupted_chunks);
        println!("  Compression: {}", args.compression);
        if !args.unchunked {
            println!("  Chunk size: {} bytes", args.chunk_size);
        }
    }
    println!();

    // Open input file
    let mut input_file = File::open(&args.input)?;

    // Create output file and writer
    let output_file = File::create(&args.output)?;
    let mut writer = create_writer(output_file, &args)?;

    // Perform recovery
    let stats = recover_mcap(&mut input_file, &mut writer, &args).await?;

    writer.finish()?;

    // Print recovery results
    print_recovery_results(&stats, &args)?;

    Ok(())
}

fn create_writer<W: Write + Seek>(
    writer: W,
    args: &RecoverArgs,
) -> CliResult<Writer<BufWriter<W>>> {
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

async fn recover_mcap<R: Read + Seek, W: Write + Seek>(
    input: &mut R,
    writer: &mut Writer<W>,
    args: &RecoverArgs,
) -> CliResult<RecoveryStats> {
    let mut stats = RecoveryStats::default();
    let mut buffer = Vec::new();

    println!("🔍 Scanning file for recoverable data...");

    // Read the entire file into memory for analysis
    input.read_to_end(&mut buffer)?;

    if args.verbose {
        println!("File size: {} bytes", buffer.len());
    }

    // Try to recover using MCAP library's error-tolerant parsing
    match mcap::MessageStream::new(&buffer) {
        Ok(message_stream) => {
            println!("✅ File structure appears valid, recovering messages...");

            // Track schemas and channels for ID mapping
            let mut schema_mapping = std::collections::HashMap::new();
            let mut channel_mapping = std::collections::HashMap::new();

            for message_result in message_stream {
                match message_result {
                    Ok(message) => {
                        stats.total_messages += 1;

                        // Recover schema if we haven't seen it
                        let schema_id = if let Some(schema) = &message.channel.schema {
                            if let Some(&existing_id) = schema_mapping.get(&schema.id) {
                                existing_id
                            } else {
                                match writer.add_schema(
                                    &schema.name,
                                    &schema.encoding,
                                    &schema.data,
                                ) {
                                    Ok(new_id) => {
                                        schema_mapping.insert(schema.id, new_id);
                                        stats.recovered_schemas += 1;
                                        new_id
                                    }
                                    Err(e) => {
                                        if args.verbose {
                                            println!(
                                                "⚠️  Failed to add schema {}: {}",
                                                schema.id, e
                                            );
                                        }
                                        stats.corrupted_messages += 1;
                                        continue;
                                    }
                                }
                            }
                        } else {
                            0 // No schema
                        };

                        // Recover channel if we haven't seen it
                        let channel_id =
                            if let Some(&existing_id) = channel_mapping.get(&message.channel.id) {
                                existing_id
                            } else {
                                match writer.add_channel(
                                    schema_id,
                                    &message.channel.topic,
                                    &message.channel.message_encoding,
                                    &message.channel.metadata,
                                ) {
                                    Ok(new_id) => {
                                        channel_mapping.insert(message.channel.id, new_id);
                                        stats.recovered_channels += 1;
                                        new_id
                                    }
                                    Err(e) => {
                                        if args.verbose {
                                            println!(
                                                "⚠️  Failed to add channel {}: {}",
                                                message.channel.id, e
                                            );
                                        }
                                        stats.corrupted_messages += 1;
                                        continue;
                                    }
                                }
                            };

                        // Try to write the message
                        match writer.write_to_known_channel(
                            &mcap::records::MessageHeader {
                                channel_id,
                                sequence: message.sequence,
                                log_time: message.log_time,
                                publish_time: message.publish_time,
                            },
                            &message.data,
                        ) {
                            Ok(_) => {
                                stats.recovered_messages += 1;
                                if args.verbose && stats.recovered_messages % 1000 == 0 {
                                    println!(
                                        "📦 Recovered {} messages so far...",
                                        stats.recovered_messages
                                    );
                                }
                            }
                            Err(e) => {
                                if args.verbose {
                                    println!("⚠️  Failed to write message: {}", e);
                                }
                                stats.corrupted_messages += 1;
                            }
                        }
                    }
                    Err(e) => {
                        if args.verbose {
                            println!("⚠️  Corrupted message encountered: {}", e);
                        }
                        stats.corrupted_messages += 1;

                        if args.skip_corrupted_chunks {
                            stats.skipped_chunks += 1;
                            continue;
                        }

                        // Try to continue reading despite the error
                        continue;
                    }
                }
            }
        }
        Err(e) => {
            println!("❌ File appears to be severely corrupted: {}", e);
            println!("🔧 Attempting byte-level recovery...");

            // TODO: Implement more sophisticated byte-level recovery
            // This would scan for MCAP magic bytes and try to reconstruct records
            return Err(crate::error::CliError::invalid_argument(
                "Byte-level recovery not yet implemented. File too corrupted to recover."
                    .to_string(),
            ));
        }
    }

    Ok(stats)
}

fn print_recovery_results(stats: &RecoveryStats, args: &RecoverArgs) -> CliResult<()> {
    println!("📊 Recovery Results:");
    println!("===================");

    if stats.total_messages > 0 {
        let recovery_rate = (stats.recovered_messages as f64 / stats.total_messages as f64) * 100.0;
        println!("Messages:");
        println!("  Total found: {}", stats.total_messages);
        println!("  Successfully recovered: {}", stats.recovered_messages);
        println!("  Corrupted/skipped: {}", stats.corrupted_messages);
        println!("  Recovery rate: {:.1}%", recovery_rate);
        println!();
    }

    if stats.recovered_schemas > 0 || stats.recovered_channels > 0 {
        println!("Metadata:");
        println!("  Schemas recovered: {}", stats.recovered_schemas);
        println!("  Channels recovered: {}", stats.recovered_channels);
        if stats.recovered_attachments > 0 {
            println!("  Attachments recovered: {}", stats.recovered_attachments);
        }
        if stats.recovered_metadata > 0 {
            println!("  Metadata records recovered: {}", stats.recovered_metadata);
        }
        println!();
    }

    if stats.skipped_chunks > 0 {
        println!("Chunks skipped due to corruption: {}", stats.skipped_chunks);
        println!();
    }

    if stats.recovered_messages > 0 {
        println!("✅ Recovery completed successfully!");
        println!("📁 Recovered file written to: {}", args.output);

        if stats.corrupted_messages > 0 {
            println!("⚠️  Some data could not be recovered due to corruption.");
        }
    } else {
        println!("❌ No data could be recovered from the input file.");
        return Err(crate::error::CliError::invalid_argument(
            "Recovery failed - no recoverable data found".to_string(),
        ));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_recover_args() {
        let args = RecoverArgs {
            input: "corrupted.mcap".to_string(),
            output: "recovered.mcap".to_string(),
            compression: "zstd".to_string(),
            chunk_size: 4194304,
            unchunked: false,
            skip_corrupted_chunks: true,
            verbose: false,
        };
        assert_eq!(args.input, "corrupted.mcap");
        assert_eq!(args.output, "recovered.mcap");
        assert!(args.skip_corrupted_chunks);
    }

    #[test]
    fn test_recovery_stats_default() {
        let stats = RecoveryStats::default();
        assert_eq!(stats.total_messages, 0);
        assert_eq!(stats.recovered_messages, 0);
        assert_eq!(stats.corrupted_messages, 0);
    }
}
