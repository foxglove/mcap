use anyhow::{Context, Result};

use crate::{
    cli::SortArgs,
    commands::transform_common::{compression_from_str, input_bytes_from_optional_file},
};

pub fn run(args: SortArgs) -> Result<()> {
    let bytes = input_bytes_from_optional_file(Some(&args.file))?;
    let mut stream = mcap::MessageStream::new(&bytes)
        .with_context(|| format!("failed to read messages from {}", args.file.display()))?;

    let mut messages = Vec::new();
    while let Some(msg) = stream.next() {
        messages.push(msg.context("failed to decode message while sorting")?);
    }
    if messages.is_empty() {
        anyhow::bail!("input file contains no messages");
    }
    messages.sort_by_key(|m| m.log_time);

    let compression = compression_from_str(&args.compression)?;
    let output = std::fs::File::create(&args.output_file)
        .with_context(|| format!("failed to create output {}", args.output_file.display()))?;
    let mut writer = mcap::WriteOptions::new()
        .compression(compression)
        .chunk_size(Some(args.chunk_size))
        .use_chunks(args.chunked)
        .calculate_chunk_crcs(args.include_crc)
        .create(std::io::BufWriter::new(output))
        .with_context(|| {
            format!(
                "failed to initialize writer for {}",
                args.output_file.display()
            )
        })?;

    for message in messages {
        writer.write(&message).with_context(|| {
            format!(
                "failed to write message for topic '{}'",
                message.channel.topic
            )
        })?;
    }

    writer
        .finish()
        .with_context(|| format!("failed to finalize {}", args.output_file.display()))?;
    Ok(())
}
