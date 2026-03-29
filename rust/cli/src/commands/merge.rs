use anyhow::{Context, Result};

use crate::{
    cli::MergeArgs,
    commands::transform_common::{compression_from_str, input_bytes_from_optional_file},
};

pub fn run(args: MergeArgs) -> Result<()> {
    if args.files.is_empty() {
        anyhow::bail!("merge requires at least one input file");
    }

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

    for path in args.files {
        let bytes = input_bytes_from_optional_file(Some(&path))?;
        let mut stream = mcap::MessageStream::new(&bytes)
            .with_context(|| format!("failed to read messages from {}", path.display()))?;
        while let Some(message) = stream.next() {
            let message = message
                .with_context(|| format!("failed decoding message in {}", path.display()))?;
            writer.write(&message).with_context(|| {
                format!("failed writing merged message from {}", path.display())
            })?;
        }
    }

    writer
        .finish()
        .with_context(|| format!("failed finalizing {}", args.output_file.display()))?;
    Ok(())
}
