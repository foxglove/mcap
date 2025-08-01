use crate::error::CliResult;
use crate::utils::{format_table, validation::validate_input_file};
use clap::Args;
use mcap::Summary;
use memmap2::Mmap;
use std::fs::File;
use std::io::Write;

#[derive(Args)]
pub struct ChannelsArgs {
    /// Path to MCAP file
    file: String,
}

pub async fn execute(args: ChannelsArgs) -> CliResult<()> {
    // Validate input file
    validate_input_file(&args.file)?;

    // Memory map the file
    let file = File::open(&args.file)?;
    let mapped = unsafe { Mmap::map(&file)? };

    // Read the summary from the file
    let summary = Summary::read(&mapped)?;

    // Print the channels
    print_channels(&mut std::io::stdout(), &summary)?;

    Ok(())
}

fn print_channels<W: Write>(writer: &mut W, summary: &Option<Summary>) -> std::io::Result<()> {
    if let Some(info) = summary {
        let headers = vec!["id", "schemaId", "topic", "messageEncoding", "metadata"];
        let mut rows = Vec::new();

        // Convert HashMap to sorted Vec for consistent output
        let mut channels: Vec<_> = info.channels.iter().collect();
        channels.sort_by_key(|(id, _)| *id);

        for (&channel_id, channel) in channels {
            let schema_id = channel
                .schema
                .as_ref()
                .map(|s| s.id.to_string())
                .unwrap_or_else(|| "0".to_string());

            // For now, simplified metadata representation
            let metadata = if channel.metadata.is_empty() {
                "{}".to_string()
            } else {
                format!("{{...{} entries...}}", channel.metadata.len())
            };

            rows.push(vec![
                channel_id.to_string(),
                schema_id,
                channel.topic.clone(),
                channel.message_encoding.clone(),
                metadata,
            ]);
        }

        format_table(writer, headers, rows)?;
    } else {
        writeln!(writer, "No channel information available")?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_channels_args() {
        let args = ChannelsArgs {
            file: "test.mcap".to_string(),
        };
        assert_eq!(args.file, "test.mcap");
    }
}
