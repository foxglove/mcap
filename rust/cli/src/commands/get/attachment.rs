use crate::error::CliResult;
use crate::utils::validation::validate_input_file;
use clap::Args;
use mcap::Summary;
use memmap2::Mmap;
use std::fs::File;
use std::io::Write;

#[derive(Args)]
pub struct GetAttachmentArgs {
    /// MCAP file to extract attachment from
    mcap_file: String,

    /// Name of attachment to extract
    #[arg(short, long)]
    name: String,

    /// Offset of attachment (if multiple attachments with same name exist)
    #[arg(long)]
    offset: Option<u64>,

    /// Output file (defaults to stdout)
    #[arg(short, long)]
    output: Option<String>,
}

pub async fn execute(args: GetAttachmentArgs) -> CliResult<()> {
    // Validate input file
    validate_input_file(&args.mcap_file)?;

    println!("📎 Extracting attachment from MCAP file");
    println!("MCAP file: {}", args.mcap_file);
    println!("Attachment name: {}", args.name);

    // Open and map the MCAP file
    let file = File::open(&args.mcap_file)?;
    let mapped = unsafe { Mmap::map(&file)? };

    // Read the summary to get attachment indexes
    let summary = Summary::read(&mapped)?;
    let summary = summary.ok_or_else(|| {
        crate::error::CliError::invalid_argument(
            "MCAP file does not contain a summary section".to_string(),
        )
    })?;

    // Find attachments with the specified name
    let matching_attachments: Vec<_> = summary
        .attachment_indexes
        .iter()
        .filter(|idx| idx.name == args.name)
        .collect();

    if matching_attachments.is_empty() {
        return Err(crate::error::CliError::invalid_argument(format!(
            "Attachment '{}' not found",
            args.name
        )));
    }

    // Select the attachment to extract
    let attachment_idx = if matching_attachments.len() == 1 {
        matching_attachments[0]
    } else if let Some(offset) = args.offset {
        matching_attachments
            .iter()
            .find(|idx| idx.offset == offset)
            .ok_or_else(|| {
                crate::error::CliError::invalid_argument(format!(
                    "Attachment '{}' not found at offset {}",
                    args.name, offset
                ))
            })?
    } else {
        return Err(crate::error::CliError::invalid_argument(format!(
            "Multiple attachments named '{}' exist. Specify an offset with --offset",
            args.name
        )));
    };

    println!("  Offset: {}", attachment_idx.offset);
    println!("  Size: {} bytes", attachment_idx.data_size);
    println!("  Media type: {}", attachment_idx.media_type);

    // Extract the attachment data
    let attachment_data = extract_attachment_data(&mapped, attachment_idx)?;

    // Write to output
    match args.output {
        Some(output_path) => {
            let mut output_file = File::create(&output_path)?;
            output_file.write_all(&attachment_data)?;
            println!("✅ Attachment extracted to: {}", output_path);
        }
        None => {
            // Check if stdout is redirected
            if crate::utils::stdout_redirected() {
                std::io::stdout().write_all(&attachment_data)?;
            } else {
                return Err(crate::error::CliError::BinaryOutputRedirection);
            }
        }
    }

    Ok(())
}

fn extract_attachment_data(
    mapped: &[u8],
    attachment_idx: &mcap::records::AttachmentIndex,
) -> CliResult<Vec<u8>> {
    // Calculate the offset to the attachment data
    // The attachment record structure is:
    // - 1 byte: opcode
    // - 8 bytes: record length
    // - 8 bytes: log time
    // - 8 bytes: creation time
    // - 4 bytes: name length
    // - N bytes: name
    // - 4 bytes: media type length
    // - M bytes: media type
    // - 8 bytes: data length
    // - data

    let data_offset = attachment_idx.offset +
        1 + // opcode
        8 + // record length
        8 + // log time
        8 + // creation time
        4 + // name length
        attachment_idx.name.len() as u64 +
        4 + // media type length
        attachment_idx.media_type.len() as u64 +
        8; // data length

    let start = data_offset as usize;
    let end = start + attachment_idx.data_size as usize;

    if end > mapped.len() {
        return Err(crate::error::CliError::invalid_argument(
            "Attachment data extends beyond file bounds".to_string(),
        ));
    }

    Ok(mapped[start..end].to_vec())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_attachment_args() {
        let args = GetAttachmentArgs {
            mcap_file: "test.mcap".to_string(),
            name: "attachment1".to_string(),
            offset: Some(1234),
            output: Some("output.bin".to_string()),
        };
        assert_eq!(args.mcap_file, "test.mcap");
        assert_eq!(args.name, "attachment1");
        assert_eq!(args.offset, Some(1234));
        assert_eq!(args.output, Some("output.bin".to_string()));
    }
}
