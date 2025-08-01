use crate::error::CliResult;
use crate::utils::{format_bytes, format_table, validation::validate_input_file};
use clap::Args;
use mcap::Summary;
use memmap2::Mmap;
use std::fs::File;
use std::io::Write;

#[derive(Args)]
pub struct AttachmentsArgs {
    /// Path to MCAP file
    file: String,
}

pub async fn execute(args: AttachmentsArgs) -> CliResult<()> {
    // Validate input file
    validate_input_file(&args.file)?;

    // Memory map the file
    let file = File::open(&args.file)?;
    let mapped = unsafe { Mmap::map(&file)? };

    // Read the summary from the file
    let summary = Summary::read(&mapped)?;

    // Print the attachments
    print_attachments(&mut std::io::stdout(), &summary)?;

    Ok(())
}

fn print_attachments<W: Write>(writer: &mut W, summary: &Option<Summary>) -> std::io::Result<()> {
    if let Some(info) = summary {
        if info.attachment_indexes.is_empty() {
            writeln!(writer, "No attachments found")?;
            return Ok(());
        }

        let headers = vec![
            "offset",
            "length",
            "log_time",
            "create_time",
            "data_size",
            "name",
        ];
        let mut rows = Vec::new();

        for attachment in &info.attachment_indexes {
            rows.push(vec![
                attachment.offset.to_string(),
                format_bytes(attachment.length),
                attachment.log_time.to_string(),
                attachment.create_time.to_string(),
                format_bytes(attachment.data_size),
                attachment.name.clone(),
            ]);
        }

        format_table(writer, headers, rows)?;
    } else {
        writeln!(writer, "No attachment information available")?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_attachments_args() {
        let args = AttachmentsArgs {
            file: "test.mcap".to_string(),
        };
        assert_eq!(args.file, "test.mcap");
    }
}
