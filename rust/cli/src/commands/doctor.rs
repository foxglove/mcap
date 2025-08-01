use anyhow::Result;
use clap::Args;
use colored::*;
use std::path::PathBuf;

use crate::utils::io::read_mcap_summary;

#[derive(Args)]
pub struct DoctorArgs {
    /// MCAP file to check
    pub file: PathBuf,

    /// Require strict message ordering
    #[arg(long)]
    pub strict_message_order: bool,
}

struct Diagnosis {
    errors: Vec<String>,
    warnings: Vec<String>,
}

impl Diagnosis {
    fn new() -> Self {
        Self {
            errors: Vec::new(),
            warnings: Vec::new(),
        }
    }

    fn error(&mut self, message: String) {
        eprintln!("{}", message.red());
        self.errors.push(message);
    }

    fn warn(&mut self, message: String) {
        eprintln!("{}", message.yellow());
        self.warnings.push(message);
    }

    fn has_errors(&self) -> bool {
        !self.errors.is_empty()
    }
}

struct McapDoctor {
    path: String,
    strict_message_order: bool,
    diagnosis: Diagnosis,
}

impl McapDoctor {
    fn new(path: String, strict_message_order: bool) -> Self {
        Self {
            path,
            strict_message_order,
            diagnosis: Diagnosis::new(),
        }
    }

    fn examine(&mut self) -> Result<()> {
        println!("Examining {}", self.path);

        // Try to read the summary
        match read_mcap_summary(&self.path)? {
            Some(summary) => {
                self.validate_summary(&summary);
            }
            None => {
                self.diagnosis
                    .warn("File has no summary section".to_string());
            }
        }

        // Additional validation could be added here:
        // - Check for proper magic bytes
        // - Validate chunk structure
        // - Check message ordering if strict_message_order is enabled
        // - Validate schema references
        // - Check compression integrity
        // - Validate UTF-8 strings

        Ok(())
    }

    fn validate_summary(&mut self, summary: &mcap::Summary) {
        // Check for orphaned schemas (schemas not referenced by any channel)
        let mut referenced_schemas = std::collections::HashSet::new();
        for channel in summary.channels.values() {
            if let Some(schema) = &channel.schema {
                referenced_schemas.insert(schema.id);
            }
        }

        for schema in summary.schemas.values() {
            if !referenced_schemas.contains(&schema.id) {
                self.diagnosis.warn(format!(
                    "Schema {} is not referenced by any channel",
                    schema.id
                ));
            }
        }

        // Check for channels referencing non-existent schemas
        for channel in summary.channels.values() {
            if let Some(schema) = &channel.schema {
                if !summary.schemas.contains_key(&schema.id) {
                    self.diagnosis.error(format!(
                        "Channel {} references non-existent schema {}",
                        channel.id, schema.id
                    ));
                }
            }
        }

        // Basic statistics validation
        if let Some(stats) = &summary.stats {
            if stats.message_count == 0 {
                self.diagnosis.warn("File contains no messages".to_string());
            }

            if stats.channel_count == 0 {
                self.diagnosis.warn("File contains no channels".to_string());
            }

            // Check if message counts are consistent
            let total_messages: u64 = stats.channel_message_counts.values().sum();
            if total_messages != stats.message_count {
                self.diagnosis.error(format!(
                    "Sum of channel message counts ({}) does not match total message count ({})",
                    total_messages, stats.message_count
                ));
            }

            // Check for time ordering issues (basic check)
            if stats.message_start_time > stats.message_end_time {
                self.diagnosis
                    .error("Message start time is after message end time".to_string());
            }
        }
    }

    fn report(&self) -> Result<()> {
        if self.diagnosis.errors.is_empty() && self.diagnosis.warnings.is_empty() {
            println!("{}", "No issues found".green());
        } else {
            if !self.diagnosis.warnings.is_empty() {
                println!("\n{} warning(s) found:", self.diagnosis.warnings.len());
            }
            if !self.diagnosis.errors.is_empty() {
                println!("\n{} error(s) found:", self.diagnosis.errors.len());
            }
        }

        if self.diagnosis.has_errors() {
            anyhow::bail!("Encountered {} errors", self.diagnosis.errors.len());
        }

        Ok(())
    }
}

pub async fn run(args: DoctorArgs) -> Result<()> {
    let path = args.file.to_string_lossy().to_string();
    let mut doctor = McapDoctor::new(path, args.strict_message_order);

    doctor.examine()?;
    doctor.report()?;

    Ok(())
}
