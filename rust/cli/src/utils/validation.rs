use crate::error::{CliError, CliResult};
use std::path::Path;

/// Validate that a file exists and is readable
pub fn validate_input_file(path: &str) -> CliResult<()> {
    // Skip validation for remote URLs
    if path.contains("://") {
        return Ok(());
    }

    let path = Path::new(path);

    if !path.exists() {
        return Err(CliError::file_not_found(path.display().to_string()));
    }

    if !path.is_file() {
        return Err(CliError::invalid_argument(format!(
            "Path is not a file: {}",
            path.display()
        )));
    }

    // Check if we can read the file
    if let Err(e) = std::fs::File::open(path) {
        return Err(CliError::Io(e));
    }

    Ok(())
}

/// Validate that an output path is writable
pub fn validate_output_file(path: &str) -> CliResult<()> {
    let path = Path::new(path);

    // Check if parent directory exists
    if let Some(parent) = path.parent() {
        if !parent.exists() {
            return Err(CliError::invalid_argument(format!(
                "Parent directory does not exist: {}",
                parent.display()
            )));
        }
    }

    // If file exists, check if it's writable
    if path.exists() {
        if path.is_dir() {
            return Err(CliError::invalid_argument(format!(
                "Output path is a directory: {}",
                path.display()
            )));
        }

        // Try to open for writing to check permissions
        if let Err(e) = std::fs::OpenOptions::new().write(true).open(path) {
            return Err(CliError::Io(e));
        }
    }

    Ok(())
}

/// Validate topic filter patterns
pub fn validate_topic_filter(filter: &str) -> CliResult<()> {
    // Basic validation - could be extended with regex validation
    if filter.is_empty() {
        return Err(CliError::invalid_argument("Topic filter cannot be empty"));
    }

    // Check for invalid characters that might cause issues
    if filter.contains('\0') {
        return Err(CliError::invalid_argument(
            "Topic filter contains null character",
        ));
    }

    Ok(())
}

/// Validate time range parameters
pub fn validate_time_range(start: Option<u64>, end: Option<u64>) -> CliResult<()> {
    if let (Some(start), Some(end)) = (start, end) {
        if start >= end {
            return Err(CliError::invalid_argument(
                "Start time must be before end time",
            ));
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[test]
    fn test_validate_input_file() {
        // Test with non-existent file
        assert!(validate_input_file("/non/existent/file.mcap").is_err());

        // Test with valid file
        let temp_file = NamedTempFile::new().unwrap();
        assert!(validate_input_file(temp_file.path().to_str().unwrap()).is_ok());

        // Test with remote URL (should pass validation)
        assert!(validate_input_file("gs://bucket/file.mcap").is_ok());
    }

    #[test]
    fn test_validate_topic_filter() {
        assert!(validate_topic_filter("/valid/topic").is_ok());
        assert!(validate_topic_filter("").is_err());
        assert!(validate_topic_filter("topic\0with\0null").is_err());
    }

    #[test]
    fn test_validate_time_range() {
        assert!(validate_time_range(None, None).is_ok());
        assert!(validate_time_range(Some(100), None).is_ok());
        assert!(validate_time_range(None, Some(200)).is_ok());
        assert!(validate_time_range(Some(100), Some(200)).is_ok());
        assert!(validate_time_range(Some(200), Some(100)).is_err());
        assert!(validate_time_range(Some(100), Some(100)).is_err());
    }
}
