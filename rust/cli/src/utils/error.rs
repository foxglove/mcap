use thiserror::Error;

pub type Result<T> = std::result::Result<T, McapCliError>;

#[derive(Debug, Error)]
pub enum McapCliError {
    #[error("MCAP error: {0}")]
    Mcap(#[from] mcap::McapError),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Serialization error: {0}")]
    Serde(#[from] serde_json::Error),

    #[error("Not implemented: {0}")]
    NotImplemented(String),

    #[error("Invalid input: {0}")]
    InvalidInput(String),

    #[error("File not found: {0}")]
    FileNotFound(String),
}

/// Enhanced error handling for MCAP files with better error messages
pub fn enhance_mcap_error(error: mcap::McapError, file_path: &str) -> anyhow::Error {
    match error {
        mcap::McapError::BadMagic => {
            // Try to read the actual bytes at the start of the file to show what was found
            match std::fs::File::open(file_path) {
                Ok(mut file) => {
                    use std::io::Read;
                    let mut buffer = [0u8; 8];
                    match file.read_exact(&mut buffer) {
                        Ok(_) => {
                            anyhow::anyhow!("Invalid magic at start of file, found: {:?}", buffer)
                        }
                        Err(_) => {
                            anyhow::anyhow!("Invalid magic at start of file (could not read bytes)")
                        }
                    }
                }
                Err(_) => anyhow::anyhow!("Invalid magic at start of file (could not open file)"),
            }
        }
        other => anyhow::anyhow!("MCAP error: {}", other),
    }
}

/// Create an enhanced error result from an MCAP result
pub fn from_mcap_result<T>(result: mcap::McapResult<T>, file_path: &str) -> anyhow::Result<T> {
    result.map_err(|err| enhance_mcap_error(err, file_path))
}
