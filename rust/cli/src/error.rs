use thiserror::Error;

pub type CliResult<T> = Result<T, CliError>;

#[derive(Error, Debug)]
pub enum CliError {
    #[error("MCAP error: {0}")]
    Mcap(#[from] mcap::McapError),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("Invalid argument: {0}")]
    InvalidArgument(String),

    #[error("File not found: {0}")]
    FileNotFound(String),

    #[error("Binary output can screw up your terminal. Supply -o or redirect to a file or pipe")]
    BinaryOutputRedirection,

    #[error("Configuration error: {0}")]
    Config(#[from] config::ConfigError),

    #[error("Cloud storage error: {0}")]
    #[cfg(feature = "cloud")]
    CloudStorage(#[from] cloud_storage::Error),

    #[error("Unexpected error: {0}")]
    Other(#[from] anyhow::Error),
}

impl CliError {
    pub fn invalid_argument<S: Into<String>>(msg: S) -> Self {
        CliError::InvalidArgument(msg.into())
    }

    pub fn file_not_found<S: Into<String>>(path: S) -> Self {
        CliError::FileNotFound(path.into())
    }
}
