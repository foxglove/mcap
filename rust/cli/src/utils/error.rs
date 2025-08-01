use thiserror::Error;

pub type Result<T> = std::result::Result<T, McapCliError>;

#[derive(Error, Debug)]
pub enum McapCliError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("MCAP error: {0}")]
    Mcap(#[from] mcap::McapError),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("Command not implemented: {0}")]
    NotImplemented(String),

    #[error("Invalid input: {0}")]
    InvalidInput(String),

    #[error("File not found: {0}")]
    FileNotFound(String),
}
