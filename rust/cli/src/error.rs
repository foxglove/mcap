use async_http_range_reader::AsyncHttpRangeReaderError;
use mcap::McapError;
use std::fmt::Display;
use thiserror::Error;
use tracing::debug;

#[derive(Debug, Error)]
pub enum CliError {
    #[error("{0}")]
    UnexpectedInput(String),
    #[error("{0}")]
    UnexpectedResponse(String),
    #[error("{0}")]
    Io(#[from] tokio::io::Error),
    #[error("An error occurred when parsing a url: {0}")]
    UrlParse(#[from] url::ParseError),
    #[error("{0}")]
    HttpReader(#[from] AsyncHttpRangeReaderError),
    #[error("{0}")]
    Mcap(#[from] McapError),
}

impl CliError {
    pub fn into_human_message(self, prefix: impl Display) -> String {
        debug!(target = "mcap::cli", "top level error was returned: {self:#?}");
        format!("{prefix}: {self}")
    }
}

pub type CliResult<T> = Result<T, CliError>;
