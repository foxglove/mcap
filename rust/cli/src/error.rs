use async_http_range_reader::AsyncHttpRangeReaderError;
use mcap::McapError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum CliError {
    #[error("Unexpected input was recieved: {0}")]
    UnexpectedInput(String),
    #[error("An unexpected response was returned: {0}")]
    UnexpectedResponse(String),
    #[error("An IO error occurred: {0}")]
    Io(#[from] tokio::io::Error),
    #[error("An error occurred when parsing a url: {0}")]
    UrlParse(#[from] url::ParseError),
    #[error("An http error occurred: {0}")]
    HttpReader(#[from] AsyncHttpRangeReaderError),
    #[error("An MCAP error occurred: {0}")]
    Mcap(#[from] McapError),
}

pub type CliResult<T> = Result<T, CliError>;
