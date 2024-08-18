use std::ops::Range;

use ::url::Url;
use async_trait::async_trait;
use tokio::fs::File;
use tokio::io::{AsyncRead, AsyncSeek};
use tracing::instrument;

use crate::error::{CliError, CliResult};

mod gcs;
mod url;

/// Asynchronously seek, read and prefetch bytes in a way that can be used by [`mcap::tokio::RecordReader`]
#[async_trait]
pub trait SeekableMcapReader: AsyncSeek + AsyncRead + Unpin {
    async fn prefetch(&mut self, bytes: Range<u64>);
}

#[async_trait]
impl SeekableMcapReader for File {
    async fn prefetch(&mut self, _bytes: Range<u64>) {
        // noop for files
    }
}

/// A file descriptor representing a file supported by the MCAP readers
#[derive(Debug)]
pub enum McapFd {
    Gcs {
        bucket_name: String,
        object_name: String,
    },
    Url(Url),
    File(String),
}

fn is_valid_scheme(scheme: &str) -> bool {
    ["https", "http"].contains(&scheme)
}

impl McapFd {
    /// Parse a string value into an `McapFd`
    ///
    /// If the string is poorly formed this method will return an error
    pub fn parse(path: String) -> CliResult<McapFd> {
        if path.starts_with("gs://") {
            let Some((bucket_name, object_name)) = path.trim_start_matches("gs://").split_once('/')
            else {
                return Err(CliError::UnexpectedInput(
                    "The provided path was not a valid GCS url.".to_string(),
                ));
            };

            Ok(McapFd::Gcs {
                bucket_name: bucket_name.into(),
                object_name: object_name.into(),
            })
        } else {
            // If the path is a
            if let Ok(url) = Url::parse(&path) {
                if !is_valid_scheme(url.scheme()) {
                    return Err(CliError::UnexpectedInput(format!(
                        "The provided remote scheme '{}' is not supported.",
                        url.scheme()
                    )));
                }

                return Ok(McapFd::Url(url));
            }

            Ok(McapFd::File(path))
        }
    }

    /// Create a [`SeekableMcapReader`] for the current descriptor
    #[instrument]
    pub async fn create_seekable_reader(
        &self,
    ) -> CliResult<std::pin::Pin<Box<dyn SeekableMcapReader>>> {
        match self {
            Self::File(path) => Ok(Box::pin(File::open(path).await?)),
            Self::Url(url) => Ok(Box::pin(
                url::create_seekable_url_reader(url.clone(), Default::default()).await?,
            )),
            Self::Gcs {
                bucket_name,
                object_name,
            } => Ok(Box::pin(
                gcs::create_seekable_gcs_reader(bucket_name, object_name).await?,
            )),
        }
    }

    /// Create a reader for the current descriptor that implements `AsyncRead`
    #[instrument]
    pub async fn create_reader(&self) -> CliResult<std::pin::Pin<Box<dyn AsyncRead>>> {
        match self {
            Self::File(path) => Ok(Box::pin(File::open(path).await?)),
            Self::Url(url) => Ok(Box::pin(
                url::create_url_reader(url.clone(), Default::default()).await?,
            )),
            Self::Gcs {
                bucket_name,
                object_name,
            } => Ok(Box::pin(
                gcs::create_gcs_reader(bucket_name, object_name).await?,
            )),
        }
    }
}
