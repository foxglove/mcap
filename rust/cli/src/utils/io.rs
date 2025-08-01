use anyhow::Result;
use memmap2::Mmap;
use std::fs::File;
use tokio::fs::File as AsyncFile;
use tokio::io::{AsyncRead, AsyncSeek};

use crate::utils::error::from_mcap_result;

pub enum FileInput {
    Local(String),
    Remote {
        scheme: String,
        bucket: String,
        path: String,
    },
}

impl FileInput {
    pub fn from_path(path: &str) -> Self {
        // Simple check for remote schemes
        if path.starts_with("gs://")
            || path.starts_with("s3://")
            || path.starts_with("http://")
            || path.starts_with("https://")
        {
            // Parse remote path
            // TODO: Implement proper remote path parsing
            Self::Remote {
                scheme: "gs".to_string(),
                bucket: "placeholder".to_string(),
                path: path.to_string(),
            }
        } else {
            Self::Local(path.to_string())
        }
    }
}

// Helper trait to combine AsyncRead and AsyncSeek
pub trait AsyncReadSeek: AsyncRead + AsyncSeek + Send + Unpin {}
impl<T: AsyncRead + AsyncSeek + Send + Unpin> AsyncReadSeek for T {}

pub async fn with_reader<T, F, Fut>(path: &str, f: F) -> Result<T>
where
    F: FnOnce(bool, Box<dyn AsyncReadSeek>) -> Fut,
    Fut: std::future::Future<Output = Result<T>>,
{
    let input = FileInput::from_path(path);
    match input {
        FileInput::Local(path) => {
            let file = AsyncFile::open(&path).await?;
            let reader: Box<dyn AsyncReadSeek> = Box::new(file);
            f(false, reader).await
        }
        FileInput::Remote { .. } => {
            // TODO: Implement remote file reading
            anyhow::bail!("Remote file support not yet implemented");
        }
    }
}

// Synchronous version for working with memory-mapped files
pub fn with_sync_reader<T, F>(path: &str, f: F) -> Result<T>
where
    F: FnOnce(File) -> Result<T>,
{
    let input = FileInput::from_path(path);
    match input {
        FileInput::Local(path) => {
            let file = File::open(&path)?;
            f(file)
        }
        FileInput::Remote { .. } => {
            anyhow::bail!("Remote file support not yet implemented");
        }
    }
}

// Helper function to memory-map an MCAP file
pub fn map_mcap_file(path: &str) -> Result<Mmap> {
    with_sync_reader(path, |file| {
        let mmap = unsafe { Mmap::map(&file)? };
        Ok(mmap)
    })
}

// Helper function to read MCAP summary from a file path with enhanced error handling
pub fn read_mcap_summary(path: &str) -> Result<Option<mcap::Summary>> {
    let mmap = map_mcap_file(path)?;
    let summary_result = mcap::Summary::read(&mmap);
    let summary = from_mcap_result(summary_result, path)?;
    Ok(summary)
}
