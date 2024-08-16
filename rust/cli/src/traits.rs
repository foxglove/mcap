use std::ops::Range;

use async_trait::async_trait;
use tokio::fs::File;
use tokio::io::{AsyncRead, AsyncSeek};

#[async_trait]
pub trait McapReader: AsyncSeek + AsyncRead + Unpin {
    async fn prefetch(&mut self, bytes: Range<u64>);
}

#[async_trait]
impl McapReader for File {
    async fn prefetch(&mut self, _bytes: Range<u64>) {
        // noop for files
    }
}
