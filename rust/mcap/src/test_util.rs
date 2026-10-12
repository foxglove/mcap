//! Helpers shared by the unit tests.

use std::io::{self, Cursor, Seek, SeekFrom, Write};

/// A `Write + Seek` sink that fails once `budget` bytes have been written, like a disk filling up.
pub(crate) struct FullDisk {
    pub(crate) inner: Cursor<Vec<u8>>,
    budget: u64,
}

impl FullDisk {
    pub(crate) fn new(budget: u64) -> Self {
        Self {
            inner: Cursor::new(Vec::new()),
            budget,
        }
    }
}

impl Write for FullDisk {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if self.inner.position() + buf.len() as u64 > self.budget {
            return Err(io::Error::other("No space left on device"));
        }
        self.inner.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl Seek for FullDisk {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.inner.seek(pos)
    }
}
