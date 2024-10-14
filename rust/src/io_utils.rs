use std::io::{self, prelude::*};

use crc32fast::Hasher;

pub struct CountingCrcWriter<W> {
    inner: W,
    hasher: Hasher,
    count: u64,
}

impl<W: Write> CountingCrcWriter<W> {
    pub fn new(inner: W) -> Self {
        Self {
            inner,
            hasher: Hasher::new(),
            count: 0,
        }
    }

    pub fn position(&self) -> u64 {
        self.count
    }

    pub fn get_mut(&mut self) -> &mut W {
        &mut self.inner
    }

    /// Consumes the reader and returns the inner writer and the checksum
    pub fn finalize(self) -> (W, u32) {
        (self.inner, self.hasher.finalize())
    }
}

impl<W: Write> Write for CountingCrcWriter<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let res = self.inner.write(buf)?;
        self.count += res as u64;
        self.hasher.update(&buf[..res]);
        Ok(res)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}
