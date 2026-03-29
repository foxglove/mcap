use std::io::{self, prelude::*};

use crc32fast::Hasher;

pub struct CountingCrcWriter<W> {
    inner: W,
    hasher: Option<Hasher>,
    count: u64,
}

impl<W> CountingCrcWriter<W> {
    pub fn new(inner: W, calculate_crc: bool) -> Self {
        Self {
            inner,
            hasher: if calculate_crc {
                Some(Hasher::new())
            } else {
                None
            },
            count: 0,
        }
    }

    pub fn with_hasher(inner: W, hasher: Option<Hasher>) -> Self {
        Self {
            inner,
            hasher,
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
    pub fn finalize(self) -> (W, Option<Hasher>) {
        (self.inner, self.hasher)
    }

    pub fn current_checksum(&self) -> u32 {
        self.hasher
            .clone()
            .map(|hasher| hasher.finalize())
            .unwrap_or(0)
    }
}

impl<W: Write> Write for CountingCrcWriter<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let res = self.inner.write(buf)?;
        self.count += res as u64;
        if let Some(hasher) = &mut self.hasher {
            hasher.update(&buf[..res]);
        }
        Ok(res)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

impl<W: Seek> Seek for CountingCrcWriter<W> {
    fn seek(&mut self, pos: io::SeekFrom) -> io::Result<u64> {
        self.inner.seek(pos)
    }

    fn stream_position(&mut self) -> io::Result<u64> {
        self.inner.stream_position()
    }
}
