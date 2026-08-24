use std::io::{self, prelude::*};

use crc32fast::Hasher;

/// Bytes are hashed in batches of at least this size (except at drain points), so the
/// hasher's SIMD fast path gets bulk input even when the writer receives many small
/// writes. Record serialization emits individual fields as 1-8 byte writes; hashing at
/// that granularity keeps crc32fast on its scalar fallback, which is more than an order
/// of magnitude slower than hashing the same bytes in bulk.
const HASH_BATCH_SIZE: usize = 32 * 1024;

pub struct CountingCrcWriter<W> {
    inner: W,
    hasher: Option<Hasher>,
    // Bytes already written to `inner` but not yet folded into `hasher`. Hash-only
    // batching: I/O is written through immediately, so ordering and flush semantics are
    // unaffected. Every read of the hasher (current_checksum, finalize) accounts for
    // these pending bytes.
    pending: Vec<u8>,
    count: u64,
}

impl<W> CountingCrcWriter<W> {
    pub fn new(inner: W, calculate_crc: bool) -> Self {
        Self::with_hasher(inner, calculate_crc.then(Hasher::new))
    }

    pub fn with_hasher(inner: W, hasher: Option<Hasher>) -> Self {
        Self {
            inner,
            hasher,
            pending: Vec::new(),
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
    pub fn finalize(mut self) -> (W, Option<Hasher>) {
        self.drain_pending();
        (self.inner, self.hasher)
    }

    pub fn current_checksum(&self) -> u32 {
        self.hasher
            .clone()
            .map(|mut hasher| {
                if !self.pending.is_empty() {
                    hasher.update(&self.pending);
                }
                hasher.finalize()
            })
            .unwrap_or(0)
    }

    fn drain_pending(&mut self) {
        if !self.pending.is_empty() {
            self.hasher
                .as_mut()
                .expect("pending bytes only accumulate while hashing")
                .update(&self.pending);
            self.pending.clear();
        }
    }
}

impl<W: Write> Write for CountingCrcWriter<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let res = self.inner.write(buf)?;
        self.count += res as u64;
        if self.hasher.is_some() {
            let written = &buf[..res];
            if written.len() >= HASH_BATCH_SIZE {
                // Large writes are hashed directly (after any pending bytes, to keep the
                // hashed stream in write order), skipping the extra copy.
                self.drain_pending();
                self.hasher.as_mut().expect("checked above").update(written);
            } else {
                self.pending.extend_from_slice(written);
                if self.pending.len() >= HASH_BATCH_SIZE {
                    self.drain_pending();
                }
            }
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

#[cfg(test)]
mod tests {
    use super::*;

    fn reference_crc(data: &[u8]) -> u32 {
        let mut hasher = Hasher::new();
        hasher.update(data);
        hasher.finalize()
    }

    #[test]
    fn batched_hash_matches_reference_across_write_sizes() {
        let data: Vec<u8> = (0..1_000_000u32).map(|i| (i % 251) as u8).collect();
        // Mix of tiny field-sized writes, payload-sized writes, and writes larger than
        // the batch size, mirroring real record serialization patterns.
        for sizes in [
            &[1usize, 2, 4, 8][..],
            &[100][..],
            &[HASH_BATCH_SIZE - 1][..],
            &[HASH_BATCH_SIZE][..],
            &[HASH_BATCH_SIZE + 1][..],
            &[3, 100, HASH_BATCH_SIZE + 7, 8, HASH_BATCH_SIZE][..],
        ] {
            let mut writer = CountingCrcWriter::new(Vec::new(), true);
            let mut offset = 0;
            let mut size_idx = 0;
            while offset < data.len() {
                let size = sizes[size_idx % sizes.len()].min(data.len() - offset);
                writer.write_all(&data[offset..offset + size]).unwrap();
                offset += size;
                size_idx += 1;
            }
            assert_eq!(writer.position(), data.len() as u64);
            // current_checksum must account for unhashed pending bytes without
            // disturbing the stream.
            assert_eq!(writer.current_checksum(), reference_crc(&data));
            assert_eq!(writer.current_checksum(), reference_crc(&data));
            let (inner, hasher) = writer.finalize();
            assert_eq!(inner, data);
            assert_eq!(hasher.unwrap().finalize(), reference_crc(&data));
        }
    }

    #[test]
    fn disabled_hasher_accumulates_nothing() {
        let mut writer = CountingCrcWriter::new(Vec::new(), false);
        writer.write_all(&[1, 2, 3]).unwrap();
        assert_eq!(writer.current_checksum(), 0);
        assert!(writer.pending.is_empty());
        let (inner, hasher) = writer.finalize();
        assert_eq!(inner, vec![1, 2, 3]);
        assert!(hasher.is_none());
    }
}
