use std::io::{Cursor, Seek, SeekFrom, Write};

/// The kind of writer that should be used for writing chunks.
///
/// This is used to select what [`ChunkSink`] should be used by the MCAP writer.
#[derive(Default)]
pub(crate) enum ChunkMode {
    /// Mode specifying that chunks should be written directly to the output
    #[default]
    Direct,
    /// Mode specifying that chunks should be buffered before writing to the output
    Buffered {
        /// The reusable buffer used by the [`ChunkSink`] when writing to [`ChunkSink::Buffered`]
        buffer: Vec<u8>,
    },
}

/// The writer used for writing chunks.
///
/// If chunks are buffered they will be written to an internal buffer, which can be flushed to the
/// provided writer once the chunk is completed.
pub(crate) struct ChunkSink<W> {
    pub inner: W,
    pub buffer: Option<Cursor<Vec<u8>>>,
    /// The compressor's frame header (see [`Self::hold_frame_header`]), held here until it is
    /// written to `inner`. Only used when writing directly.
    frame_header: Vec<u8>,
    /// Whether the next write is the compressor's frame header and should be held back.
    expecting_frame_header: bool,
}

impl<W> ChunkSink<W> {
    pub fn new(writer: W, mode: ChunkMode) -> Self {
        Self {
            inner: writer,
            buffer: match mode {
                ChunkMode::Buffered { mut buffer } => {
                    // ensure the buffer is empty before using it for the chunk
                    buffer.clear();
                    Some(Cursor::new(buffer))
                }
                ChunkMode::Direct => None,
            },
            frame_header: Vec::new(),
            expecting_frame_header: false,
        }
    }

    /// Treats the next write as the compressor's frame header and keeps it in memory instead of
    /// passing it to the underlying writer, so that it cannot fail. The frame header is written
    /// through ahead of the next write, flush, seek, or [`Self::finish`], and any error surfaces
    /// there instead.
    ///
    /// This exists for compressors that take ownership of the sink while they write their frame
    /// header, and drop the sink if that write fails. By the time the frame header is written
    /// through, the caller owns the compressor and can always get the sink back from it. The sink
    /// cannot tell where the frame header ends: it holds exactly one write, so the compressor
    /// must emit its frame header as a single write.
    ///
    /// This is the compressor's frame header, not the MCAP chunk header, which is written before
    /// the compressor is set up.
    ///
    /// Has no effect when chunks are buffered, since writes then go to memory anyway.
    // Only the lz4 compressor needs this today.
    #[cfg_attr(not(feature = "lz4"), allow(dead_code))]
    pub fn hold_frame_header(&mut self) {
        if self.buffer.is_none() {
            self.expecting_frame_header = true;
        }
    }
}

impl<W: Write> ChunkSink<W> {
    /// Writes a held frame header through to the underlying writer.
    fn write_frame_header(&mut self) -> std::io::Result<()> {
        if !self.frame_header.is_empty() {
            self.inner.write_all(&self.frame_header)?;
            self.frame_header.clear();
        }
        Ok(())
    }

    pub fn finish(self) -> (W, std::io::Result<ChunkMode>) {
        let ChunkSink {
            mut inner,
            buffer,
            frame_header,
            ..
        } = self;
        let mode = match buffer {
            Some(buffer) => {
                let buffer = buffer.into_inner();
                if let Err(err) = inner.write_all(&buffer) {
                    return (inner, Err(err));
                }
                ChunkMode::Buffered { buffer }
            }
            None => {
                if let Err(err) = inner.write_all(&frame_header) {
                    return (inner, Err(err));
                }
                ChunkMode::Direct
            }
        };
        (inner, Ok(mode))
    }
}

impl<W: Write + Seek> Seek for ChunkSink<W> {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        match &mut self.buffer {
            Some(buffer) => buffer.seek(pos),
            None => {
                self.write_frame_header()?;
                self.inner.seek(pos)
            }
        }
    }

    fn stream_position(&mut self) -> std::io::Result<u64> {
        match &mut self.buffer {
            Some(buffer) => buffer.stream_position(),
            None => {
                self.write_frame_header()?;
                self.inner.stream_position()
            }
        }
    }
}

impl<W: Write> Write for ChunkSink<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match &mut self.buffer {
            Some(buffer) => buffer.write(buf),
            None if self.expecting_frame_header => {
                self.expecting_frame_header = false;
                self.frame_header.extend_from_slice(buf);
                Ok(buf.len())
            }
            None => {
                self.write_frame_header()?;
                self.inner.write(buf)
            }
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match &mut self.buffer {
            Some(buffer) => buffer.flush(),
            None => {
                self.write_frame_header()?;
                self.inner.flush()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A writer that fails once `budget` bytes have been written.
    struct FullDisk {
        inner: Cursor<Vec<u8>>,
        budget: u64,
    }

    impl FullDisk {
        fn new(budget: u64) -> Self {
            Self {
                inner: Cursor::new(Vec::new()),
                budget,
            }
        }
    }

    impl Write for FullDisk {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            if self.inner.position() + buf.len() as u64 > self.budget {
                return Err(std::io::Error::other("No space left on device"));
            }
            self.inner.write(buf)
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn frame_header_goes_through_ahead_of_the_next_operation() {
        let mut sink = ChunkSink::new(Cursor::new(Vec::new()), ChunkMode::Direct);
        sink.hold_frame_header();
        sink.write_all(b"header").unwrap();
        assert!(sink.inner.get_ref().is_empty());
        assert_eq!(sink.stream_position().unwrap(), 6);
        assert_eq!(sink.inner.get_ref(), b"header");
        sink.write_all(b"data").unwrap();
        assert_eq!(sink.inner.get_ref(), b"headerdata");
    }

    #[test]
    fn frame_header_write_error_surfaces_later() {
        let mut sink = ChunkSink::new(FullDisk::new(0), ChunkMode::Direct);
        sink.hold_frame_header();
        sink.write_all(b"header").unwrap();
        assert!(sink.write_all(b"data").is_err());
        assert!(sink.finish().1.is_err());
    }

    #[test]
    fn holding_the_frame_header_has_no_effect_when_buffered() {
        let mode = ChunkMode::Buffered { buffer: Vec::new() };
        let mut sink = ChunkSink::new(FullDisk::new(0), mode);
        sink.hold_frame_header();
        sink.write_all(b"header").unwrap();
        assert!(sink.frame_header.is_empty());
        assert!(sink.finish().1.is_err());
    }
}
