use std::io::{Cursor, Seek, Write};

/// The kind of writer that should be used for writing chunks.
///
/// This is used to select what [`ChunkSink`] should be used by the MCAP writer.
pub(crate) enum ChunkMode {
    /// Mode specifying that chunks should be written directly to the output
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
pub(crate) enum ChunkSink<W> {
    Direct(W),
    Buffered(W, Cursor<Vec<u8>>),
}

impl<W: Write> ChunkSink<W> {
    fn as_mut_write(&mut self) -> &mut dyn Write {
        match self {
            Self::Direct(w) => w,
            Self::Buffered(_, w) => w,
        }
    }
}

impl<W: Seek> ChunkSink<W> {
    fn as_mut_seek(&mut self) -> &mut dyn Seek {
        match self {
            Self::Direct(w) => w,
            Self::Buffered(_, w) => w,
        }
    }
}

impl<W: Seek> Seek for ChunkSink<W> {
    fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
        self.as_mut_seek().seek(pos)
    }

    fn stream_position(&mut self) -> std::io::Result<u64> {
        self.as_mut_seek().stream_position()
    }
}

impl<W: Write> Write for ChunkSink<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.as_mut_write().write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.as_mut_write().flush()
    }
}
