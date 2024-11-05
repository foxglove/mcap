use std::io::{Cursor, Seek, Write};

/// The kind of writer that should be used for writing chunks.
///
/// This is used to select what [`ChunkSink`] should be used by the MCAP writer.
pub enum ChunkMode {
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
pub enum ChunkSink<W> {
    Direct(W),
    Buffered(W, Cursor<Vec<u8>>),
}

impl<W: Seek> Seek for ChunkSink<W> {
    fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
        match self {
            Self::Direct(w) => w.seek(pos),
            Self::Buffered(_, w) => w.seek(pos),
        }
    }

    fn stream_position(&mut self) -> std::io::Result<u64> {
        match self {
            Self::Direct(w) => w.stream_position(),
            Self::Buffered(_, w) => w.stream_position(),
        }
    }
}

impl<W: Write> Write for ChunkSink<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self {
            Self::Direct(w) => w.write(buf),
            Self::Buffered(_, w) => w.write(buf),
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            Self::Direct(w) => w.flush(),
            Self::Buffered(_, w) => w.flush(),
        }
    }
}
