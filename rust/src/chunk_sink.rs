use std::io::{Cursor, Seek, Write};

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
        }
    }
}

impl<W: Write> ChunkSink<W> {
    fn as_mut_write(&mut self) -> &mut dyn Write {
        match &mut self.buffer {
            Some(w) => w,
            None => &mut self.inner,
        }
    }

    pub fn finish(self) -> (W, std::io::Result<ChunkMode>) {
        let ChunkSink { mut inner, buffer } = self;
        let mode = match buffer {
            Some(buffer) => {
                let buffer = buffer.into_inner();
                if let Err(err) = inner.write_all(&buffer) {
                    return (inner, Err(err));
                }
                ChunkMode::Buffered { buffer }
            }
            None => ChunkMode::Direct,
        };
        (inner, Ok(mode))
    }
}

impl<W: Seek> ChunkSink<W> {
    fn as_mut_seek(&mut self) -> &mut dyn Seek {
        match &mut self.buffer {
            Some(w) => w,
            None => &mut self.inner,
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
