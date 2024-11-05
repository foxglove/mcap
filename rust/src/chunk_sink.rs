use std::io::{self, Cursor, Seek, Write};

trait WriteSeek: Write + Seek {}

impl<T: Write + Seek> WriteSeek for T {}

pub trait ChunkSink<W>: Write + Seek {
    fn wrap(writer: W) -> Self
    where
        Self: Sized;
    fn finish(self) -> io::Result<W>;
}

pub struct PassthroughChunkSink<W> {
    writer: W,
}

impl<W> PassthroughChunkSink<W> {
    pub fn new(writer: W) -> Self {
        Self { writer }
    }
}

macro_rules! impl_seek {
    ($seeker:tt) => {
        fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
            self.$seeker.seek(pos)
        }

        fn rewind(&mut self) -> std::io::Result<()> {
            self.$seeker.rewind()
        }

        fn seek_relative(&mut self, offset: i64) -> std::io::Result<()> {
            self.$seeker.seek_relative(offset)
        }

        fn stream_position(&mut self) -> std::io::Result<u64> {
            self.$seeker.stream_position()
        }
    };
}

macro_rules! impl_write {
    ($writer:tt) => {
        fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
            self.$writer.write_all(buf)
        }

        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.$writer.write(buf)
        }

        fn flush(&mut self) -> std::io::Result<()> {
            self.$writer.flush()
        }

        fn write_fmt(&mut self, fmt: std::fmt::Arguments<'_>) -> std::io::Result<()> {
            self.$writer.write_fmt(fmt)
        }

        fn write_vectored(&mut self, bufs: &[std::io::IoSlice<'_>]) -> std::io::Result<usize> {
            self.$writer.write_vectored(bufs)
        }
    };
}

impl<W: Seek> Seek for PassthroughChunkSink<W> {
    impl_seek!(writer);
}

impl<W: Write> Write for PassthroughChunkSink<W> {
    impl_write!(writer);
}

impl<W: Write + Seek> ChunkSink<W> for PassthroughChunkSink<W> {
    fn finish(self) -> io::Result<W> {
        Ok(self.writer)
    }

    fn wrap(writer: W) -> Self {
        Self { writer }
    }
}

pub struct BufferedChunkSink<W> {
    buffer: Cursor<Vec<u8>>,
    writer: W,
}

impl<W: Write> BufferedChunkSink<W> {
    pub fn new(writer: W) -> Self {
        Self {
            buffer: Cursor::new(vec![]),
            writer,
        }
    }
}

impl<W> Write for BufferedChunkSink<W> {
    impl_write!(buffer);
}

impl<W> Seek for BufferedChunkSink<W> {
    impl_seek!(buffer);
}

impl<W: Write> ChunkSink<W> for BufferedChunkSink<W> {
    fn wrap(writer: W) -> Self {
        Self::new(writer)
    }

    fn finish(mut self) -> io::Result<W> {
        self.writer.write_all(&self.buffer.into_inner())?;
        Ok(self.writer)
    }
}
