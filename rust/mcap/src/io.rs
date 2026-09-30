//! Blocking [`std::io`] adapters for the sans-io readers.
use std::{
    io::{self, Read},
    sync::Arc,
};

use crate::{
    sans_io::{LinearReaderOptions, MessageReadEvent, MessageReader as SansIoReader},
    Channel, McapResult, Message,
};

/// Streams linked [`Message`]s from any [`Read`] source, in file order, with memory bounded by
/// the largest record or chunk rather than the file.
///
/// This is the streaming counterpart of [`crate::MessageStream`]: it applies the same
/// schema/channel validation, stops at the end of the data section, and yields nothing further
/// after the first error.
///
/// ```no_run
/// use std::{fs, io::BufReader};
///
/// fn read_it() -> mcap::McapResult<()> {
///     let file = BufReader::new(fs::File::open("in.mcap")?);
///     for message in mcap::io::MessageReader::new(file) {
///         let message = message?;
///         println!("{} {}", message.log_time, message.channel.topic);
///     }
///     Ok(())
/// }
/// ```
pub struct MessageReader<R> {
    source: R,
    reader: SansIoReader,
    done: bool,
}

impl<R: Read> MessageReader<R> {
    /// Creates a reader with [`LinearReaderOptions::default`].
    pub fn new(source: R) -> Self {
        Self {
            source,
            reader: SansIoReader::new(),
            done: false,
        }
    }

    /// Creates a reader with the given options (see [`crate::sans_io::MessageReader`]).
    pub fn new_with_options(source: R, options: LinearReaderOptions) -> Self {
        Self {
            source,
            reader: SansIoReader::new_with_options(options),
            done: false,
        }
    }

    /// Gets a channel seen so far by ID.
    pub fn get_channel(&self, channel_id: u16) -> Option<Arc<Channel<'static>>> {
        self.reader.get_channel(channel_id)
    }

    /// Returns the underlying source.
    pub fn into_inner(self) -> R {
        self.source
    }
}

impl<R: Read> Iterator for MessageReader<R> {
    type Item = McapResult<Message<'static>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        while let Some(event) = self.reader.next_event() {
            match event {
                Ok(MessageReadEvent::ReadRequest(need)) => {
                    // `Interrupted` means retry, per the `Read` contract. Retry before
                    // `notify_read`, since notifying zero bytes would signal EOF.
                    let read = loop {
                        match self.source.read(self.reader.insert(need)) {
                            Ok(n) => break n,
                            Err(err) if err.kind() == io::ErrorKind::Interrupted => continue,
                            Err(err) => {
                                self.done = true;
                                return Some(Err(err.into()));
                            }
                        }
                    };
                    self.reader.notify_read(read);
                }
                Ok(MessageReadEvent::Message(message)) => return Some(Ok(message)),
                Err(err) => {
                    self.done = true;
                    return Some(Err(err));
                }
            }
        }
        self.done = true;
        None
    }
}

#[cfg(test)]
mod tests {
    use std::io::{self, Cursor, Read};

    use super::*;
    use crate::sans_io::message_reader::test_support::{
        payload, truncated_in_last_message, two_channel_mcap,
    };

    #[test]
    fn matches_message_stream() {
        for use_chunks in [false, true] {
            let mcap = two_channel_mcap(use_chunks);
            let expected = crate::MessageStream::new(&mcap)
                .expect("stream")
                .collect::<McapResult<Vec<_>>>()
                .expect("messages");
            let actual = MessageReader::new(Cursor::new(&mcap))
                .collect::<McapResult<Vec<_>>>()
                .expect("messages");
            assert_eq!(actual, expected, "use_chunks={use_chunks}");
            assert_eq!(actual[2].data, payload(3));
        }
    }

    /// Hands out one byte per read, so every record spans many read requests.
    struct OneByteAtATime<'a>(&'a [u8]);

    impl Read for OneByteAtATime<'_> {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            let n = 1.min(buf.len()).min(self.0.len());
            buf[..n].copy_from_slice(&self.0[..n]);
            self.0 = &self.0[n..];
            Ok(n)
        }
    }

    #[test]
    fn tolerates_short_reads() {
        let mcap = two_channel_mcap(true);
        let messages = MessageReader::new(OneByteAtATime(&mcap))
            .collect::<McapResult<Vec<_>>>()
            .expect("messages");
        assert_eq!(messages.len(), 4);
    }

    #[test]
    fn ends_after_the_first_error_even_when_errors_are_ignored() {
        let mcap = two_channel_mcap(false);
        let truncated = truncated_in_last_message(&mcap);
        // `flatten` discards errors; a reader that kept repeating the error would never end.
        let count = MessageReader::new(Cursor::new(truncated)).flatten().count();
        assert_eq!(count, 3, "the cut message is lost, the rest survive");
        let items: Vec<_> = MessageReader::new(Cursor::new(truncated)).collect();
        assert!(matches!(items.last(), Some(Err(McapError::UnexpectedEof))));
        assert_eq!(items.iter().filter(|item| item.is_err()).count(), 1);
    }

    /// Returns `Interrupted` on every other call, like a signal landing mid-read.
    struct InterruptEveryOther<'a> {
        bytes: &'a [u8],
        interrupt_next: bool,
    }

    impl Read for InterruptEveryOther<'_> {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            self.interrupt_next = !self.interrupt_next;
            if self.interrupt_next {
                return Err(io::Error::from(io::ErrorKind::Interrupted));
            }
            let n = buf.len().min(self.bytes.len());
            buf[..n].copy_from_slice(&self.bytes[..n]);
            self.bytes = &self.bytes[n..];
            Ok(n)
        }
    }

    #[test]
    fn retries_interrupted_reads() {
        let mcap = two_channel_mcap(true);
        let messages = MessageReader::new(InterruptEveryOther {
            bytes: &mcap,
            interrupt_next: false,
        })
        .collect::<McapResult<Vec<_>>>()
        .expect("interruptions are retried, not surfaced");
        assert_eq!(messages.len(), 4);
    }

    #[test]
    fn stops_before_the_summary() {
        let mcap = two_channel_mcap(true);
        let summary_start = crate::read::footer(&mcap).expect("footer").summary_start as u64;
        let mut reader = MessageReader::new(Cursor::new(&mcap));
        let messages = reader
            .by_ref()
            .collect::<McapResult<Vec<_>>>()
            .expect("messages");
        assert_eq!(messages.len(), 4);
        assert!(
            reader.into_inner().position() <= summary_start,
            "the reader must not consume the summary section"
        );
    }

    /// Fails after `ok_bytes` bytes, like a disk error mid-file.
    struct FailAfter<'a> {
        bytes: &'a [u8],
        ok_bytes: usize,
    }

    impl Read for FailAfter<'_> {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            if self.ok_bytes == 0 {
                return Err(io::Error::other("disk on fire"));
            }
            let n = buf.len().min(self.ok_bytes).min(self.bytes.len());
            buf[..n].copy_from_slice(&self.bytes[..n]);
            self.bytes = &self.bytes[n..];
            self.ok_bytes -= n;
            Ok(n)
        }
    }

    #[test]
    fn surfaces_source_errors_once() {
        let mcap = two_channel_mcap(false);
        let items: Vec<_> = MessageReader::new(FailAfter {
            bytes: &mcap,
            ok_bytes: mcap.len() / 2,
        })
        .collect();
        assert!(
            matches!(items.last(), Some(Err(McapError::Io(_)))),
            "{items:?}"
        );
        assert_eq!(items.iter().filter(|item| item.is_err()).count(), 1);
    }

    use crate::McapError;
}
