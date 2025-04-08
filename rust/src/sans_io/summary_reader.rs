use binrw::BinRead;

use crate::{
    parse_record,
    records::{Footer, Record},
    sans_io::linear_reader::{LinearReadEvent, LinearReader, LinearReaderOptions},
    McapError, McapResult, Summary, MAGIC,
};
use std::io::SeekFrom;

const FOOTER_RECORD_AND_END_MAGIC: usize = 1 // footer opcode
    + 8 // footer length
    + 8 // footer summary start field
    + 8 // footer summary offset start field
    + 4 // footer summary CRC field
    + 8; // end magic

/// Events returned by the summary reader. The summary reader yields
pub enum SummaryReadEvent {
    ReadRequest(usize),
    SeekRequest(std::io::SeekFrom),
}

#[derive(Default)]
enum State {
    #[default]
    SeekingToFooter,
    ReadingFooter {
        loaded_bytes: usize,
    },
    SeekingToSummary {
        summary_start: u64,
    },
    ReadingSummary {
        summary_start: u64,
        reader: Box<LinearReader>,
        channeler: crate::read::ChannelAccumulator<'static>,
    },
}

/// Reads the summary section of an MCAP file, parsing records and producing a [`crate::Summary`]
/// when done.
///
/// This struct does not perform any I/O on its own, instead it requests reads and seeks from the
/// caller and allows them to use their own I/O primitives.
/// ```no_run
/// use std::fs;
///
/// use tokio::fs::File as AsyncFile;
/// use tokio::io::{AsyncReadExt, AsyncSeekExt};
/// use std::io::{Read, Seek};
///
/// use mcap::sans_io::summary_reader::SummaryReadEvent;
/// use mcap::McapResult;
///
/// // Asynchronously...
/// async fn summarize_async() -> McapResult<Option<mcap::Summary>> {
///     let mut file = AsyncFile::open("in.mcap").await.expect("couldn't open file");
///     let mut reader = mcap::sans_io::summary_reader::SummaryReader::new();
///     while let Some(event) = reader.next_event() {
///         match event? {
///             SummaryReadEvent::ReadRequest(need) => {
///                 let written = file.read(reader.insert(need)).await?;
///                 reader.notify_read(written);
///             },
///             SummaryReadEvent::SeekRequest(to) => {
///                 reader.notify_seeked(file.seek(to).await?);
///             }
///         }
///     }
///     Ok(reader.finish())
/// }
///
/// // Or synchronously.
/// fn read_sync() -> McapResult<Option<mcap::Summary>> {
///     let mut file = fs::File::open("in.mcap")?;
///     let mut reader = mcap::sans_io::summary_reader::SummaryReader::new();
///     while let Some(event) = reader.next_event() {
///         match event? {
///             SummaryReadEvent::ReadRequest(need) => {
///                 let written = file.read(reader.insert(need))?;
///                 reader.notify_read(written);
///             },
///             SummaryReadEvent::SeekRequest(to) => {
///                 reader.notify_seeked(file.seek(to)?);
///             }
///         }
///     }
///     Ok(reader.finish())
/// }
/// ```
#[derive(Default)]
pub struct SummaryReader {
    pos: u64,
    footer_buf: Vec<u8>,
    file_size: Option<u64>,
    state: State,
    summary: crate::Summary,
    summary_present: bool,
    at_eof: bool,
}

impl SummaryReader {
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the next event from the reader. Call this repeatedly and act on the resulting
    /// events in order to read the MCAP summary.
    pub fn next_event(&mut self) -> Option<McapResult<SummaryReadEvent>> {
        self.next_event_inner().transpose()
    }

    pub fn next_event_inner(&mut self) -> McapResult<Option<SummaryReadEvent>> {
        loop {
            match &mut self.state {
                State::SeekingToFooter => {
                    let Some(file_size) = self.file_size else {
                        return Ok(Some(SummaryReadEvent::SeekRequest(SeekFrom::End(
                            -(FOOTER_RECORD_AND_END_MAGIC as i64),
                        ))));
                    };
                    if file_size < FOOTER_RECORD_AND_END_MAGIC as u64 + 8 {
                        return Err(crate::McapError::UnexpectedEof);
                    }
                    let footer_start_pos = file_size - FOOTER_RECORD_AND_END_MAGIC as u64;
                    if self.pos == footer_start_pos {
                        self.state = State::ReadingFooter { loaded_bytes: 0 };
                        continue;
                    } else {
                        return Ok(Some(SummaryReadEvent::SeekRequest(SeekFrom::Start(
                            footer_start_pos,
                        ))));
                    }
                }
                State::ReadingFooter { loaded_bytes } => {
                    if *loaded_bytes >= FOOTER_RECORD_AND_END_MAGIC {
                        let opcode = self.footer_buf[0];
                        // Ignore the length, it must always be 20 bytes
                        let footer_body = &self.footer_buf[1 + 8..FOOTER_RECORD_AND_END_MAGIC - 8];
                        let end_magic =
                            &self.footer_buf[FOOTER_RECORD_AND_END_MAGIC - 8..*loaded_bytes];
                        if opcode != crate::records::op::FOOTER {
                            return Err(McapError::BadFooter);
                        }
                        if end_magic != MAGIC {
                            return Err(McapError::BadMagic);
                        }
                        let mut cursor = std::io::Cursor::new(footer_body);
                        let footer = Footer::read_le(&mut cursor)?;
                        if footer.summary_start == 0 {
                            // There is no summary.
                            return Ok(None);
                        }
                        self.summary_present = true;
                        self.state = State::SeekingToSummary {
                            summary_start: footer.summary_start,
                        };
                        continue;
                    } else {
                        if self.at_eof {
                            return Err(McapError::UnexpectedEof);
                        }
                        return Ok(Some(SummaryReadEvent::ReadRequest(
                            FOOTER_RECORD_AND_END_MAGIC - *loaded_bytes,
                        )));
                    }
                }
                State::SeekingToSummary { summary_start } => {
                    if self.pos == *summary_start {
                        self.state = State::ReadingSummary {
                            summary_start: *summary_start,
                            reader: Box::new(LinearReader::new_with_options(
                                LinearReaderOptions::default().with_skip_start_magic(true),
                            )),
                            channeler: crate::read::ChannelAccumulator::default(),
                        };
                        continue;
                    } else {
                        return Ok(Some(SummaryReadEvent::SeekRequest(SeekFrom::Start(
                            *summary_start,
                        ))));
                    }
                }
                State::ReadingSummary {
                    reader, channeler, ..
                } => match reader.next_event() {
                    Some(Ok(LinearReadEvent::Record { data, opcode })) => {
                        match parse_record(opcode, data)?.into_owned() {
                            Record::AttachmentIndex(index) => {
                                self.summary.attachment_indexes.push(index);
                            }
                            Record::MetadataIndex(index) => {
                                self.summary.metadata_indexes.push(index);
                            }
                            Record::Statistics(statistics) => {
                                self.summary.stats = Some(statistics);
                            }
                            Record::Channel(channel) => channeler.add_channel(channel)?,
                            Record::Schema { header, data } => {
                                channeler.add_schema(header, data)?;
                            }
                            Record::ChunkIndex(index) => self.summary.chunk_indexes.push(index),
                            _ => {}
                        };
                        continue;
                    }
                    Some(Ok(LinearReadEvent::ReadRequest(n))) => {
                        return Ok(Some(SummaryReadEvent::ReadRequest(n)));
                    }
                    Some(Err(err)) => {
                        return Err(err);
                    }
                    None => {
                        self.summary.schemas = channeler.schemas.clone();
                        self.summary.channels = channeler.channels.clone();
                        return Ok(None);
                    }
                },
            }
        }
    }

    /// Inform the summary reader of the result of the latest read on the underlying stream.
    ///
    /// Panics if `n` is greater than the last `n` provided to [`Self::insert`].
    pub fn notify_read(&mut self, n: usize) {
        self.at_eof = n == 0;
        match &mut self.state {
            State::ReadingFooter { loaded_bytes, .. } => {
                assert!(
                    self.footer_buf.len() >= *loaded_bytes + n,
                    "notify_read called with n > last inserted length",
                );
                *loaded_bytes += n;
            }
            State::ReadingSummary { reader, .. } => {
                reader.notify_read(n);
            }
            _ => {}
        }
        self.pos += n as u64;
    }

    /// Inform the summary reader of the result of the latest seek of the underlying stream.
    pub fn notify_seeked(&mut self, pos: u64) {
        if self.at_eof && self.pos != pos {
            self.at_eof = false;
        }
        // limitation: we assume the first seek that occurs is a seek to the footer start. The user
        // might seek somewhere else, we don't really have a way to tell.
        if self.file_size.is_none() {
            self.file_size = Some(pos + FOOTER_RECORD_AND_END_MAGIC as u64);
        }
        if self.pos != pos {
            // if we're actively reading and got an unexpected seek, we need to reset.
            match self.state {
                State::ReadingFooter { .. } => {
                    self.footer_buf.clear();
                    self.state = State::SeekingToFooter;
                }
                State::ReadingSummary { summary_start, .. } => {
                    self.state = State::SeekingToSummary { summary_start };
                    self.summary = Summary::default();
                }
                _ => {}
            }
        }
        self.pos = pos;
    }

    /// Get a mutable buffer of size `n` to read new MCAP data into from the stream.
    pub fn insert(&mut self, n: usize) -> &mut [u8] {
        match &mut self.state {
            State::ReadingFooter { loaded_bytes } => {
                self.footer_buf.resize(*loaded_bytes + n, 0);
                &mut self.footer_buf[*loaded_bytes..]
            }
            State::ReadingSummary { reader, .. } => reader.insert(n),
            _ => {
                // we don't need data in any other state, but just for simplicity give the user a place
                // to put their bogus data.
                self.footer_buf.resize(n, 0);
                &mut self.footer_buf[..]
            }
        }
    }

    /// Get the finished summary information out of the reader. Returns None if the MCAP reader has
    /// no summary section.
    pub fn finish(self) -> Option<Summary> {
        if self.summary_present {
            Some(self.summary)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Seek};

    #[test]
    fn test_smoke() {
        let mut f = std::fs::File::open("tests/data/compressed.mcap").expect("could not open file");
        let mut summary_loader = SummaryReader::new();
        while let Some(event) = summary_loader.next_event() {
            let event = event.expect("got error instead of event");
            match event {
                SummaryReadEvent::ReadRequest(n) => {
                    let read = f.read(summary_loader.insert(n)).expect("failed file read");
                    summary_loader.notify_read(read);
                }
                SummaryReadEvent::SeekRequest(to) => {
                    let pos = f.seek(to).expect("failed file seek");
                    summary_loader.notify_seeked(pos);
                }
            }
        }
        let Some(summary) = summary_loader.finish() else {
            panic!("should have found a summary")
        };
        assert_eq!(summary.chunk_indexes.len(), 413);
    }

    #[test]
    fn test_truncated_mcap() {
        let mut buf = Vec::new();
        std::fs::File::open("tests/data/uncompressed.mcap")
            .expect("could not open file")
            .read_to_end(&mut buf)
            .expect("could not read file");
        let truncated = &buf[..buf.len() - 100];
        let mut cursor = std::io::Cursor::new(truncated);

        let mut summary_loader = SummaryReader::new();
        let mut failed = false;
        while let Some(event) = summary_loader.next_event() {
            match event {
                Ok(SummaryReadEvent::ReadRequest(n)) => {
                    let read = cursor
                        .read(summary_loader.insert(n))
                        .expect("failed file read");
                    summary_loader.notify_read(read);
                }
                Ok(SummaryReadEvent::SeekRequest(to)) => {
                    let pos = cursor.seek(to).expect("failed file seek");
                    summary_loader.notify_seeked(pos);
                }
                Err(err) => {
                    assert!(matches!(err, McapError::BadFooter));
                    failed = true;
                    break;
                }
            }
        }
        assert!(failed);
    }
}
