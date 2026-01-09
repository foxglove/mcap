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
    options: SummaryReaderOptions,
}

#[derive(Debug, Default)]
pub struct SummaryReaderOptions {
    /// The file size, if known in advance.
    pub file_size: Option<u64>,
    /// If Some(limit), the reader will return an error on any record with length > `limit`.
    pub record_length_limit: Option<usize>,
}

impl SummaryReaderOptions {
    /// Configure the reader with a known file size.
    pub fn with_file_size(mut self, size: u64) -> Self {
        self.file_size = Some(size);
        self
    }

    /// Configure the reader to return an error on any record with length > `limit`.
    pub fn with_record_length_limit(mut self, limit: usize) -> Self {
        self.record_length_limit = Some(limit);
        self
    }
}

/// Returns the lesser of the remaining file size, and the configured record length limit.
fn compute_record_length_limit(
    pos: u64,
    file_size: Option<u64>,
    record_length_limit: Option<usize>,
) -> Option<usize> {
    let remain =
        file_size.map(|size| usize::try_from(size.saturating_sub(pos)).unwrap_or(usize::MAX));
    match (remain, record_length_limit) {
        (Some(remain), Some(limit)) => Some(std::cmp::min(remain, limit)),
        (Some(remain), None) => Some(remain),
        (None, Some(limit)) => Some(limit),
        (None, None) => None,
    }
}

impl SummaryReader {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn new_with_options(options: SummaryReaderOptions) -> Self {
        Self {
            file_size: options.file_size,
            options,
            ..Default::default()
        }
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
                    // trailing end-magic is 8 bytes
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
                        // Footer record bodies are fixed-size (20 bytes): summary_start(u64) + summary_offset_start(u64) + crc(u32).
                        // We ignore the encoded length and slice the known body range.
                        // | 1          | 8                  | 20           | 8              |
                        // | opcode(u8) | record_length(u64) | footer_body  | magic(8 bytes) |
                        let footer_body = &self.footer_buf[9..FOOTER_RECORD_AND_END_MAGIC - 8];
                        let end_magic =
                            &self.footer_buf[FOOTER_RECORD_AND_END_MAGIC - 8..*loaded_bytes];

                        if end_magic != MAGIC {
                            return Err(McapError::BadMagic);
                        }
                        if opcode != crate::records::op::FOOTER {
                            return Err(McapError::BadFooter);
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
                        let mut options =
                            LinearReaderOptions::default().with_skip_start_magic(true);
                        if let Some(limit) = compute_record_length_limit(
                            self.pos,
                            self.file_size,
                            self.options.record_length_limit,
                        ) {
                            options = options.with_record_length_limit(limit);
                        }
                        self.state = State::ReadingSummary {
                            summary_start: *summary_start,
                            reader: Box::new(LinearReader::new_with_options(options)),
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
    use assert_matches::assert_matches;
    use std::io::{Read, Seek, Write};

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
                    assert!(matches!(err, McapError::BadMagic));
                    failed = true;
                    break;
                }
            }
        }
        assert!(failed);
    }

    #[test]
    fn test_bounds() {
        let buf = Vec::new();
        let mut file = std::io::Cursor::new(buf);

        // Write just the magic.
        file.write_all(MAGIC).unwrap();
        assert_matches!(Summary::read(file.get_ref()), Err(McapError::UnexpectedEof));

        // Write a statistics section.
        let stats_record = file.stream_position().unwrap();
        file.write_all(&[0x0b]).unwrap();
        file.write_all(&46_u64.to_le_bytes()).unwrap();
        file.write_all(&[0x0; 46]).unwrap();

        // Footer record header.
        let footer_record = file.stream_position().unwrap();
        file.write_all(&[0x02]).unwrap();
        file.write_all(&20_u64.to_le_bytes()).unwrap();
        let footer_content = file.stream_position().unwrap();

        // Write out an footer record with invalid offsets.
        file.write_all(&[0xff; 20]).unwrap();
        file.write_all(MAGIC).unwrap();
        assert_matches!(Summary::read(file.get_ref()), Err(McapError::UnexpectedEof));

        // Write a footer with no summary offset.
        file.seek(SeekFrom::Start(footer_content)).unwrap();
        file.write_all(&[0x00; 20]).unwrap();
        file.write_all(MAGIC).unwrap();
        assert_matches!(Summary::read(file.get_ref()), Ok(None));

        // Write out a valid footer that points to itself as the start of the summary section.
        file.seek(SeekFrom::Start(footer_content)).unwrap();
        file.write_all(&footer_record.to_le_bytes()).unwrap();
        file.write_all(&[0x00; 12]).unwrap();
        file.write_all(MAGIC).unwrap();
        assert_matches!(
            Summary::read(file.get_ref()),
            Ok(Some(Summary { stats: None, .. }))
        );

        // Write out a valid footer that points to the stats record.
        file.seek(SeekFrom::Start(footer_content)).unwrap();
        file.write_all(&stats_record.to_le_bytes()).unwrap();
        file.write_all(&[0x00; 12]).unwrap();
        file.write_all(MAGIC).unwrap();
        assert_matches!(
            Summary::read(file.get_ref()),
            Ok(Some(Summary { stats: Some(_), .. }))
        );

        // Update stats header length to a value too large to allocate.
        file.seek(SeekFrom::Start(stats_record + 1)).unwrap();
        file.write_all(&[0x11; 8]).unwrap();
        assert_matches!(
            Summary::read(file.get_ref()),
            Err(McapError::RecordTooLarge { opcode: 0x0b, .. })
        );

        // Update stats header length to u64::MAX to probe for overflow arithmetic.
        file.seek(SeekFrom::Start(stats_record + 1)).unwrap();
        file.write_all(&[0xff; 8]).unwrap();
        assert_matches!(
            Summary::read(file.get_ref()),
            Err(McapError::RecordTooLarge { opcode: 0x0b, .. })
        );

        // Update the footer to point to itself as the start of the summary section again, so that
        // the reader ignores the stats record with the invalid length.
        file.seek(SeekFrom::Start(footer_content)).unwrap();
        file.write_all(&footer_record.to_le_bytes()).unwrap();

        // Update footer header length to extend one byte beyond the end of the file (20 bytes for
        // record, 8 bytes for magic, 1 byte for fun).
        file.seek(SeekFrom::Start(footer_record + 1)).unwrap();
        file.write_all(&(20_u64 + 8 + 1).to_le_bytes()).unwrap();
        assert_matches!(Summary::read(file.get_ref()), Err(McapError::UnexpectedEof));

        // Update footer header length to a value that's too large to be allocated.
        file.seek(SeekFrom::Start(footer_record + 1)).unwrap();
        file.write_all(&[0x11; 8]).unwrap();
        assert_matches!(
            Summary::read(file.get_ref()),
            Err(McapError::RecordTooLarge { opcode: 2, .. })
        );

        // Update footer header length to u64::MAX to probe for overflow arithmetic.
        file.seek(SeekFrom::Start(footer_record + 1)).unwrap();
        file.write_all(&[0xff; 8]).unwrap();
        assert_matches!(
            Summary::read(file.get_ref()),
            Err(McapError::RecordTooLarge { opcode: 2, .. })
        );
    }
}
