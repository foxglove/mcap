use binrw::BinRead;

use crate::{
    parse_record,
    records::{Footer, Record},
    sans_io::linear_reader::{LinearReadEvent, LinearReader, LinearReaderOptions},
    McapResult, Summary,
};
use std::io::SeekFrom;

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

#[derive(Default)]
pub struct SummaryReader {
    pos: u64,
    footer_buf: Vec<u8>,
    file_size: Option<u64>,
    state: State,
    summary: crate::Summary,
    summary_present: bool,
}

impl SummaryReader {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn next_event(&mut self) -> Option<McapResult<SummaryReadEvent>> {
        self.next_event_inner().transpose()
    }

    pub fn next_event_inner(&mut self) -> McapResult<Option<SummaryReadEvent>> {
        loop {
            match &mut self.state {
                State::SeekingToFooter => {
                    let Some(file_size) = self.file_size else {
                        return Ok(Some(SummaryReadEvent::SeekRequest(SeekFrom::End(-28))));
                    };
                    if file_size < 28 {
                        return Err(crate::McapError::UnexpectedEof);
                    }
                    let footer_start_pos = file_size - 28;
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
                    if *loaded_bytes >= 20 {
                        let mut cursor = std::io::Cursor::new(&self.footer_buf[..*loaded_bytes]);
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
                        return Ok(Some(SummaryReadEvent::ReadRequest(20 - *loaded_bytes)));
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

    pub fn notify_read(&mut self, n: usize) {
        match &mut self.state {
            State::ReadingFooter { loaded_bytes, .. } => {
                *loaded_bytes += n;
            }
            State::ReadingSummary { reader, .. } => {
                reader.notify_read(n);
            }
            _ => {}
        }
        self.pos += n as u64;
    }

    pub fn notify_seeked(&mut self, pos: u64) {
        // potential source for bugs: we assume the first seek that occurs is a seek to the
        // footer start. The user might seek somewhere else, we don't really have a way to tell.
        if self.file_size.is_none() {
            self.file_size = Some(pos + 28);
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
}
