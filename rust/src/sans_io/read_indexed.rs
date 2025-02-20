use binrw::BinRead;

use crate::{
    parse_record,
    read::ChannelAccumulator,
    records::{op, AttachmentIndex, Footer, Record},
    sans_io::read::{LinearReader, ReadAction},
    McapError, McapResult, Summary,
};
use std::io::SeekFrom;
pub enum ReadSeekAction {
    Read(usize),
    Seek(std::io::SeekFrom),
}

#[derive(Debug, thiserror::Error)]
pub enum IndexedReadError {
    #[error("unexpected seek: {0:?}")]
    UnexpectedSeek(SeekFrom),
    #[from(McapError)]
    #[error("parse error: {0}")]
    ParseError(McapError),
}

impl From<binrw::Error> for IndexedReadError {
    fn from(value: binrw::Error) -> Self {
        Self::ParseError(McapError::Parse(value))
    }
}
impl From<McapError> for IndexedReadError {
    fn from(value: McapError) -> Self {
        Self::ParseError(value)
    }
}

#[derive(Default)]
enum SummaryLoaderState {
    #[default]
    Start,
    ReadingFooter,
    ReadingSummary {
        reader: LinearReader,
        channeler: crate::read::ChannelAccumulator<'static>,
    },
}

#[derive(Default)]
pub struct SummaryLoader {
    footer_data: Vec<u8>,
    last_seek: Option<SeekFrom>,
    pos: u64,
    last_read_size: Option<usize>,
    state: SummaryLoaderState,
    summary: crate::Summary<'static>,
}

impl SummaryLoader {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn next_action_inner(&mut self) -> Result<Option<ReadSeekAction>, IndexedReadError> {
        if let Some(from) = self.last_seek.take() {
            match self.state {
                SummaryLoaderState::Start => {
                    self.state = SummaryLoaderState::ReadingFooter;
                }
                SummaryLoaderState::ReadingFooter => {
                    self.state = SummaryLoaderState::ReadingSummary {
                        reader: LinearReader::new(),
                        channeler: ChannelAccumulator::default(),
                    };
                }
                _ => {
                    return Err(IndexedReadError::UnexpectedSeek(from));
                }
            }
        }
        loop {
            match &mut self.state {
                SummaryLoaderState::Start => {
                    return Ok(Some(ReadSeekAction::Seek(SeekFrom::End(-28))))
                }
                SummaryLoaderState::ReadingFooter => {
                    if self.footer_data.len() >= 20 {
                        // figure out the footer length
                        let mut cursor = std::io::Cursor::new(&self.footer_data);
                        let footer = Footer::read_le(&mut cursor)?;
                        return Ok(Some(ReadSeekAction::Seek(SeekFrom::Start(
                            footer.summary_start,
                        ))));
                    }
                    return Ok(Some(ReadSeekAction::Read(20 - self.footer_data.len())));
                }
                SummaryLoaderState::ReadingSummary { reader, channeler } => {
                    match reader.next_action() {
                        Some(Ok(ReadAction::GetRecord { data, opcode })) => {
                            match parse_record(opcode, data)? {
                                Record::AttachmentIndex(i) => {
                                    self.summary.attachment_indexes.push(i)
                                }
                                Record::MetadataIndex(i) => self.summary.metadata_indexes.push(i),
                                Record::Statistics(s) => self.summary.stats = Some(s),
                                Record::Channel(c) => channeler.add_channel(c.clone())?,
                                Record::Schema { header, data } => {
                                    channeler.add_schema(header, data.to_owned())?
                                }
                                Record::ChunkIndex(ci) => self.summary.chunk_indexes.push(ci),
                                _ => {}
                            };
                            continue;
                        }
                        Some(Ok(ReadAction::NeedMore(n))) => {
                            return Ok(Some(ReadSeekAction::Read(n)));
                        }
                        Some(Err(err)) => {
                            return Err(err.into());
                        }
                        None => {
                            self.summary.schemas = channeler.schemas.clone();
                            self.summary.channels = channeler.channels.clone();
                            return Ok(None);
                        }
                    }
                }
            }
        }
    }

    pub fn set_seeked(&mut self, pos: usize) {
        unimplemented!()
    }

    pub fn set_read(&mut self, n: usize) {
        unimplemented!()
    }

    pub fn insert(&mut self, n: usize) -> &mut [u8] {
        unimplemented!()
    }

    pub fn finish(self) -> McapResult<Summary<'static>> {
        unimplemented!()
    }
}

pub struct IndexedReader {}

impl IndexedReader {
    pub fn new() -> Self {
        unimplemented!()
    }

    pub fn next_action(&mut self) -> Option<McapResult<ReadSeekAction>> {
        unimplemented!()
    }

    pub fn set_pos(&mut self, pos: usize) {
        unimplemented!()
    }

    pub fn set_read(&mut self, n: usize) {
        unimplemented!()
    }

    pub fn insert(&mut self, n: usize) -> &mut [u8] {
        unimplemented!()
    }
}
