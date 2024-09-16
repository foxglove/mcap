use crate::records::{op, Record};
use crate::{parse_record, McapResult, Message, Summary, MAGIC};
use std::io::SeekFrom;
use tokio::io::{AsyncRead, AsyncSeek};
use tokio::io::{AsyncReadExt, AsyncSeekExt};

use super::RecordReader;

const FOOTER_START_OFFSET_FROM_END: i32 = 8 + 4 + 8 + 8;
pub struct IndexedReader {
    summary: Summary<'static>,
    end_pos: i64,
}

#[derive(Default, Clone)]
pub struct IterMessageOptions {
    start_after: Option<u64>,
    end_before: Option<u64>,
    topics: Option<Vec<String>>,
    reverse: bool,
}

impl IndexedReader {
    pub async fn init<R>(reader: &mut R) -> McapResult<Self>
    where
        R: AsyncRead + AsyncSeek + std::marker::Unpin,
    {
        // seek to footer start
        let footer_start_pos = reader
            .seek(SeekFrom::End(-1 * (FOOTER_START_OFFSET_FROM_END as i64)))
            .await?;
        let file_end = footer_start_pos + FOOTER_START_OFFSET_FROM_END as u64;
        let mut footer_magic_buf: Box<[u8]> =
            vec![0; FOOTER_START_OFFSET_FROM_END as usize].into_boxed_slice();
        reader.read_exact(&mut footer_magic_buf[..]).await?;
        if &footer_magic_buf[(FOOTER_START_OFFSET_FROM_END - 8) as usize..] != MAGIC {
            return Err(crate::McapError::BadMagic);
        }
        let footer = match parse_record(op::FOOTER, &footer_magic_buf[..8])? {
            Record::Footer(f) => f,
            _ => return Err(crate::McapError::BadFooter),
        };
        // read the entire summary
        if footer.summary_offset_start > file_end {
            return Err(crate::McapError::BadFooter);
        }
        if footer.summary_start > file_end {
            return Err(crate::McapError::BadFooter);
        }
        if footer.summary_start == 0 {
            return Err(crate::McapError::NoIndexAvailable);
        }
        let summary_end = if footer.summary_offset_start != 0 {
            footer.summary_offset_start
        } else {
            file_end - (MAGIC.len() as u64)
        };
        if summary_end < footer.summary_start {
            return Err(crate::McapError::BadFooter);
        }
        let summary_len = summary_end - footer.summary_start;
        if summary_len > usize::MAX as u64 {
            return Err(crate::McapError::BadFooter);
        }
        // parse and store
        let summary = {
            let mut summary_vec: Vec<u8> = vec![0; summary_len as usize];
            reader.seek(SeekFrom::Start(footer.summary_start.into()));
            reader.read_exact(&mut summary_vec[..]).await?;
            Summary::from_buf(&summary_vec[..])?.into_owned()
        };
        Ok(Self {
            summary: summary,
            end_pos: file_end as i64,
        })
    }

    pub fn iter_messages(self, opts: &IterMessageOptions) -> MessageIterator {}
}

struct MessageIndex {
    timestamp: u64,
    offset: u64,
    chunk_slot_index: u32,
}

struct ChunkSlot {
    uncompressed: Vec<u8>,
    unread: u64,
}

pub struct MessageIterator<R> {
    r: RecordReader<R>,
    message_indexes: Vec<MessageIndex>,
    chunk_slots: Vec<ChunkSlot>,
    opts: IterMessageOptions,
}

impl<R: AsyncRead + AsyncSeek + std::marker::Unpin> MessageIterator<R> {}
