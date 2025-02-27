use binrw::BinRead;

use crate::{
    records::{op, ChunkIndex, MessageHeader},
    McapError, McapResult,
};
use std::{collections::BTreeSet, io::SeekFrom, ops::Deref};

#[derive(Clone, Copy)]
struct MessageIndex {
    chunk_slot_idx: usize,
    log_time: u64,
    offset: usize,
}

/// Events yielded by the IndexedReader.
pub enum IndexedReadEvent<'a> {
    /// The reader needs more data to provide the next record. Call [`IndexedReader::insert`] then
    /// [`IndexedReader::notify_read`] to load more data. The value provided here is a hint for how
    /// much data to insert.
    ReadRequest(usize),
    /// The reader needs to seek to a different position in the file. Call
    /// [`IndexedReader::notify_seeked`] to inform the reader of the result of the seek.
    SeekRequest(SeekFrom),
    /// Get a new message from the reader.
    Message {
        header: crate::records::MessageHeader,
        data: &'a [u8],
    },
}
enum State {
    SeekingToChunk,
    LoadingChunkData { into_slot: usize },
    YieldingMessages,
    Done,
}

struct ChunkSlot {
    buf: Vec<u8>,
    message_count: usize,
}

/// Reads messages from an MCAP file using index information from the summary. This enables
/// efficient filtering by topic, time range, and efficient iteration in log-time order.
///
/// This struct does not perform any I/O on its own, instead it requests reads and seeks from the
/// caller and allows them to use their own I/O primitives.
/// ```no_run
/// use std::fs;
///
/// use std::io::{Read, Seek};
///
/// use mcap::sans_io::summary_reader::SummaryReadEvent;
/// use mcap::sans_io::indexed_reader::IndexedReadEvent;
/// use mcap::McapResult;
///
/// fn read_sync() -> McapResult<()> {
///     let mut file = fs::File::open("in.mcap")?;
///     let summary = {
///         let mut reader = mcap::sans_io::summary_reader::SummaryReader::new();
///         while let Some(event) = reader.next_event() {
///             match event? {
///                 SummaryReadEvent::ReadRequest(need) => {
///                     let written = file.read(reader.insert(need))?;
///                     reader.notify_read(written);
///                 },
///                 SummaryReadEvent::SeekRequest(to) => {
///                     reader.notify_seeked(file.seek(to)?);
///                 }
///             }
///         }
///         reader.finish().unwrap()
///     };
///     let mut reader = mcap::sans_io::indexed_reader::IndexedReader::new(&summary).expect("could not construct reader");
///     while let Some(event) = reader.next_event() {
///         match event? {
///             IndexedReadEvent::ReadRequest(need) => {
///                 let written = file.read(reader.insert(need))?;
///                 reader.notify_read(written);
///             },
///             IndexedReadEvent::SeekRequest(to) => {
///                 reader.notify_seeked(file.seek(to)?);
///             },
///             IndexedReadEvent::Message{ header, data } => {
///                 let channel = summary.channels.get(&header.channel_id).unwrap();
///                 // do something with the message header and data
///             }
///         }
///     }
///     Ok(())
/// }
/// ```
pub struct IndexedReader {
    // This MCAP's chunk indexes, pre-filtered by time range and topic and sorted in the order
    // they should be visited.
    chunk_indexes: Vec<ChunkIndex>,
    // The index in `chunk_indexes` of the current chunk to be loaded. cur_chunk_index >=
    // chunk_indexes.len() means that all chunks have been loaded.
    cur_chunk_index: usize,
    // A set of decompressed chunks. Slots are re-used when their message count reaches zero.
    chunk_slots: Vec<ChunkSlot>,
    // An index into the messages stored in chunk slots. Index entries are sorted in the order
    // they should be yielded.
    message_indexes: Vec<MessageIndex>,
    // The index in `message_indexes` of the next message to yield. cur_message_index >=
    // message_indexes.len() means that no more indexed messages are available, and more messages
    // should be loaded from the next chunk.
    cur_message_index: usize,
    // A buffer to store compressed chunk data while loading a chunk from the underlying reader.
    cur_compressed_chunk: Vec<u8>,
    // The count of valid bytes in `cur_compressed_chunk`. This may be less than
    // `cur_compressed_chunk.len()`, if the user has called insert(n) to read more data in but the
    // read operation resulted in fewer successfully read bytes.
    cur_compressed_chunk_loaded_bytes: usize,
    // The current known position of the reader in the underlying file.
    pos: u64,
    // describes what the indexed reader is currently trying to do.
    state: State,
    // What order messages should be yielded
    order: ReadOrder,
    // Criteria for what messages from the MCAP should be yielded
    filter: Filter,
    at_eof: bool,
}
struct Filter {
    // inclusive log time range start
    start: Option<u64>,
    // exclusive log time range end
    end: Option<u64>,
    // If non-empty, only channels with these IDs will be yielded
    channel_ids: BTreeSet<u16>,
}

#[derive(Debug, Default, Clone, Copy)]
pub enum ReadOrder {
    /// Yield messages in order of message.log_time. For messages with equal log times, the message
    /// earlier in the underlying file will be yielded first.
    #[default]
    LogTime,
    /// Yield messages in reverse message.log_time order. For messages with equal log times, the
    /// message later in the underlying file will be yielded first.
    ReverseLogTime,
    /// Yield messages in the order they are present in the file.
    File,
}

#[derive(Default, Clone)]
pub struct IndexedReaderOptions {
    pub start: Option<u64>,
    pub end: Option<u64>,
    pub order: ReadOrder,
    pub include_topics: Option<BTreeSet<String>>,
}

impl IndexedReaderOptions {
    pub fn new() -> Self {
        Self::default()
    }

    /// Configure the reader to yield messages in the specified order (defaults to log-time order).
    pub fn with_order(mut self, order: ReadOrder) -> Self {
        self.order = order;
        self
    }

    /// Configure the reader to yield only messages from topics matching this set of strings.
    /// By default, all topics will be yielded.
    pub fn include_topics<T: IntoIterator<Item = impl Deref<Target = str>>>(
        mut self,
        topics: T,
    ) -> Self {
        self.include_topics = Some(topics.into_iter().map(|p| p.to_owned()).collect());
        self
    }

    /// Configure the reader to yield only messages with log time on or after this time.
    pub fn log_time_on_or_after(mut self, start: u64) -> Self {
        self.start = Some(start);
        self
    }

    /// Configure the reader to yield only messages with log time before this time.
    pub fn log_time_before(mut self, end: u64) -> Self {
        self.end = Some(end);
        self
    }
}

impl IndexedReader {
    pub fn new(summary: &crate::Summary) -> McapResult<Self> {
        Self::new_with_options(summary, IndexedReaderOptions::default())
    }

    pub fn new_with_options(
        summary: &crate::Summary,
        options: IndexedReaderOptions,
    ) -> McapResult<Self> {
        let channel_ids = if let Some(include_topics) = options.include_topics {
            let mut set = BTreeSet::new();
            for (id, channel) in summary.channels.iter() {
                if include_topics.contains(&channel.topic) {
                    set.insert(*id);
                }
            }
            set
        } else {
            BTreeSet::new()
        };

        // filter out chunks that we won't use
        let mut chunk_indexes: Vec<crate::records::ChunkIndex> = summary
            .chunk_indexes
            .clone()
            .into_iter()
            .filter(|chunk_index| {
                if let Some(start) = options.start {
                    if chunk_index.message_end_time < start {
                        return false;
                    }
                }
                if let Some(end) = options.end {
                    if chunk_index.message_start_time >= end {
                        return false;
                    }
                }
                if channel_ids.is_empty() {
                    return true;
                }
                // NOTE: if there are no message indexes, we can't reject this chunk because
                // the file may not have message indexes included.
                if chunk_index.message_index_offsets.is_empty() {
                    return true;
                }
                for key in chunk_index.message_index_offsets.keys() {
                    if channel_ids.contains(key) {
                        return true;
                    }
                }
                false
            })
            .collect();

        // put the chunk indexes in the order that we want to read them
        match options.order {
            ReadOrder::File => {
                chunk_indexes.sort_by(|a, b| a.chunk_start_offset.cmp(&b.chunk_start_offset))
            }
            ReadOrder::LogTime => {
                chunk_indexes.sort_by(|a, b| {
                    match a.message_start_time.cmp(&b.message_start_time) {
                        std::cmp::Ordering::Equal => {
                            a.chunk_start_offset.cmp(&b.chunk_start_offset)
                        }
                        other => other,
                    }
                });
            }
            ReadOrder::ReverseLogTime => {
                chunk_indexes.sort_by(|a, b| match b.message_end_time.cmp(&a.message_end_time) {
                    std::cmp::Ordering::Equal => b.chunk_start_offset.cmp(&a.chunk_start_offset),
                    other => other,
                });
            }
        };

        // check through all chunk indexes once to ensure that we have address space for an
        // uncompressed chunk.
        for chunk_index in chunk_indexes.iter() {
            if chunk_index.compressed_size > usize::MAX as u64 {
                return Err(McapError::TooLong(chunk_index.compressed_size));
            }
            if chunk_index.uncompressed_size > usize::MAX as u64 {
                return Err(McapError::TooLong(chunk_index.uncompressed_size));
            }
        }
        // need to deep-clone channels and schemas here.
        Ok(Self {
            state: State::SeekingToChunk,
            chunk_indexes,
            chunk_slots: Vec::new(),
            message_indexes: Vec::new(),
            cur_compressed_chunk: Vec::new(),
            cur_compressed_chunk_loaded_bytes: 0,
            cur_message_index: 0,
            cur_chunk_index: 0,
            pos: 0,
            order: options.order,
            filter: Filter {
                start: options.start,
                end: options.end,
                channel_ids,
            },
            at_eof: false,
        })
    }

    /// Returns the next event from the reader. Call this repeatedly and act on the resulting
    /// events in order to read messages from the MCAP.
    pub fn next_event(&mut self) -> Option<McapResult<IndexedReadEvent>> {
        self.next_event_inner().transpose()
    }

    fn next_event_inner(&mut self) -> McapResult<Option<IndexedReadEvent>> {
        loop {
            match &mut self.state {
                State::SeekingToChunk => {
                    // If there is no chunk to seek to, we're done.
                    if self.cur_chunk_index >= self.chunk_indexes.len() {
                        self.state = State::Done;
                        return Ok(None);
                    }
                    let cur_chunk = &self.chunk_indexes[self.cur_chunk_index];
                    // If we're not already at the start of compressed data, seek to it.
                    let compressed_data_start = get_compressed_data_start(cur_chunk);
                    if self.pos != compressed_data_start {
                        return Ok(Some(IndexedReadEvent::SeekRequest(SeekFrom::Start(
                            compressed_data_start,
                        ))));
                    }
                    // Seek is done, time to load the compressed chunk data.
                    self.cur_compressed_chunk.clear();
                    self.cur_compressed_chunk_loaded_bytes = 0;
                    self.state = State::LoadingChunkData {
                        into_slot: find_or_make_chunk_slot(
                            &mut self.chunk_slots,
                            cur_chunk.uncompressed_size as usize, // size checked in new()
                        ),
                    };
                    continue;
                }
                State::LoadingChunkData { into_slot } => {
                    // This is a defensive check - the reader should not enter this state if there
                    // is no valid chunk to load.
                    if self.cur_chunk_index >= self.chunk_indexes.len() {
                        self.state = State::Done;
                        return Ok(None);
                    }
                    let cur_chunk = &self.chunk_indexes[self.cur_chunk_index];
                    let compressed_size = cur_chunk.compressed_size as usize; // size checked in new()
                    let uncompressed_size = cur_chunk.uncompressed_size as usize; // size checked in new()

                    // Keep requesting more data until we have all of the compressed chunk.
                    if self.cur_compressed_chunk_loaded_bytes < compressed_size {
                        let need = compressed_size - self.cur_compressed_chunk_loaded_bytes;
                        if self.at_eof {
                            return Err(McapError::UnexpectedEof);
                        }
                        return Ok(Some(IndexedReadEvent::ReadRequest(need)));
                    }
                    // decompress the chunk into the current slot. For un-compressed chunks, we do
                    // nothing, because we already loaded the "compressed" data into the chunk slot.
                    let slot = &mut self.chunk_slots[*into_slot];
                    //
                    slot.buf.resize(uncompressed_size, 0);
                    match cur_chunk.compression.as_str() {
                        "" => {
                            // data is already loaded into current slot
                        }
                        #[cfg(feature = "zstd")]
                        "zstd" => {
                            // decompress zstd into current slot
                            let n = zstd::zstd_safe::decompress(
                                &mut slot.buf[..],
                                &self.cur_compressed_chunk[..compressed_size],
                            )
                            .map_err(|err| {
                                McapError::DecompressionError(
                                    zstd::zstd_safe::get_error_name(err).into(),
                                )
                            })?;
                            if n != uncompressed_size {
                                return Err(McapError::DecompressionError(
                                    format!("zstd decompression error: expected {uncompressed_size}, got {n}"),
                                ));
                            }
                        }
                        #[cfg(feature = "lz4")]
                        "lz4" => {
                            use std::io::Read;
                            let mut decoder = lz4::Decoder::new(std::io::Cursor::new(
                                &self.cur_compressed_chunk[..compressed_size],
                            ))?;
                            decoder.read_exact(&mut slot.buf[..])?;
                        }
                        other => return Err(McapError::UnsupportedCompression(other.into())),
                    }
                    // index the current chunk slot
                    // before starting, check if all existing message indexes have been exhausted -
                    // if so, clear them out now.
                    if self.cur_message_index >= self.message_indexes.len() {
                        self.cur_message_index = 0;
                        self.message_indexes.clear();
                    }
                    // load new indexes into `self.message_indexes`
                    let message_count = index_messages(
                        *into_slot,
                        &slot.buf,
                        self.order,
                        &self.filter,
                        &mut self.message_indexes,
                        self.cur_message_index,
                    )?;
                    slot.message_count = message_count;
                    // If there is more dead space at the front of `self.message_indexes` than the
                    // set of new message indexes, compact the message index array now.
                    if message_count < (self.cur_message_index) {
                        self.message_indexes.drain(0..self.cur_message_index);
                        self.cur_message_index = 0;
                    }
                    // this chunk is finished, move on
                    self.cur_chunk_index += 1;
                    self.state = State::YieldingMessages;
                    continue;
                }
                State::YieldingMessages => {
                    // if we have run out of messages to yield, load the next chunk
                    if self.cur_message_index >= self.message_indexes.len() {
                        self.state = State::SeekingToChunk;
                        continue;
                    }
                    // if the next chunk contains messages that should be yielded before the next indexed message,
                    // load the next chunk
                    let message_index = self.message_indexes[self.cur_message_index];
                    if self.cur_chunk_index < self.chunk_indexes.len() {
                        let should_load_chunk = match self.order {
                            ReadOrder::File => false,
                            ReadOrder::LogTime => {
                                self.chunk_indexes[self.cur_chunk_index].message_start_time
                                    < message_index.log_time
                            }
                            ReadOrder::ReverseLogTime => {
                                self.chunk_indexes[self.cur_chunk_index].message_end_time
                                    > message_index.log_time
                            }
                        };
                        if should_load_chunk {
                            self.state = State::SeekingToChunk;
                            continue;
                        }
                    }
                    self.cur_message_index += 1;
                    self.chunk_slots[message_index.chunk_slot_idx].message_count -= 1;
                    let record =
                        &self.chunk_slots[message_index.chunk_slot_idx].buf[message_index.offset..];
                    // This should not happen - we failed in the indexing process somehow.
                    if record[0] != op::MESSAGE {
                        panic!("invariant: message indexes should point to message records");
                    }
                    let len = u64::from_le_bytes(record[1..9].try_into().unwrap()) as usize; // size checked when indexing
                    let mut cursor = std::io::Cursor::new(&record[9..9 + len]);
                    let header = MessageHeader::read_le(&mut cursor)?;
                    let header_end = cursor.position() as usize; // we can assume position <= record.len() <= usize::MAX here
                    let msg_buf = cursor.into_inner();
                    let data = &msg_buf[header_end..];
                    return Ok(Some(IndexedReadEvent::Message { header, data }));
                }
                State::Done => {
                    return Ok(None);
                }
            }
        }
    }

    /// Inform the reader of the result of the latest read on the underlying stream. 0 implies
    /// that the end of stream has been reached.
    ///
    /// Panics if `n` is greater than the last `n` provided to [`Self::insert`].
    pub fn notify_read(&mut self, n: usize) {
        self.at_eof = n == 0;
        if let State::LoadingChunkData { into_slot } = &self.state {
            let buffer_length = if self.chunk_indexes[self.cur_chunk_index]
                .compression
                .is_empty()
            {
                self.chunk_slots[*into_slot].buf.len()
            } else {
                self.cur_compressed_chunk.len()
            };
            if buffer_length < self.cur_compressed_chunk_loaded_bytes + n {
                panic!("notify_read called with n > last inserted length");
            }
            self.cur_compressed_chunk_loaded_bytes += n;
        }
        self.pos += n as u64;
    }

    /// Inform the reader of the result of the latest seek of the underlying stream.
    pub fn notify_seeked(&mut self, pos: u64) {
        if self.at_eof && self.pos != pos {
            self.at_eof = false;
        }
        // If we're currently loading data, we need to reset and start loading from the beginning.
        if self.pos != pos && matches!(self.state, State::LoadingChunkData { .. }) {
            let mut state = State::SeekingToChunk;
            std::mem::swap(&mut state, &mut self.state);
            let State::LoadingChunkData { .. } = state else {
                unreachable!();
            };
        }
        self.pos = pos;
    }

    /// Get a mutable buffer of size `n` to read new MCAP data into from the stream.
    pub fn insert(&mut self, n: usize) -> &mut [u8] {
        let buf = match self.state {
            State::LoadingChunkData { into_slot } => {
                if self.chunk_indexes[self.cur_chunk_index]
                    .compression
                    .is_empty()
                {
                    &mut self.chunk_slots[into_slot].buf
                } else {
                    &mut self.cur_compressed_chunk
                }
            }
            _ => &mut self.cur_compressed_chunk,
        };
        let start = self.cur_compressed_chunk_loaded_bytes;
        let end = start + n;
        buf.resize(end, 0);
        &mut buf[start..end]
    }
}

/// Insert indexes into `message_indexes` for every message in this chunk that matches the filter
/// criteria.
fn index_messages(
    chunk_slot_idx: usize,
    chunk_data: &[u8],
    order: ReadOrder,
    filter: &Filter,
    message_indexes: &mut Vec<MessageIndex>,
    cur_message_index: usize,
) -> McapResult<usize> {
    let mut offset = 0usize;
    let mut sorting_required = cur_message_index != 0;
    let mut latest_timestamp = 0;
    let new_message_index_start = message_indexes.len();
    while offset < chunk_data.len() {
        let record = &chunk_data[offset..];
        let opcode = record[0];
        if record.len() < 9 {
            return Err(McapError::UnexpectedEoc);
        }
        let len = len_as_usize(u64::from_le_bytes(record[1..9].try_into().unwrap()))?;
        let next_offset = offset + 9 + len;
        if opcode != op::MESSAGE {
            offset = next_offset;
            continue;
        }
        let msg = MessageHeader::read_le(&mut std::io::Cursor::new(&record[9..9 + len]))?;
        if let Some(end) = filter.end {
            if msg.log_time >= end {
                offset = next_offset;
                continue;
            }
        }
        if let Some(start) = filter.start {
            if msg.log_time < start {
                offset = next_offset;
                continue;
            }
        }
        if !filter.channel_ids.is_empty() && !filter.channel_ids.contains(&msg.channel_id) {
            offset = next_offset;
            continue;
        }
        if !sorting_required {
            sorting_required = msg.log_time < latest_timestamp
        }
        latest_timestamp = std::cmp::max(latest_timestamp, msg.log_time);
        message_indexes.push(MessageIndex {
            chunk_slot_idx,
            log_time: msg.log_time,
            offset,
        });
        offset = next_offset
    }
    match order {
        ReadOrder::File => {
            // in file order, message indexes do not need sorting
        }
        ReadOrder::LogTime => {
            if sorting_required {
                let unread_message_indexes = &mut message_indexes[cur_message_index..];
                unread_message_indexes.sort_by(|a, b| a.log_time.cmp(&b.log_time));
            }
        }
        ReadOrder::ReverseLogTime => {
            // first, reverse the order of the new message indexes. This will make the sort much faster.
            let new_message_indexes = &mut message_indexes[new_message_index_start..];
            new_message_indexes.reverse();
            if sorting_required {
                let unread_message_indexes = &mut message_indexes[cur_message_index..];
                unread_message_indexes.sort_by(|a, b| b.log_time.cmp(&a.log_time));
            }
        }
    }
    Ok(message_indexes.len() - new_message_index_start)
}

/// Finds a chunk slot with no outstanding messages in it and returns its index, or creates a new one.
fn find_or_make_chunk_slot(chunk_slots: &mut Vec<ChunkSlot>, uncompressed_size: usize) -> usize {
    for (i, slot) in chunk_slots.iter_mut().enumerate() {
        if slot.message_count == 0 {
            slot.buf.clear();
            slot.buf.reserve(uncompressed_size);
            return i;
        }
    }
    let idx = chunk_slots.len();
    chunk_slots.push(ChunkSlot {
        message_count: 0,
        buf: Vec::with_capacity(uncompressed_size),
    });
    idx
}

fn get_compressed_data_start(chunk_index: &ChunkIndex) -> u64 {
    chunk_index.chunk_start_offset
    + 1 // opcode
    + 8 // chunk record length
    + 8 // start time
    + 8 // end time
    + 8 // uncompressed size
    + 4 // CRC
    + 4 // compression string length
    + (chunk_index.compression.len() as u64) // compression string
    + 8 // compressed size
}

fn len_as_usize(len: u64) -> McapResult<usize> {
    len.try_into().map_err(|_| McapError::TooLong(len))
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        io::{Read, Seek},
    };

    use crate::sans_io::{SummaryReadEvent, SummaryReader};

    use super::*;

    fn make_mcap(compression: Option<crate::Compression>, chunks: &[&[(u16, u64)]]) -> Vec<u8> {
        let mut writer = crate::WriteOptions::new()
            .compression(compression)
            .chunk_size(None)
            .create(std::io::Cursor::new(Vec::new()))
            .expect("could not make the writer");
        let mut sequence = 0;
        for chunk in chunks.iter() {
            for &(id, log_time) in chunk.iter() {
                writer
                    .write(&crate::Message {
                        channel: std::sync::Arc::new(crate::Channel {
                            id,
                            topic: if id % 2 == 0 {
                                "even".into()
                            } else {
                                "odd".into()
                            },
                            schema: None,
                            message_encoding: "ros1msg".into(),
                            metadata: BTreeMap::new(),
                        }),
                        sequence,
                        log_time,
                        publish_time: log_time,
                        data: std::borrow::Cow::Owned(vec![1, 2, 3]),
                    })
                    .expect("failed write");
                sequence += 1;
            }
            writer.flush().expect("failed to flush chunk");
        }
        writer.finish().expect("failed on finish");
        writer.into_inner().into_inner()
    }

    fn read_mcap(options: IndexedReaderOptions, mcap: &[u8]) -> Vec<(u16, u64)> {
        let summary = crate::Summary::read(mcap)
            .expect("summary reading should succeed")
            .expect("there should be a summary");
        let mut reader = IndexedReader::new_with_options(&summary, options)
            .expect("reader construction should not fail");
        let mut cursor = std::io::Cursor::new(&mcap);
        let mut found = Vec::new();
        let mut iterations = 0;
        while let Some(event) = reader.next_event() {
            match event.expect("indexed reader failed") {
                IndexedReadEvent::ReadRequest(n) => {
                    let res = cursor
                        .read(reader.insert(n))
                        .expect("read should not fail on cursor");
                    reader.notify_read(res);
                }
                IndexedReadEvent::SeekRequest(to) => {
                    let pos = cursor.seek(to).expect("seek should not fail on cursor");
                    reader.notify_seeked(pos);
                }
                IndexedReadEvent::Message { header, .. } => {
                    found.push((header.channel_id, header.log_time));
                }
            }
            iterations += 1;
            if iterations > 100000 {
                panic!("too many iterations");
            }
        }
        found
    }

    fn test_read_order(chunks: &[&[(u16, u64)]]) {
        let mcap = make_mcap(None, chunks);
        for order in [
            ReadOrder::LogTime,
            ReadOrder::ReverseLogTime,
            ReadOrder::File,
        ] {
            let mut expected: Vec<(u16, u64)> = chunks.iter().cloned().flatten().cloned().collect();
            match order {
                ReadOrder::File => {}
                // sort in log time order (stable, so that file order is preserved) for equal values
                ReadOrder::LogTime => expected.sort_by(|a, b| a.1.cmp(&b.1)),
                // sort in log time order then reverse
                ReadOrder::ReverseLogTime => {
                    expected.sort_by(|a, b| a.1.cmp(&b.1));
                    expected.reverse();
                }
            }
            let found = read_mcap(IndexedReaderOptions::new().with_order(order), &mcap);
            assert_eq!(&found, &expected, "order: {order:?}");
        }
    }
    #[test]
    fn test_simple_order() {
        test_read_order(&[
            &[(0, 1), (0, 2), (0, 3)],
            &[(0, 4), (0, 5), (0, 6)],
            &[(0, 7), (0, 8), (0, 9)],
        ]);
    }
    #[test]
    fn test_overlapping_chunks() {
        test_read_order(&[
            &[(0, 2), (0, 4), (0, 6)],
            &[(1, 1), (1, 3), (1, 5)],
            &[(2, 5), (2, 7), (2, 9)],
        ]);
    }

    #[test]
    fn test_in_chunk_disorder() {
        test_read_order(&[
            &[(0, 4), (0, 2), (0, 6)],
            &[(1, 5), (1, 3), (1, 1)],
            &[(2, 9), (2, 8), (2, 7)],
        ]);
    }
    #[test]
    fn test_continuing_overlap() {
        test_read_order(&[
            &[(0, 1), (0, 10)],
            &[(1, 2), (1, 3)],
            &[(2, 4), (2, 5)],
            &[(3, 6), (3, 7)],
            &[(4, 8), (4, 9)],
        ]);
    }

    #[test]
    fn test_time_range_filter() {
        let mcap = make_mcap(None, &[&[(0, 1), (0, 2), (0, 3), (0, 4), (0, 5), (0, 6)]]);
        let messages = read_mcap(
            IndexedReaderOptions::new()
                .log_time_on_or_after(3)
                .log_time_before(6),
            &mcap,
        );
        assert_eq!(&messages, &[(0, 3), (0, 4), (0, 5)])
    }
    #[test]
    fn test_compression() {
        for compression in [
            None,
            Some(crate::Compression::Lz4),
            Some(crate::Compression::Zstd),
        ] {
            let mcap = make_mcap(compression, &[&[(0, 1), (0, 2)], &[(0, 3), (0, 4)]]);
            let messages = read_mcap(IndexedReaderOptions::new(), &mcap);
            assert_eq!(
                &messages,
                &[(0, 1), (0, 2), (0, 3), (0, 4)],
                "decompression with {compression:?}"
            )
        }
    }

    #[test]
    fn test_channel_filter() {
        let mcap = make_mcap(None, &[&[(0, 1), (1, 2), (2, 3), (1, 4), (0, 5), (1, 6)]]);
        let messages = read_mcap(IndexedReaderOptions::new().include_topics(["even"]), &mcap);
        assert_eq!(&messages, &[(0, 1), (2, 3), (0, 5)])
    }

    #[test]
    fn test_against_fixtures() {
        let path = "tests/data/compressed.mcap";
        let count = 826;
        let block_sizes = [None, Some(16 * 1024), Some(1024), Some(128)];
        for &block_size in block_sizes.iter() {
            let mut file = std::fs::File::open(path).expect("could not open file");
            let summary = {
                let mut reader = SummaryReader::new();
                while let Some(event) = reader.next_event() {
                    match event.expect("failed to get next summary read event") {
                        SummaryReadEvent::SeekRequest(pos) => {
                            reader.notify_seeked(file.seek(pos).expect("seek failed"));
                        }
                        SummaryReadEvent::ReadRequest(n) => {
                            let n = match block_size {
                                Some(block_size) => block_size,
                                None => n,
                            };
                            let read = file.read(reader.insert(n)).expect("read failed");
                            reader.notify_read(read);
                        }
                    }
                }
                reader.finish().expect("file should contain a summary")
            };
            let mut reader = IndexedReader::new(&summary).expect("failed to construct summary");
            let mut messages = Vec::new();
            while let Some(event) = reader.next_event() {
                match event.expect("failed to read next event") {
                    IndexedReadEvent::SeekRequest(pos) => {
                        reader.notify_seeked(file.seek(pos).expect("seek failed"));
                    }
                    IndexedReadEvent::ReadRequest(n) => {
                        let n = match block_size {
                            Some(block_size) => block_size,
                            None => n,
                        };
                        let read = file.read(reader.insert(n)).expect("read failed");
                        reader.notify_read(read);
                    }
                    IndexedReadEvent::Message { header, .. } => {
                        messages.push(header.log_time);
                    }
                }
            }
            assert_eq!(
                messages.len(),
                count,
                "wrong message count for fixture {path}"
            );
            let mut last_log_time = 0u64;
            for &log_time in messages.iter() {
                assert!(log_time >= last_log_time, "out-of-order for fixture {path}");
                last_log_time = log_time;
            }
        }
    }
}
