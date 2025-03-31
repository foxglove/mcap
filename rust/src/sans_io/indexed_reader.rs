use binrw::BinRead;

use crate::{
    records::{op, ChunkIndex, MessageHeader},
    McapError, McapResult,
};
use std::{collections::BTreeSet, ops::Deref};

#[derive(Clone, Copy)]
struct MessageIndex {
    chunk_slot_idx: usize,
    log_time: u64,
    offset: usize,
}

/// Events yielded by the IndexedReader.
pub enum IndexedReadEvent<'a> {
    /// The reader needs the content of a chunk record to continue yielding messages.
    /// Read a slice out of the underlying file with the given offset and length into a buffer,
    /// and call [`IndexedReader::insert_chunk_record_data`] with the result to get more messages.
    ReadChunkRequest { offset: u64, length: usize },
    Message {
        header: crate::records::MessageHeader,
        data: &'a [u8],
    },
}

struct ChunkSlot {
    buf: Vec<u8>,
    data_start: u64,
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
///             IndexedReadEvent::ReadChunkRequest{start, length} => {
///                 file.seek(start)?;
///                 let mut buffer = reader.reclaim_buffer();
///                 buffer.resize(length, 0);
///                 file.read_exact(&mut buffer)?;
///                 reader.insert_chunk_record_data(start, buffer);
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
    // A set of decompressed chunks. Slots are re-used when their message count reaches zero.  There
    // may be more than one chunk slot in use at a time if we are reading in log-time or
    // reverse-log-time order, and there are chunks that overlap in time range.
    chunk_slots: Vec<ChunkSlot>,
    // An index into the messages stored in chunk slots. Index entries are sorted in the order
    // they should be yielded.
    message_indexes: Vec<MessageIndex>,
    // The index in `message_indexes` of the next message to yield. cur_message_index >=
    // message_indexes.len() means that no more indexed messages are available, and more messages
    // should be loaded from the next chunk.
    cur_message_index: usize,
    // What order messages should be yielded
    order: ReadOrder,
    // Criteria for what messages from the MCAP should be yielded
    filter: Filter,
}

fn chunk_request(index: &ChunkIndex) -> Option<McapResult<IndexedReadEvent<'static>>> {
    let length = match len_as_usize(index.compressed_size) {
        Ok(len) => len,
        Err(err) => return Some(Err(err)),
    };
    Some(Ok(IndexedReadEvent::ReadChunkRequest {
        offset: get_compressed_data_start(index),
        length,
    }))
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
                chunk_indexes.sort_by_key(|chunk_index| chunk_index.chunk_start_offset);
            }
            ReadOrder::LogTime => {
                chunk_indexes.sort_by_key(|chunk_index| chunk_index.message_start_time);
            }
            ReadOrder::ReverseLogTime => {
                // load chunks in reverse order of their _end_ time.
                chunk_indexes.sort_by_key(|chunk_index| chunk_index.message_end_time);
                chunk_indexes.reverse();
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
            chunk_indexes,
            chunk_slots: Vec::new(),
            message_indexes: Vec::new(),
            cur_message_index: 0,
            cur_chunk_index: 0,
            order: options.order,
            filter: Filter {
                start: options.start,
                end: options.end,
                channel_ids,
            },
        })
    }

    /// Returns the next event from the reader. Call this repeatedly and act on the resulting
    /// events in order to read messages from the MCAP.
    pub fn next_event(&self) -> Option<McapResult<IndexedReadEvent>> {
        if self.cur_message_index < self.message_indexes.len() {
            let message_index = &self.message_indexes[self.cur_message_index];
            // There is a message index at the top of the queue - check if we need to yield it
            // or load another chunk first.
            if self.cur_chunk_index < self.chunk_indexes.len() {
                let chunk_index = &self.chunk_indexes[self.cur_chunk_index];
                if self.yield_chunk_first(chunk_index, message_index) {
                    return chunk_request(chunk_index);
                }
            }
            // Time to yield the message.
            let buf = &self.chunk_slots[message_index.chunk_slot_idx].buf[message_index.offset..];
            assert_eq!(
                buf[0],
                op::MESSAGE,
                "invariant: message indexes should point to message records"
            );
            let msg_len = match len_as_usize(u64::from_le_bytes(buf[1..9].try_into().unwrap())) {
                Ok(len) => len,
                Err(err) => return Some(Err(err)),
            };
            let msg_data = &buf[9..9 + msg_len];
            let mut reader = std::io::Cursor::new(msg_data);
            let header = match MessageHeader::read_le(&mut reader) {
                Ok(header) => header,
                Err(err) => return Some(Err(err.into())),
            };
            let data_start_offset = reader.position() as usize;
            let data = &msg_data[data_start_offset..];
            return Some(Ok(IndexedReadEvent::Message { header, data }));
        }
        // we're out of message indexes, we need to load a chunk.
        // if we're out of chunks, we're done.
        if self.cur_chunk_index >= self.chunk_indexes.len() {
            return None;
        }
        return chunk_request(&self.chunk_indexes[self.cur_chunk_index]);
    }

    /// Call to insert new compressed records into this reader. The return value indicates whether
    /// the reader now has enough data to yield messages.
    pub fn insert_chunk_record_data(
        &mut self,
        start_offset: u64,
        compressed_data: &[u8],
    ) -> McapResult<()> {
        // linear search through our chunk indexes to figure out which one it is. In the common case,
        // the first chunk index will be right.
        let Some((i, chunk_index)) = self
            .chunk_indexes
            .iter()
            .enumerate()
            .find(|(_, chunk_index)| get_compressed_data_start(chunk_index) == start_offset)
        else {
            return Err(McapError::UnexpectedChunkDataInserted);
        };
        if compressed_data.len() != chunk_index.compressed_size as usize {
            return Err(McapError::UnexpectedChunkDataInserted);
        }
        let uncompressed_size = chunk_index.uncompressed_size as usize;
        let slot_idx =
            find_or_make_chunk_slot(&mut self.chunk_slots, start_offset, uncompressed_size);

        let slot = &mut self.chunk_slots[slot_idx];
        //
        slot.buf.resize(uncompressed_size, 0);
        match chunk_index.compression.as_str() {
            "" => {
                slot.buf[..].copy_from_slice(compressed_data);
            }
            #[cfg(feature = "zstd")]
            "zstd" => {
                // decompress zstd into current slot
                let n = zstd::zstd_safe::decompress(&mut slot.buf[..], compressed_data).map_err(
                    |err| {
                        McapError::DecompressionError(zstd::zstd_safe::get_error_name(err).into())
                    },
                )?;
                if n != uncompressed_size {
                    return Err(McapError::DecompressionError(format!(
                        "zstd decompression error: expected {uncompressed_size}, got {n}"
                    )));
                }
            }
            #[cfg(feature = "lz4")]
            "lz4" => {
                use std::io::Read;
                let mut decoder = lz4::Decoder::new(std::io::Cursor::new(compressed_data))?;
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
            slot_idx,
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
        // Now we need to remove the corresponding chunk index. In the common case, where
        // the caller has inserted the next-needed chunk, we can just increment an index. In other
        // cases, we remove the chunk index, which is O(n).
        if i == 0 {
            self.cur_chunk_index += 1;
        } else {
            self.chunk_indexes.remove(i);
        }
        Ok(())
    }

    /// Call to indicate to the reader that it should move on to the next message.
    pub fn consume_message(&mut self) {
        self.cur_message_index += 1;
    }

    fn yield_chunk_first(&self, chunk_index: &ChunkIndex, message_index: &MessageIndex) -> bool {
        match self.order {
            ReadOrder::File => {
                let chunk_slot = &self.chunk_slots[message_index.chunk_slot_idx];
                get_compressed_data_start(chunk_index) < chunk_slot.data_start
            }
            ReadOrder::LogTime => chunk_index.message_start_time < message_index.log_time,
            ReadOrder::ReverseLogTime => chunk_index.message_end_time > message_index.log_time,
        }
    }
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
    // sorting_required tracks whether the set of message indexes will need to be sorted after loading them.
    // If there are any unread indexes in `message_indexes` before we begin loading
    // the new chunk, they will need to be sorted with the new messages from the new chunk.
    // If not, and we also don't detect any out-of-order messages within the chunk, we skip sorting.
    let mut sorting_required = message_indexes.len() > 0;
    let mut latest_timestamp = 0;
    let new_message_index_start = message_indexes.len();
    while offset < chunk_data.len() {
        let record = &chunk_data[offset..];
        // 1 byte opcode + 8 byte length == 9
        if record.len() < 9 {
            return Err(McapError::UnexpectedEoc);
        }
        let opcode = record[0];
        let len = len_as_usize(u64::from_le_bytes(record[1..9].try_into().unwrap()))?;
        if record.len() < (9 + len) {
            return Err(McapError::UnexpectedEoc);
        }
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
        latest_timestamp = latest_timestamp.max(msg.log_time);
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
fn find_or_make_chunk_slot(
    chunk_slots: &mut Vec<ChunkSlot>,
    data_start: u64,
    uncompressed_size: usize,
) -> usize {
    for (i, slot) in chunk_slots.iter_mut().enumerate() {
        if slot.message_count == 0 {
            slot.buf.clear();
            slot.data_start = data_start;
            slot.buf.reserve(uncompressed_size);
            return i;
        }
    }
    let idx = chunk_slots.len();
    chunk_slots.push(ChunkSlot {
        message_count: 0,
        data_start: 0,
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
        let mut buf = Vec::new();
        while let Some(event) = reader.next_event() {
            match event.expect("indexed reader failed") {
                IndexedReadEvent::ReadChunkRequest { offset, length } => {
                    cursor
                        .seek(std::io::SeekFrom::Start(offset))
                        .expect("should not fail to seek");
                    buf.resize(length, 0);
                    cursor
                        .read_exact(&mut buf)
                        .expect("should not fail on read");
                    reader
                        .insert_chunk_record_data(offset, &buf)
                        .expect("failed to insert");
                }
                IndexedReadEvent::Message { header, .. } => {
                    found.push((header.channel_id, header.log_time));
                    reader.consume_message();
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
        let mut buffer = Vec::new();
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
                    IndexedReadEvent::ReadChunkRequest { offset, length } => {
                        file.seek(std::io::SeekFrom::Start(offset))
                            .expect("failed seek");
                        buffer.resize(length, 0);
                        file.read_exact(&mut buffer).expect("failed read");
                        reader
                            .insert_chunk_record_data(offset, &buffer)
                            .expect("failed on insert");
                    }
                    IndexedReadEvent::Message { header, .. } => {
                        messages.push(header.log_time);
                        reader.consume_message();
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
