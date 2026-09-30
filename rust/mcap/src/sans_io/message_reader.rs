//! A [sans-io](https://sans-io.readthedocs.io/) reader that yields linked [`Message`]s.
//!
//! [`MessageReader`] composes [`LinearReader`] with the bookkeeping [`crate::MessageStream`]
//! uses, so it applies the same validation while streaming from any source of bytes. Use
//! [`crate::io::MessageReader`] unless you are driving the reads yourself.
use std::{borrow::Cow, sync::Arc};

use crate::{
    read::ChannelAccumulator,
    records::{op, Record},
    sans_io::{LinearReadEvent, LinearReader, LinearReaderOptions},
    Channel, McapError, McapResult, Message,
};

/// What the caller should do next to progress through the file.
#[derive(Debug)]
pub enum MessageReadEvent {
    /// More data is needed: call [`MessageReader::insert`] then [`MessageReader::notify_read`].
    /// The value is a size hint.
    ReadRequest(usize),
    /// The next message in file order, linked to its channel and schema.
    Message(Message<'static>),
}

/// Streams linked messages from any source of bytes.
///
/// It applies the same schema/channel validation as [`crate::MessageStream`] and yields nothing
/// after the first error. Like `MessageStream`, it reads to the end of the file without
/// re-checking the summary, so a truncated file or a bad end magic is reported after the last
/// message. With `skip_end_magic` it ends at the data end record. Messages own their data, so
/// they outlive the reader's buffer.
pub struct MessageReader {
    reader: LinearReader,
    channeler: ChannelAccumulator<'static>,
    done: bool,
    skip_end_magic: bool,
    past_data_end: bool,
}

impl Default for MessageReader {
    fn default() -> Self {
        Self::new()
    }
}

impl MessageReader {
    /// Creates a reader with [`LinearReaderOptions::default`].
    pub fn new() -> Self {
        Self::new_with_options(LinearReaderOptions::default())
    }

    /// Creates a reader with the given options. `emit_chunks` is always disabled, because the
    /// reader must see the records inside each chunk.
    pub fn new_with_options(options: LinearReaderOptions) -> Self {
        let skip_end_magic = options.skip_end_magic;
        Self {
            reader: LinearReader::new_with_options(options.with_emit_chunks(false)),
            channeler: ChannelAccumulator::default(),
            done: false,
            skip_end_magic,
            past_data_end: false,
        }
    }

    /// The next event, or `None` once the file is read or an error has been returned.
    pub fn next_event(&mut self) -> Option<McapResult<MessageReadEvent>> {
        if self.done {
            return None;
        }
        let event = self.next_event_inner();
        if !matches!(event, Some(Ok(_))) {
            self.done = true;
        }
        event
    }

    fn next_event_inner(&mut self) -> Option<McapResult<MessageReadEvent>> {
        loop {
            let event = match self.reader.next_event()? {
                Ok(event) => event,
                Err(err) => return Some(Err(err)),
            };
            match event {
                LinearReadEvent::ReadRequest(need) => {
                    return Some(Ok(MessageReadEvent::ReadRequest(need)));
                }
                // The summary repeats schemas and channels, so read on to the end magic without
                // re-checking it, or stop here if the magic is not to be checked. A chunk may not
                // hold a data end record, so one inside a chunk is skipped like any stray record.
                LinearReadEvent::Record {
                    opcode: op::DATA_END,
                    ..
                } => {
                    if self.reader.in_chunk() {
                        continue;
                    }
                    if self.skip_end_magic {
                        return None;
                    }
                    self.past_data_end = true;
                }
                LinearReadEvent::Record { opcode, data } => {
                    let record = match crate::parse_record(opcode, data) {
                        Ok(record) => record,
                        Err(err) => return Some(Err(err)),
                    };
                    // Summary records are parsed but not re-checked.
                    if self.past_data_end {
                        continue;
                    }
                    match record {
                        Record::Schema { header, data } => {
                            let data = Cow::Owned(data.into_owned());
                            if let Err(err) = self.channeler.add_schema(header, data) {
                                return Some(Err(err));
                            }
                        }
                        Record::Channel(channel) => {
                            if let Err(err) = self.channeler.add_channel(channel) {
                                return Some(Err(err));
                            }
                        }
                        Record::Message { header, data } => {
                            let Some(channel) = self.channeler.get(header.channel_id) else {
                                return Some(Err(McapError::UnknownChannel(
                                    header.sequence,
                                    header.channel_id,
                                )));
                            };
                            return Some(Ok(MessageReadEvent::Message(Message {
                                channel,
                                sequence: header.sequence,
                                log_time: header.log_time,
                                publish_time: header.publish_time,
                                data: Cow::Owned(data.into_owned()),
                            })));
                        }
                        _ => {}
                    }
                }
            }
        }
    }

    /// Returns a buffer of `n` bytes to fill in response to a [`MessageReadEvent::ReadRequest`].
    pub fn insert(&mut self, n: usize) -> &mut [u8] {
        self.reader.insert(n)
    }

    /// Reports how many bytes were written into the buffer from [`Self::insert`]. Zero means EOF.
    pub fn notify_read(&mut self, n: usize) {
        self.reader.notify_read(n);
    }

    /// Gets a channel seen so far by ID.
    pub fn get_channel(&self, channel_id: u16) -> Option<Arc<Channel<'static>>> {
        self.channeler.get(channel_id)
    }
}

#[cfg(test)]
pub(crate) mod test_support {
    use std::{borrow::Cow, collections::BTreeMap, io::Cursor};

    use crate::{records::MessageHeader, WriteOptions};

    /// Two channels (the second schemaless) and four messages, chunked or not.
    pub(crate) fn two_channel_mcap(use_chunks: bool) -> Vec<u8> {
        let mut buffer = Vec::new();
        let mut writer = WriteOptions::new()
            .use_chunks(use_chunks)
            .chunk_size(Some(64))
            .compression(None)
            .create(Cursor::new(&mut buffer))
            .expect("writer");
        let schema_id = writer
            .add_schema("Example", "jsonschema", br#"{"type":"object"}"#)
            .expect("schema");
        let with_schema = writer
            .add_channel(schema_id, "/with_schema", "json", &BTreeMap::new())
            .expect("channel");
        let schemaless = writer
            .add_channel(0, "/schemaless", "raw", &BTreeMap::new())
            .expect("channel");
        for (sequence, channel_id) in [
            (1, with_schema),
            (2, schemaless),
            (3, with_schema),
            (4, schemaless),
        ] {
            writer
                .write_to_known_channel(
                    &MessageHeader {
                        channel_id,
                        sequence,
                        log_time: u64::from(sequence) * 10,
                        publish_time: u64::from(sequence) * 10,
                    },
                    &[sequence as u8; 32],
                )
                .expect("message");
        }
        writer.finish().expect("finish");
        drop(writer);
        buffer
    }

    /// Bytes before a record's body: opcode and length.
    const RECORD_PREFIX: usize = 1 + 8;
    /// Fixed-size fields of a message record: channel_id, sequence, log_time, publish_time.
    const MESSAGE_HEADER: usize = 2 + 4 + 8 + 8;

    /// Offsets of every top-level record with `opcode` (start of the opcode byte).
    pub(crate) fn record_offsets(mcap: &[u8], opcode: u8) -> Vec<usize> {
        let mut offsets = Vec::new();
        let mut at = crate::MAGIC.len();
        while at + RECORD_PREFIX <= mcap.len() {
            let len =
                u64::from_le_bytes(mcap[at + 1..at + RECORD_PREFIX].try_into().unwrap()) as usize;
            if mcap[at] == opcode {
                offsets.push(at);
            }
            at += RECORD_PREFIX + len;
        }
        offsets
    }

    /// The file cut off inside its last top-level message record.
    pub(crate) fn truncated_in_last_message(mcap: &[u8]) -> &[u8] {
        let last = *record_offsets(mcap, crate::records::op::MESSAGE)
            .last()
            .expect("a message record");
        // Keep the whole header and 5 payload bytes, so the cut lands inside the payload.
        &mcap[..last + RECORD_PREFIX + MESSAGE_HEADER + 5]
    }

    /// A chunk body's fixed fields: start/end time, uncompressed size, CRC.
    const CHUNK_FIXED: usize = 8 + 8 + 8 + 4;

    /// The file with a (never valid) data end record spliced into its first chunk, which must be
    /// uncompressed. The chunk's CRC is cleared so the splice does not fail it.
    pub(crate) fn with_data_end_in_first_chunk(mcap: &[u8]) -> Vec<u8> {
        let chunk = record_offsets(mcap, crate::records::op::CHUNK)[0];
        let body = chunk + RECORD_PREFIX;
        let compression_len_at = body + CHUNK_FIXED;
        let compression_len = u32::from_le_bytes(
            mcap[compression_len_at..compression_len_at + 4]
                .try_into()
                .unwrap(),
        ) as usize;
        assert_eq!(compression_len, 0, "the chunk must be uncompressed");
        let records_len_at = compression_len_at + 4;
        let records_at = records_len_at + 8;

        // Opcode, length, and a data section CRC of zero.
        let mut data_end = vec![crate::records::op::DATA_END];
        data_end.extend_from_slice(&4u64.to_le_bytes());
        data_end.extend_from_slice(&0u32.to_le_bytes());
        let added = data_end.len() as u64;

        let mut out = mcap.to_vec();
        let grow = |out: &mut [u8], at: usize| {
            let old = u64::from_le_bytes(out[at..at + 8].try_into().unwrap());
            out[at..at + 8].copy_from_slice(&(old + added).to_le_bytes());
        };
        grow(&mut out, chunk + 1); // record length
        grow(&mut out, body + 16); // uncompressed size
        grow(&mut out, records_len_at); // records length
        out[body + 24..body + 28].copy_from_slice(&0u32.to_le_bytes()); // crc
        out.splice(records_at..records_at, data_end);
        out
    }

    /// The payload the writer used for message `sequence`.
    pub(crate) fn payload(sequence: u32) -> Cow<'static, [u8]> {
        Cow::Owned(vec![sequence as u8; 32])
    }
}

#[cfg(test)]
mod tests {
    use super::test_support::two_channel_mcap;
    use super::*;

    /// Drives the sans-io reader over an in-memory slice, mimicking an I/O adapter.
    fn drain(reader: &mut MessageReader, mut bytes: &[u8]) -> Vec<McapResult<Message<'static>>> {
        let mut out = Vec::new();
        while let Some(event) = reader.next_event() {
            match event {
                Ok(MessageReadEvent::ReadRequest(need)) => {
                    let n = need.min(bytes.len());
                    reader.insert(n).copy_from_slice(&bytes[..n]);
                    reader.notify_read(n);
                    bytes = &bytes[n..];
                }
                Ok(MessageReadEvent::Message(message)) => out.push(Ok(message)),
                Err(err) => out.push(Err(err)),
            }
        }
        out
    }

    #[test]
    fn matches_message_stream() {
        for use_chunks in [false, true] {
            let mcap = two_channel_mcap(use_chunks);
            let expected = crate::MessageStream::new(&mcap)
                .expect("stream")
                .collect::<McapResult<Vec<_>>>()
                .expect("messages");
            let actual = drain(&mut MessageReader::new(), &mcap)
                .into_iter()
                .collect::<McapResult<Vec<_>>>()
                .expect("messages");
            assert_eq!(actual, expected, "use_chunks={use_chunks}");
            assert_eq!(actual.len(), 4);
            assert!(actual[1].channel.schema.is_none());
        }
    }

    #[test]
    fn yields_nothing_after_an_error() {
        let mcap = two_channel_mcap(false);
        let truncated = super::test_support::truncated_in_last_message(&mcap);
        let mut reader = MessageReader::new();
        let items = drain(&mut reader, truncated);
        assert_eq!(items.len(), 4, "three messages, then the error");
        assert!(matches!(items.last(), Some(Err(McapError::UnexpectedEof))));
        assert_eq!(items.iter().filter(|item| item.is_err()).count(), 1);
        assert!(reader.next_event().is_none(), "the reader stays finished");
    }

    #[test]
    fn a_data_end_record_inside_a_chunk_does_not_end_the_stream() {
        let mcap = two_channel_mcap(true);
        assert!(
            super::test_support::record_offsets(&mcap, crate::records::op::CHUNK).len() >= 2,
            "the fixture needs messages in a later chunk"
        );
        let mcap = super::test_support::with_data_end_in_first_chunk(&mcap);

        let items = drain(&mut MessageReader::new(), &mcap);
        assert!(items.iter().all(|item| item.is_ok()), "{items:?}");
        assert_eq!(items.len(), 4);

        let messages = crate::MessageStream::new(&mcap)
            .expect("stream")
            .collect::<McapResult<Vec<_>>>()
            .expect("messages");
        assert_eq!(messages.len(), 4);

        let raw = crate::read::RawMessageStream::new(&mcap)
            .expect("stream")
            .collect::<McapResult<Vec<_>>>()
            .expect("messages");
        assert_eq!(raw.len(), 4);
    }

    #[test]
    fn reads_through_the_summary_to_the_end_magic() {
        let mcap = two_channel_mcap(true);
        let summary_start = crate::read::footer(&mcap).expect("footer").summary_start as usize;
        assert!(summary_start > 0, "the fixture needs a summary section");
        // The summary opens with the schema: opcode, length, id (2), name length (4), name.
        assert_eq!(mcap[summary_start], crate::records::op::SCHEMA);
        let name_at = summary_start + 9 + 2 + 4;

        // A summary that disagrees with the data (a renamed schema) is not an error.
        let mut damaged = mcap.clone();
        damaged[name_at] = b'X';
        let items = drain(&mut MessageReader::new(), &damaged);
        assert!(items.iter().all(|item| item.is_ok()), "{items:?}");
        assert_eq!(items.len(), 4);

        // Truncated summary or bad end magic: every message, then the error.
        let truncated = &mcap[..summary_start + 20];
        let items = drain(&mut MessageReader::new(), truncated);
        assert_eq!(items.len(), 5, "{items:?}");
        assert!(items[..4].iter().all(|item| item.is_ok()));
        assert!(matches!(items[4], Err(McapError::UnexpectedEof)));

        let mut bad_magic = mcap.clone();
        *bad_magic.last_mut().unwrap() ^= 0xFF;
        let items = drain(&mut MessageReader::new(), &bad_magic);
        assert_eq!(items.len(), 5, "{items:?}");
        assert!(matches!(items[4], Err(McapError::BadMagic)));

        // With skip_end_magic the reader ends at the data end record.
        for file in [truncated, &bad_magic[..]] {
            let mut reader = MessageReader::new_with_options(
                LinearReaderOptions::default().with_skip_end_magic(true),
            );
            let items = drain(&mut reader, file);
            assert!(items.iter().all(|item| item.is_ok()), "{items:?}");
            assert_eq!(items.len(), 4);
        }
    }

    #[test]
    fn validates_chunk_crcs_only_when_asked() {
        let mut mcap = two_channel_mcap(true);
        // The last chunk ends with a message, so its final byte is payload: flipping it leaves
        // every record parseable and only the CRC wrong. (The first chunk ends with a channel
        // record, whose last byte is a length field.)
        let chunk = *super::test_support::record_offsets(&mcap, crate::records::op::CHUNK)
            .last()
            .expect("a chunk");
        // Body: start/end time (16), uncompressed size (8), crc (4), compression len (4) + "",
        // records len (8), then the records.
        let records_len_at = chunk + 9 + 16 + 8 + 4 + 4;
        let records_len =
            u64::from_le_bytes(mcap[records_len_at..records_len_at + 8].try_into().unwrap());
        let records = records_len_at + 8;
        mcap[records + records_len as usize - 1] ^= 0xFF;

        // Like LinearReader, the defaults do not validate chunk CRCs.
        let lenient = drain(&mut MessageReader::new(), &mcap);
        assert!(
            lenient.iter().all(|item| item.is_ok()),
            "new() must not validate chunk CRCs: {lenient:?}"
        );

        let strict = drain(
            &mut MessageReader::new_with_options(
                LinearReaderOptions::default().with_validate_chunk_crcs(true),
            ),
            &mcap,
        );
        assert!(
            strict
                .iter()
                .any(|item| matches!(item, Err(McapError::BadChunkCrc { .. }))),
            "with_validate_chunk_crcs(true) must report the bad chunk CRC: {strict:?}"
        );
    }

    #[test]
    fn rejects_schema_id_zero() {
        let mut mcap = two_channel_mcap(false);
        let schema = super::test_support::record_offsets(&mcap, crate::records::op::SCHEMA)[0];
        mcap[schema + 9..schema + 11].copy_from_slice(&0u16.to_le_bytes());
        let items = drain(&mut MessageReader::new(), &mcap);
        assert!(
            matches!(items.as_slice(), [Err(McapError::InvalidSchemaId)]),
            "{items:?}"
        );
    }

    #[test]
    fn rejects_unknown_channel() {
        let mut mcap = two_channel_mcap(false);
        let message = super::test_support::record_offsets(&mcap, crate::records::op::MESSAGE)[0];
        mcap[message + 9..message + 11].copy_from_slice(&99u16.to_le_bytes());
        let items = drain(&mut MessageReader::new(), &mcap);
        assert!(
            matches!(items.as_slice(), [Err(McapError::UnknownChannel(1, 99))]),
            "{items:?}"
        );
    }

    #[test]
    fn exposes_channels_seen_so_far() {
        let mcap = two_channel_mcap(false);
        let mut reader = MessageReader::new();
        assert!(reader.get_channel(1).is_none());
        let items = drain(&mut reader, &mcap);
        assert_eq!(items.len(), 4);
        assert_eq!(
            reader.get_channel(1).expect("channel 1").topic,
            "/with_schema"
        );
        assert_eq!(
            reader.get_channel(2).expect("channel 2").topic,
            "/schemaless"
        );
    }
}
