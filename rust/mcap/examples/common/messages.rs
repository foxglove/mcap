//! Example helpers for reading MCAP files through the sans-io readers.
//!
//! [`MessageReader`] turns any [`Read`] source into an iterator of linked [`Message`]s, the way
//! [`mcap::MessageStream`] does for a byte slice, but streaming one record at a time so memory
//! scales with the largest record or chunk instead of the file. [`read_summary`] loads only the
//! summary section from any [`Read`] + [`Seek`] source.
#![allow(dead_code)]

use std::{
    borrow::Cow,
    collections::HashMap,
    io::{Read, Seek},
    sync::Arc,
};

use anyhow::{anyhow, Result};
use mcap::{
    records::Record,
    sans_io::{
        LinearReadEvent, LinearReader, LinearReaderOptions, SummaryReadEvent, SummaryReader,
    },
    Channel, Message, Schema, Summary,
};

/// Streams linked messages from any source of bytes.
pub struct MessageReader<R> {
    source: R,
    reader: LinearReader,
    schemas: HashMap<u16, Arc<Schema<'static>>>,
    channels: HashMap<u16, Arc<Channel<'static>>>,
}

impl<R: Read> MessageReader<R> {
    pub fn new(source: R) -> Self {
        Self::with_options(source, LinearReaderOptions::default())
    }

    pub fn with_options(source: R, options: LinearReaderOptions) -> Self {
        Self {
            source,
            reader: LinearReader::new_with_options(options),
            schemas: HashMap::new(),
            channels: HashMap::new(),
        }
    }
}

impl<R: Read> Iterator for MessageReader<R> {
    type Item = Result<Message<'static>>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let event = match self.reader.next_event()? {
                Ok(event) => event,
                Err(err) => return Some(Err(err.into())),
            };
            match event {
                LinearReadEvent::ReadRequest(need) => {
                    let buf = self.reader.insert(need);
                    match self.source.read(buf) {
                        Ok(read) => self.reader.notify_read(read),
                        Err(err) => return Some(Err(err.into())),
                    }
                }
                LinearReadEvent::Record { opcode, data } => {
                    let record = match mcap::parse_record(opcode, data) {
                        Ok(record) => record,
                        Err(err) => return Some(Err(err.into())),
                    };
                    match record {
                        Record::Schema { header, data } => {
                            self.schemas.insert(
                                header.id,
                                Arc::new(Schema {
                                    id: header.id,
                                    name: header.name,
                                    encoding: header.encoding,
                                    data: Cow::Owned(data.into_owned()),
                                }),
                            );
                        }
                        Record::Channel(channel) => {
                            let schema = if channel.schema_id == 0 {
                                None
                            } else {
                                match self.schemas.get(&channel.schema_id) {
                                    Some(schema) => Some(schema.clone()),
                                    None => {
                                        return Some(Err(anyhow!(
                                            "channel {} references unknown schema {}",
                                            channel.id,
                                            channel.schema_id
                                        )))
                                    }
                                }
                            };
                            self.channels.insert(
                                channel.id,
                                Arc::new(Channel {
                                    id: channel.id,
                                    topic: channel.topic,
                                    schema,
                                    message_encoding: channel.message_encoding,
                                    metadata: channel.metadata,
                                }),
                            );
                        }
                        Record::Message { header, data } => {
                            let Some(channel) = self.channels.get(&header.channel_id).cloned()
                            else {
                                return Some(Err(anyhow!(
                                    "message references unknown channel {}",
                                    header.channel_id
                                )));
                            };
                            return Some(Ok(Message {
                                channel,
                                sequence: header.sequence,
                                log_time: header.log_time,
                                publish_time: header.publish_time,
                                data: Cow::Owned(data.into_owned()),
                            }));
                        }
                        _ => {}
                    }
                }
            }
        }
    }
}

/// Reads only the summary section (footer, indexes, statistics) from a seekable source.
///
/// Returns `Ok(None)` when the file has no summary section.
pub fn read_summary(source: &mut (impl Read + Seek)) -> Result<Option<Summary>> {
    let mut reader = SummaryReader::new();
    while let Some(event) = reader.next_event() {
        match event? {
            SummaryReadEvent::SeekRequest(pos) => {
                let at = source.seek(pos)?;
                reader.notify_seeked(at);
            }
            SummaryReadEvent::ReadRequest(need) => {
                let read = source.read(reader.insert(need))?;
                reader.notify_read(read);
            }
        }
    }
    Ok(reader.finish())
}
