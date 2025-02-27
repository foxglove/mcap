#[path = "common/serialization.rs"]
mod serialization;
use std::collections::BTreeSet;
use std::io::{Cursor, Read, Seek};

use serde_json::{json, Value};

use std::env;
use std::process;

pub fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("Please supply an MCAP file as argument");
        process::exit(1);
    }
    let file = std::fs::read(&args[1]).expect("file wouldn't open");
    let mut messages: Vec<Value> = vec![];
    let mut schemas: Vec<Value> = vec![];
    let mut channels: Vec<Value> = vec![];
    let mut statistics: Vec<Value> = vec![];
    let mut cursor = Cursor::new(&file);
    let summary = {
        let mut reader = mcap::sans_io::SummaryReader::new();
        while let Some(event) = reader.next_event() {
            match event.expect("failed gathering summary") {
                mcap::sans_io::SummaryReadEvent::Seek(pos) => {
                    reader.notify_seeked(cursor.seek(pos).expect("failed to seek file"));
                }
                mcap::sans_io::SummaryReadEvent::Read(n) => {
                    let read = cursor.read(reader.insert(n)).expect("failed to read file");
                    reader.notify_read(read);
                }
            }
        }
        reader.finish().expect("file should have a summary section")
    };
    let mut seen_schemas = BTreeSet::new();
    let mut seen_channels = BTreeSet::new();
    let mut reader =
        mcap::sans_io::IndexedReader::new(&summary).expect("failed to initialize indexed reader");
    while let Some(event) = reader.next_event() {
        match event.expect("failed to get next event") {
            mcap::sans_io::IndexedReadEvent::Seek(pos) => {
                reader.notify_seeked(cursor.seek(pos).expect("failed to seek file"));
            }
            mcap::sans_io::IndexedReadEvent::Read(n) => {
                let read = cursor.read(reader.insert(n)).expect("failed to read file");
                reader.notify_read(read);
            }
            mcap::sans_io::IndexedReadEvent::Message { header, data } => {
                let channel_id = header.channel_id;
                if !seen_channels.contains(&channel_id) {
                    let channel = summary
                        .channels
                        .get(&channel_id)
                        .expect("malformed MCAP: channel not in summary");
                    if let Some(schema) = channel.schema.clone() {
                        if !seen_schemas.contains(&schema.id) {
                            schemas.push(serialization::as_json(&mcap::records::Record::Schema {
                                header: mcap::records::SchemaHeader {
                                    id: schema.id,
                                    name: schema.name.clone(),
                                    encoding: schema.encoding.clone(),
                                },
                                data: schema.data.clone(),
                            }));
                            seen_schemas.insert(schema.id);
                        }
                    }
                    channels.push(serialization::as_json(&mcap::records::Record::Channel(
                        mcap::records::Channel {
                            id: channel.id,
                            schema_id: channel.schema.as_ref().map(|s| s.id).unwrap_or(0),
                            topic: channel.topic.clone(),
                            message_encoding: channel.message_encoding.clone(),
                            metadata: channel.metadata.clone(),
                        },
                    )));
                    seen_channels.insert(channel.id);
                }
                messages.push(serialization::as_json(&mcap::records::Record::Message {
                    header,
                    data: std::borrow::Cow::Borrowed(data),
                }));
            }
        }
    }

    if let Some(stats) = summary.stats {
        statistics.push(serialization::as_json(&mcap::records::Record::Statistics(
            stats.clone(),
        )));
    }

    let out = json!({ "messages": messages, "schemas": schemas, "channels": channels, "statistics": statistics });
    print!("{}", serde_json::to_string_pretty(&out).unwrap());
}
