#[path = "common/serialization.rs"]
mod serialization;
use std::collections::BTreeSet;
use std::io::{Cursor, Read, Seek};

use mcap::records::SchemaHeader;
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
    let mut cursor = Cursor::new(&file);
    let summary = {
        let mut reader = mcap::sans_io::SummaryReader::new();
        while let Some(event) = reader.next_event() {
            match event.expect("failed gathering summary") {
                mcap::sans_io::SummaryReadEvent::SeekRequest(pos) => {
                    reader.notify_seeked(cursor.seek(pos).expect("failed to seek file"));
                }
                mcap::sans_io::SummaryReadEvent::ReadRequest(n) => {
                    let read = cursor.read(reader.insert(n)).expect("failed to read file");
                    reader.notify_read(read);
                }
            }
        }
        reader.finish().expect("file should have a summary section")
    };

    let mut reader =
        mcap::sans_io::IndexedReader::new(&summary).expect("failed to initialize indexed reader");
    while let Some(event) = reader.next_event() {
        match event.expect("failed to get next event") {
            mcap::sans_io::IndexedReadEvent::SeekRequest(pos) => {
                reader.notify_seeked(cursor.seek(pos).expect("failed to seek file"));
            }
            mcap::sans_io::IndexedReadEvent::ReadRequest(n) => {
                let read = cursor.read(reader.insert(n)).expect("failed to read file");
                reader.notify_read(read);
            }
            mcap::sans_io::IndexedReadEvent::Message { header, data } => {
                messages.push(serialization::as_json(&mcap::records::Record::Message {
                    header,
                    data: std::borrow::Cow::Borrowed(data),
                }));
            }
        }
    }

    let mut statistics: Vec<Value> = vec![];
    if let Some(stats) = summary.stats {
        statistics.push(serialization::as_json(&mcap::records::Record::Statistics(
            stats.clone(),
        )));
    };

    let schemas: Vec<_> = summary
        .schemas
        .values()
        .map(|schema| {
            serialization::as_json(&mcap::records::Record::Schema {
                header: SchemaHeader {
                    id: schema.id,
                    name: schema.name.clone(),
                    encoding: schema.encoding.clone(),
                },
                data: schema.data.to_owned(),
            })
        })
        .collect();

    let channels: Vec<_> = summary
        .channels
        .values()
        .map(|channel| {
            serialization::as_json(&mcap::records::Record::Channel(mcap::records::Channel {
                id: channel.id,
                schema_id: channel.schema.as_ref().map(|s| s.id).unwrap_or(0),
                topic: channel.topic.clone(),
                message_encoding: channel.message_encoding.clone(),
                metadata: channel.metadata.clone(),
            }))
        })
        .collect();

    let out = json!({ "messages": messages, "schemas": schemas, "channels": channels, "statistics": statistics });
    print!("{}", serde_json::to_string_pretty(&out).unwrap());
}
