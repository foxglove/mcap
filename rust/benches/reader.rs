use criterion::{criterion_group, criterion_main, Criterion};
use mcap::{sans_io, Channel, Message, MessageStream, Schema};
use std::borrow::Cow;
use std::io::Cursor;
use std::sync::Arc;
use std::time::Duration;

fn create_test_mcap(n: usize, compression: Option<mcap::Compression>) -> Vec<u8> {
    let mut buffer = Vec::new();
    {
        let mut writer = mcap::WriteOptions::new()
            .compression(compression)
            .profile("fooey")
            .create(Cursor::new(&mut buffer))
            .unwrap();
        // Mock message data to align with reader benchmarks in ts
        const MESSAGE_DATA: &[u8] = &[42; 10];

        let schema = Arc::new(Schema {
            id: 1,
            name: "TestSchema".to_string(),
            encoding: "raw".to_string(),
            data: Cow::Borrowed(b"{}"),
        });

        let channel = Arc::new(Channel {
            id: 0,
            topic: "test_topic".to_string(),
            message_encoding: "raw".to_string(),
            metadata: Default::default(),
            schema: Some(schema),
        });

        for i in 0..n {
            let message = Message {
                channel: channel.clone(),
                sequence: i as u32,
                log_time: i as u64,
                publish_time: i as u64,
                data: Cow::Borrowed(MESSAGE_DATA),
            };
            writer.write(&message).unwrap();
        }

        writer.finish().unwrap();
    }
    buffer
}

fn load_summary(file: &mut std::io::Cursor<&[u8]>) -> mcap::Summary {
    use std::io::{Read, Seek};
    let mut reader = sans_io::SummaryReader::new();
    while let Some(event) = reader.next_event() {
        match event.expect("next event failed") {
            sans_io::SummaryReadEvent::ReadRequest(n) => {
                let read = file.read(reader.insert(n)).expect("read failed");
                reader.notify_read(read);
            }
            sans_io::SummaryReadEvent::SeekRequest(pos) => {
                reader.notify_seeked(file.seek(pos).expect("seek failed"));
            }
        }
    }
    reader.finish().unwrap()
}

fn get_next_message(
    reader: &mut sans_io::IndexedReader,
    file: &mut std::io::Cursor<&[u8]>,
    into: &mut Vec<u8>,
) -> Option<mcap::records::MessageHeader> {
    use std::io::{Read, Seek};
    let mut buf = Vec::new();
    while let Some(event) = reader.next_event() {
        match event.expect("next event failed") {
            sans_io::IndexedReadEvent::Message { header, data } => {
                into.resize(data.len(), 0);
                into.copy_from_slice(data);
                reader.consume_message();
                return Some(header);
            }
            sans_io::IndexedReadEvent::ReadChunkRequest { offset, length } => {
                file.seek(std::io::SeekFrom::Start(offset))
                    .expect("failed to seek");
                buf.resize(length, 0);
                file.read_exact(&mut buf).expect("failed to read");
                reader
                    .insert_chunk_record_data(offset, &buf)
                    .expect("failed to insert");
            }
        }
    }
    None
}

fn bench_read_messages(c: &mut Criterion) {
    const N: usize = 1_000_000;
    let mcap_data_uncompressed = create_test_mcap(N, None);
    let mcap_data_lz4 = create_test_mcap(N, Some(mcap::Compression::Lz4));
    let mcap_data_zstd = create_test_mcap(N, Some(mcap::Compression::Zstd));
    {
        let mut group = c.benchmark_group("mcap_read_linear");
        group.throughput(criterion::Throughput::Elements(N as u64));

        group.bench_function("MessageStream_1M_uncompressed", |b| {
            b.iter(|| {
                let stream = MessageStream::new(&mcap_data_uncompressed).unwrap();
                for message in stream {
                    std::hint::black_box(message.unwrap());
                }
            });
        });

        group.bench_function("MessageStream_1M_lz4", |b| {
            b.iter(|| {
                let stream = MessageStream::new(&mcap_data_lz4).unwrap();
                for message in stream {
                    std::hint::black_box(message.unwrap());
                }
            });
        });

        group.bench_function("MessageStream_1M_zstd", |b| {
            b.iter(|| {
                let stream = MessageStream::new(&mcap_data_zstd).unwrap();
                for message in stream {
                    std::hint::black_box(message.unwrap());
                }
            });
        });

        group.finish();
    }
    {
        let mut group = c.benchmark_group("mcap_read_indexed");
        group.throughput(criterion::Throughput::Elements(N as u64));

        group.bench_function("IndexedReader_1M_uncompressed", |b| {
            b.iter(|| {
                let mut file = std::io::Cursor::new(&mcap_data_uncompressed[..]);
                let summary = load_summary(&mut file);
                let mut reader =
                    sans_io::IndexedReader::new(&summary).expect("could not build reader");
                let mut data_buf = Vec::new();
                while let Some(header) = get_next_message(&mut reader, &mut file, &mut data_buf) {
                    let message = mcap::Message {
                        channel: summary.channels.get(&header.channel_id).unwrap().clone(),
                        sequence: header.sequence,
                        log_time: header.log_time,
                        publish_time: header.publish_time,
                        data: Cow::Borrowed(&data_buf),
                    };
                    std::hint::black_box(message);
                }
            });
        });

        group.bench_function("IndexedReader_1M_zstd", |b| {
            b.iter(|| {
                let mut file = std::io::Cursor::new(&mcap_data_zstd[..]);
                let summary = load_summary(&mut file);
                let mut reader =
                    sans_io::IndexedReader::new(&summary).expect("could not build reader");
                let mut data_buf = Vec::new();
                while let Some(header) = get_next_message(&mut reader, &mut file, &mut data_buf) {
                    let message = mcap::Message {
                        channel: summary.channels.get(&header.channel_id).unwrap().clone(),
                        sequence: header.sequence,
                        log_time: header.log_time,
                        publish_time: header.publish_time,
                        data: Cow::Borrowed(&data_buf),
                    };
                    std::hint::black_box(message);
                }
            });
        });

        group.bench_function("IndexedReader_1M_lz4", |b| {
            b.iter(|| {
                let mut file = std::io::Cursor::new(&mcap_data_lz4[..]);
                let summary = load_summary(&mut file);
                let mut reader =
                    sans_io::IndexedReader::new(&summary).expect("could not build reader");
                let mut data_buf = Vec::new();
                while let Some(header) = get_next_message(&mut reader, &mut file, &mut data_buf) {
                    let message = mcap::Message {
                        channel: summary.channels.get(&header.channel_id).unwrap().clone(),
                        sequence: header.sequence,
                        log_time: header.log_time,
                        publish_time: header.publish_time,
                        data: Cow::Borrowed(&data_buf),
                    };
                    std::hint::black_box(message);
                }
            });
        });
        group.finish();
    }
}

criterion_group! {
    name = benches;
    config = Criterion::default().warm_up_time(Duration::from_secs(1)).sample_size(10);
    targets = bench_read_messages
}
criterion_main!(benches);
