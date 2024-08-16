use criterion::{criterion_group, criterion_main, Criterion};
use mcap::{parse_record, Channel, Message, MessageStream, Schema};
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
            name: "TestSchema".to_string(),
            encoding: "raw".to_string(),
            data: Cow::Borrowed(b"{}"),
        });

        let channel = Arc::new(Channel {
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
                data: Cow::Borrowed(&MESSAGE_DATA),
            };
            writer.write(&message).unwrap();
        }

        writer.finish().unwrap();
    }
    buffer
}

fn bench_read_messages(c: &mut Criterion) {
    const N: usize = 1_000_000;
    let mcap_data_uncompressed = create_test_mcap(N, None);
    let mcap_data_lz4 = create_test_mcap(N, Some(mcap::Compression::Lz4));
    let mcap_data_zstd = create_test_mcap(N, Some(mcap::Compression::Zstd));
    let mut group = c.benchmark_group("mcap_read");
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
    #[cfg(feature = "tokio")]
    {
        use mcap::tokio::read::RecordReader;
        use tokio::runtime::Builder;

        let rt = Builder::new_current_thread().build().unwrap();
        group.bench_function("AsyncMessageStream_1M_uncompressed", |b| {
            b.to_async(&rt).iter(|| async {
                let mut reader = RecordReader::new(Cursor::new(&mcap_data_uncompressed));
                let mut record = Vec::new();
                let mut count = 0;
                while let Some(result) = reader.next_record(&mut record).await {
                    count += 1;
                    std::hint::black_box(parse_record(result.unwrap(), &record).unwrap());
                }
                assert_eq!(count, N + 118);
            });
        });

        group.bench_function("AsyncMessageStream_1M_zstd", |b| {
            b.to_async(&rt).iter(|| async {
                let mut reader = RecordReader::new(Cursor::new(&mcap_data_zstd));
                let mut record = Vec::new();
                let mut count = 0;
                while let Some(result) = reader.next_record(&mut record).await {
                    count += 1;
                    std::hint::black_box(parse_record(result.unwrap(), &record).unwrap());
                }
                assert_eq!(count, N + 118);
            });
        });

        group.bench_function("AsyncMessageStream_1M_lz4", |b| {
            b.to_async(&rt).iter(|| async {
                let mut reader = RecordReader::new(Cursor::new(&mcap_data_lz4));
                let mut record = Vec::new();
                let mut count = 0;
                while let Some(result) = reader.next_record(&mut record).await {
                    count += 1;
                    std::hint::black_box(parse_record(result.unwrap(), &record).unwrap());
                }
                assert_eq!(count, N + 118);
            });
        });
    }

    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default().warm_up_time(Duration::from_secs(1)).sample_size(10);
    targets = bench_read_messages
}
criterion_main!(benches);
