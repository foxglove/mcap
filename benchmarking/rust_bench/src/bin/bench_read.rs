use std::time::Instant;

use mcap::sans_io::indexed_reader::{IndexedReadEvent, IndexedReader, IndexedReaderOptions};

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 || args.len() > 6 {
        eprintln!(
            "Usage: {} <input_file> [mode] [num_messages] [payload_size] [filter]",
            args[0]
        );
        eprintln!("  filter: topic | timerange | topic_timerange");
        std::process::exit(1);
    }

    let filename = &args[1];
    let mode = if args.len() >= 3 { &args[2] } else { "unknown" };
    let num_messages_str = if args.len() >= 4 { &args[3] } else { "0" };
    let payload_size_str = if args.len() >= 5 { &args[4] } else { "0" };
    let filter = if args.len() >= 6 {
        Some(args[5].as_str())
    } else {
        None
    };

    // Timed: file read + message iteration
    let start = Instant::now();

    let buf = std::fs::read(filename).expect("Failed to read file");

    let mut msg_count: u64 = 0;

    match filter {
        None => {
            for msg in
                mcap::MessageStream::new(&buf).expect("Failed to create message stream")
            {
                let msg = msg.expect("Failed to read message");
                // Touch data to prevent dead-code elimination
                if msg.data.is_empty() {
                    eprintln!("Empty message");
                }
                msg_count += 1;
            }
        }
        Some(filter_mode) => {
            let summary = mcap::Summary::read(&buf)
                .expect("Failed to read summary")
                .expect("No summary found in file");

            let options = match filter_mode {
                "topic" => IndexedReaderOptions::new().include_topics(vec!["/imu"]),
                "timerange" => IndexedReaderOptions::new()
                    .log_time_on_or_after(3_000_000_000)
                    .log_time_before(5_000_000_000),
                "topic_timerange" => IndexedReaderOptions::new()
                    .include_topics(vec!["/lidar"])
                    .log_time_on_or_after(4_000_000_000)
                    .log_time_before(6_000_000_000),
                other => {
                    eprintln!("Unknown filter mode: {}", other);
                    std::process::exit(1);
                }
            };

            let mut reader = IndexedReader::new_with_options(&summary, options)
                .expect("Failed to create indexed reader");
            while let Some(event) = reader.next_event() {
                match event.expect("Failed to read event") {
                    IndexedReadEvent::ReadChunkRequest { offset, length } => {
                        let chunk_data = &buf[offset as usize..][..length];
                        reader
                            .insert_chunk_record_data(offset, chunk_data)
                            .expect("Failed to insert chunk data");
                    }
                    IndexedReadEvent::Message { header: _, data } => {
                        msg_count += 1;
                        if data.is_empty() {
                            eprintln!("Empty message");
                        }
                    }
                }
            }
        }
    }

    let elapsed = start.elapsed();
    let elapsed_ns = elapsed.as_nanos();
    let wall_sec = elapsed.as_secs_f64();

    let file_size = buf.len();

    let peak_rss_kb = unsafe {
        let mut rusage: libc::rusage = std::mem::zeroed();
        libc::getrusage(libc::RUSAGE_SELF, &mut rusage);
        rusage.ru_maxrss
    };

    println!(
        "read\trust\t{}\t{}\t{}\t{}\t{}\t{:.6}\t{}",
        mode, num_messages_str, payload_size_str, file_size, elapsed_ns, wall_sec, peak_rss_kb
    );

    let _ = msg_count;
}
