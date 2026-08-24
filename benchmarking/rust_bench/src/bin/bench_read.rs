use std::io::Read;
use std::time::Instant;

use mcap::sans_io::indexed_reader::{IndexedReadEvent, IndexedReader, IndexedReaderOptions};
use mcap::sans_io::linear_reader::{LinearReadEvent, LinearReader};

/// ru_maxrss is KB on Linux but bytes on macOS; normalize to KB.
fn peak_rss_kb() -> libc::c_long {
    let mut rusage: libc::rusage = unsafe { std::mem::zeroed() };
    unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut rusage) };
    if cfg!(target_os = "macos") {
        rusage.ru_maxrss / 1024
    } else {
        rusage.ru_maxrss
    }
}

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

    let mut msg_count: u64 = 0;

    match filter {
        None => {
            // Stream the file through the sans-io LinearReader to keep
            // memory bounded rather than buffering the whole file. The
            // reader requests a few bytes at a time on unchunked files, so
            // buffer the underlying reads.
            let file = std::fs::File::open(filename).expect("Failed to open file");
            let mut file = std::io::BufReader::with_capacity(1 << 20, file);
            let mut reader = LinearReader::new();
            while let Some(event) = reader.next_event() {
                match event.expect("Failed to read event") {
                    LinearReadEvent::ReadRequest(need) => {
                        let written = file.read(reader.insert(need)).expect("Failed to read file");
                        reader.notify_read(written);
                    }
                    LinearReadEvent::Record { opcode, data } => {
                        if opcode == mcap::records::op::MESSAGE {
                            let record = mcap::parse_record(opcode, data)
                                .expect("Failed to parse message record");
                            if let mcap::records::Record::Message { data, .. } = record {
                                // Touch data to prevent dead-code elimination
                                if data.is_empty() {
                                    eprintln!("Empty message");
                                }
                                msg_count += 1;
                            }
                        }
                    }
                }
            }
        }
        Some(filter_mode) => {
            // The indexed reader operates on byte slices, so the filtered
            // path buffers the whole file. Filtered results do not feed
            // the memory table.
            let buf = std::fs::read(filename).expect("Failed to read file");
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

    let file_size = std::fs::metadata(filename)
        .expect("Failed to stat file")
        .len();

    let peak_rss_kb = peak_rss_kb();

    // TSV output: op lang mode num_msgs payload_size file_size elapsed_ns wall_sec peak_rss_kb
    // msg_count
    println!(
        "read\trust\t{}\t{}\t{}\t{}\t{}\t{:.6}\t{}\t{}",
        mode, num_messages_str, payload_size_str, file_size, elapsed_ns, wall_sec, peak_rss_kb,
        msg_count
    );
}
