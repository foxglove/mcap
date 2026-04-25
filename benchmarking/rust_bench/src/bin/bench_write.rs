use mcap::write::WriteOptions;
use mcap::Compression;
use std::collections::BTreeMap;
use std::io::BufWriter;
use std::time::Instant;

fn fill_payload(buf: &mut [u8], varied: bool) {
    for (i, byte) in buf.iter_mut().enumerate() {
        *byte = if varied { ((i.wrapping_mul(137).wrapping_add(43)) & 0xff) as u8 } else { 0x42 };
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 6 {
        eprintln!("Usage: {} <output_file> <mode> <num_messages> <payload_size|mixed> <uniform|varied>", args[0]);
        eprintln!("  mode: unchunked | chunked | zstd | lz4");
        std::process::exit(1);
    }

    let varied_fill = args[5] == "varied";

    let filename = &args[1];
    let mode = &args[2];
    let is_mixed = args[4] == "mixed";

    let opts = match mode.as_str() {
        "unchunked" => WriteOptions::new()
            .use_chunks(false)
            .profile("bench")
            .library("rust-bench"),
        "chunked" => WriteOptions::new()
            .compression(None)
            .chunk_size(Some(786432))
            .profile("bench")
            .library("rust-bench"),
        "zstd" => WriteOptions::new()
            .compression(Some(Compression::Zstd))
            .chunk_size(Some(786432))
            .profile("bench")
            .library("rust-bench"),
        "lz4" => WriteOptions::new()
            .compression(Some(Compression::Lz4))
            .chunk_size(Some(786432))
            .profile("bench")
            .library("rust-bench"),
        _ => {
            eprintln!("Unknown mode: {}", mode);
            std::process::exit(1);
        }
    };

    let file = std::fs::File::create(filename).expect("Failed to create file");
    let buf_writer = BufWriter::new(file);
    let mut writer = opts.create(buf_writer).expect("Failed to create writer");

    let schema_data = b"{\"type\":\"object\"}";
    let metadata = BTreeMap::new();

    if is_mixed {
        // ── Mixed-payload mode: simulate a 10-second robot recording ──

        // Channel definitions: (topic, schema_name, base_payload_sizes, period_ns, count)
        struct ChanDef {
            topic: &'static str,
            schema_name: &'static str,
            payload_sizes: &'static [usize],
            period_ns: u64,
            count: u64,
        }
        let chan_defs: [ChanDef; 5] = [
            ChanDef { topic: "/imu",                schema_name: "IMU",             payload_sizes: &[96],                              period_ns: 5_000_000,   count: 2000 },
            ChanDef { topic: "/odom",               schema_name: "Odometry",        payload_sizes: &[296],                             period_ns: 20_000_000,  count: 500  },
            ChanDef { topic: "/tf",                  schema_name: "TFMessage",       payload_sizes: &[80, 160, 320, 800, 1600],         period_ns: 10_000_000,  count: 1000 },
            ChanDef { topic: "/lidar",              schema_name: "PointCloud2",     payload_sizes: &[230_400],                         period_ns: 100_000_000, count: 100  },
            ChanDef { topic: "/camera/compressed",  schema_name: "CompressedImage", payload_sizes: &[524_288],                         period_ns: 66_666_667,  count: 150  },
        ];

        // Register schemas and channels (not timed)
        let mut channel_ids: Vec<u16> = Vec::new();
        for def in &chan_defs {
            let sid = writer
                .add_schema(def.schema_name, "jsonschema", schema_data)
                .expect("Failed to add schema");
            let cid = writer
                .add_channel(sid, def.topic, "json", &metadata)
                .expect("Failed to add channel");
            channel_ids.push(cid);
        }

        // Pre-generate sorted schedule: (timestamp, channel_index)
        let mut schedule: Vec<(u64, usize)> = Vec::new();
        for (ch_idx, def) in chan_defs.iter().enumerate() {
            for msg_i in 0..def.count {
                let timestamp = msg_i * def.period_ns;
                schedule.push((timestamp, ch_idx));
            }
        }
        // Sort by timestamp; ties broken by channel index (stable sort preserves push order)
        schedule.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));

        // Pre-allocate payload buffers for every distinct size
        let all_sizes: [usize; 9] = [96, 296, 80, 160, 320, 800, 1600, 230_400, 524_288];
        let mut payload_bufs: std::collections::HashMap<usize, Vec<u8>> = std::collections::HashMap::new();
        for &sz in &all_sizes {
            let mut buf = vec![0u8; sz];
            fill_payload(&mut buf, varied_fill);
            payload_bufs.insert(sz, buf);
        }

        // Per-channel sequence counters and message index (for cycling /tf sizes)
        let mut seq_counters: [u32; 5] = [0; 5];

        // ── Timed: message loop + finish ──
        let start = Instant::now();

        for &(timestamp, ch_idx) in &schedule {
            let def = &chan_defs[ch_idx];
            let seq = seq_counters[ch_idx];
            seq_counters[ch_idx] += 1;

            let payload_size = def.payload_sizes[seq as usize % def.payload_sizes.len()];
            let payload = &payload_bufs[&payload_size];

            writer
                .write_to_known_channel(
                    &mcap::records::MessageHeader {
                        channel_id: channel_ids[ch_idx],
                        sequence: seq,
                        log_time: timestamp,
                        publish_time: timestamp,
                    },
                    payload,
                )
                .expect("Failed to write message");
        }

        writer.finish().expect("Failed to finish");

        let elapsed = start.elapsed();
        let elapsed_ns = elapsed.as_nanos();
        let wall_sec = elapsed.as_secs_f64();

        let file_size = std::fs::metadata(filename)
            .expect("Failed to stat file")
            .len();

        let peak_rss_kb = unsafe {
            let mut rusage: libc::rusage = std::mem::zeroed();
            libc::getrusage(libc::RUSAGE_SELF, &mut rusage);
            rusage.ru_maxrss
        };

        println!(
            "write\trust\t{}\t{}\t{}\t{}\t{}\t{:.6}\t{}",
            mode, 3750, "mixed", file_size, elapsed_ns, wall_sec, peak_rss_kb
        );
    } else {
        // ── Fixed-payload mode (original behavior) ──
        let num_messages: u64 = args[3].parse().expect("invalid num_messages");
        let payload_size: usize = args[4].parse().expect("invalid payload_size");

        let mut payload = vec![0u8; payload_size];
        fill_payload(&mut payload, varied_fill);

        let schema_id = writer
            .add_schema("BenchMsg", "jsonschema", schema_data)
            .expect("Failed to add schema");

        let channel_id = writer
            .add_channel(schema_id, "/bench", "json", &metadata)
            .expect("Failed to add channel");

        // Timed: message loop + finish
        let start = Instant::now();

        for i in 0..num_messages {
            let log_time = i * 1000;
            writer
                .write_to_known_channel(
                    &mcap::records::MessageHeader {
                        channel_id,
                        sequence: i as u32,
                        log_time,
                        publish_time: log_time,
                    },
                    &payload,
                )
                .expect("Failed to write message");
        }

        writer.finish().expect("Failed to finish");

        let elapsed = start.elapsed();
        let elapsed_ns = elapsed.as_nanos();
        let wall_sec = elapsed.as_secs_f64();

        let file_size = std::fs::metadata(filename)
            .expect("Failed to stat file")
            .len();

        let peak_rss_kb = unsafe {
            let mut rusage: libc::rusage = std::mem::zeroed();
            libc::getrusage(libc::RUSAGE_SELF, &mut rusage);
            rusage.ru_maxrss
        };

        println!(
            "write\trust\t{}\t{}\t{}\t{}\t{}\t{:.6}\t{}",
            mode, num_messages, payload_size, file_size, elapsed_ns, wall_sec, peak_rss_kb
        );
    }
}
