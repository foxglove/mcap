// Cross-language correlation check (Rust).
use std::collections::BTreeMap;
use std::fs;
use std::io::BufWriter;
use std::time::Instant;

fn fill(buf: &mut [u8], seq: u64) {
    for (i, b) in buf.iter_mut().enumerate() {
        *b = ((i as u64 + seq) & 0xff) as u8;
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 7 {
        eprintln!("usage: xlbench <write|read> <file> <num> <size> <chunk> <none|zstd>");
        std::process::exit(1);
    }
    let op = &args[1];
    let file = &args[2];
    let num: u64 = args[3].parse().unwrap();
    let size: usize = args[4].parse().unwrap();
    let chunk: u64 = args[5].parse().unwrap();
    let comp = &args[6];

    if op == "write" {
        let compression = if comp == "zstd" {
            Some(mcap::Compression::Zstd)
        } else {
            None
        };
        let f = BufWriter::new(fs::File::create(file).unwrap());
        let mut w = mcap::WriteOptions::new()
            .compression(compression)
            .chunk_size(Some(chunk))
            .profile("xl")
            .create(f)
            .unwrap();
        let schema_id = w.add_schema("Bench", "jsonschema", b"{}").unwrap();
        let channel_id = w
            .add_channel(schema_id, "/bench", "json", &BTreeMap::new())
            .unwrap();
        let mut buf = vec![0u8; size];
        fill(&mut buf, 0); // one reusable payload, generated outside timing
        let start = Instant::now();
        for i in 0..num {
            w.write_to_known_channel(
                &mcap::records::MessageHeader {
                    channel_id,
                    sequence: i as u32,
                    log_time: i * 1000,
                    publish_time: i * 1000,
                },
                &buf,
            )
            .unwrap();
        }
        w.finish().unwrap();
        let wall = start.elapsed().as_secs_f64();
        let fsize = fs::metadata(file).unwrap().len();
        println!("rust\twrite\t{}\t{}\t{}\t{}\t{:.6}", comp, num, num * size as u64, fsize, wall);
    } else {
        let data = fs::read(file).unwrap();
        let start = Instant::now();
        let mut count: u64 = 0;
        let mut nbytes: u64 = 0;
        for message in mcap::MessageStream::new(&data).unwrap() {
            let message = message.unwrap();
            count += 1;
            nbytes += message.data.len() as u64;
        }
        let wall = start.elapsed().as_secs_f64();
        println!("rust\tread\t{}\t{}\t{}\t0\t{:.6}", comp, count, nbytes, wall);
    }
}
