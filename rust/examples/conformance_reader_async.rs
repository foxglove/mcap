#[path = "common/serialization.rs"]
mod serialization;

use serde_json::{json, Value};

use serialization::as_json;
use std::env;
use std::process;
use tokio::fs::File;

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("Please supply an MCAP file as argument");
        process::exit(1);
    }
    let file = File::open(&args[1]).await.expect("couldn't open file");
    let mut reader = mcap::tokio::RecordReader::new(file);

    let mut json_records: Vec<Value> = vec![];
    let mut buf: Vec<u8> = Vec::new();
    while let Some(opcode) = reader.next_record(&mut buf).await {
        let opcode = opcode.expect("failed to read next record");
        if opcode != mcap::records::op::MESSAGE_INDEX {
            let parsed = mcap::parse_record(opcode, &buf[..]).expect("failed to parse record");
            json_records.push(as_json(&parsed));
        }
    }
    let out = json!({ "records": json_records });
    print!("{}", serde_json::to_string_pretty(&out).unwrap());
}
