#[path = "common/serialization.rs"]
mod serialization;

use serde_json::{json, Value};

use mcap::records::Record;
use std::env;
use std::process;

pub fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("Please supply an MCAP file as argument");
        process::exit(1);
    }
    let file = std::fs::read(&args[1]).expect("file wouldn't open");
    let mut json_records: Vec<Value> = vec![];
    for rec in mcap::read::ChunkFlattener::new(&file).expect("Couldn't read file") {
        let r = rec.expect("failed to read next record");
        if !matches!(r, Record::MessageIndex(_)) {
            json_records.push(serialization::as_json(&r));
        }
    }
    let out = json!({ "records": json_records });
    print!("{}", serde_json::to_string_pretty(&out).unwrap());
}
