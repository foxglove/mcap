#[path = "common/conformance_writer.rs"]
mod conformance_writer;

use std::env;

use conformance_writer::write_spec;

pub fn main() {
    let args: Vec<String> = env::args().collect();
    let input_text =
        std::fs::read_to_string(&args[1]).expect("Should have been able to read the file");

    let spec = serde_json::from_str(&input_text).expect("Invalid json");

    let writer = mcap::WriteOptions::new()
        .use_buffered_chunks(true)
        .compression(None)
        .profile("")
        .create(mcap::write::NoSeek::new(std::io::stdout()))
        .expect("Couldn't create writer");

    write_spec(writer, &spec);
}
