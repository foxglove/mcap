use std::{env, io::BufWriter};

#[path = "common/conformance_writer.rs"]
mod conformance_writer;

use conformance_writer::write_spec;

pub fn main() {
    let args: Vec<String> = env::args().collect();
    let input_text =
        std::fs::read_to_string(&args[1]).expect("Should have been able to read the file");

    let spec = serde_json::from_str(&input_text).expect("Invalid json");

    let mut tmp = tempfile::NamedTempFile::new().expect("Couldn't open file");

    let writer = mcap::WriteOptions::new()
        .compression(None)
        .profile("")
        .create(BufWriter::new(&mut tmp))
        .expect("Couldn't create writer");

    write_spec(writer, &spec);

    std::io::copy(
        &mut std::fs::File::open(tmp.path()).expect("failed to open tmp file"),
        &mut std::io::stdout(),
    )
    .expect("failed to copy to stdout");
}
