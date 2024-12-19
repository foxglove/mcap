use std::io::Read;

use mcap::sans_io::read::{LinearReader, ReadAction};

fn main() {
    let mut f = std::fs::File::open("tests/data/break_zstd_decompression.mcap")
        .expect("failed to open file");
    let blocksize: usize = 1024;
    let mut reader = LinearReader::new();
    while let Some(action) = reader.next_action() {
        match action.expect("failed to get next action") {
            ReadAction::GetRecord { data: _, opcode } => {
                print!("{},", opcode);
            }
            ReadAction::NeedMore(_) => {
                let read = f
                    .read(reader.insert(blocksize))
                    .expect("failed to read from file");
                reader.set_written(read);
            }
        }
    }
}
