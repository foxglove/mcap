#![no_main]

use libfuzzer_sys::fuzz_target;
use mcap::read::Summary;

const MAX_INPUT_SIZE: usize = 1024 * 1024;

fuzz_target!(|data: &[u8]| {
    if data.len() > MAX_INPUT_SIZE {
        return;
    }

    let _ = Summary::read(data);
});
