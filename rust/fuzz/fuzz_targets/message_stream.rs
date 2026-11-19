#![no_main]

use libfuzzer_sys::fuzz_target;
use mcap::read::MessageStream;

const MAX_INPUT_SIZE: usize = 1024 * 1024;

fuzz_target!(|data: &[u8]| {
    // Keep individual inputs bounded to limit memory usage.
    if data.len() > MAX_INPUT_SIZE {
        return;
    }

    // Invalid MCAP data is expected; the target should handle errors safely.
    if let Ok(messages) = MessageStream::new(data) {
        for message in messages {
            // Parsing errors are valid outcomes for arbitrary input.
            if message.is_err() {
                break;
            }
        }
    }
});
