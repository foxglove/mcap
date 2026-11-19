#![no_main]

use libfuzzer_sys::fuzz_target;
use mcap::sans_io::{LinearReadEvent, LinearReader};

const MAX_INPUT_SIZE: usize = 1024 * 1024;
const MAX_READ_REQUEST: usize = 1024 * 1024;
const MAX_ITERATIONS: usize = 10_000;

fuzz_target!(|data: &[u8]| {
    if data.len() > MAX_INPUT_SIZE {
        return;
    }

    let mut reader = LinearReader::new();
    let mut offset = 0;
    let mut eof_sent = false;

    for _ in 0..MAX_ITERATIONS {
        let Some(event) = reader.next_event() else {
            break;
        };

        match event {
            Err(_) => break,
            Ok(LinearReadEvent::Record { .. }) => {}
            Ok(LinearReadEvent::ReadRequest(needed)) => {
                if needed == 0 || needed > MAX_READ_REQUEST {
                    break;
                }

                if offset == data.len() {
                    if eof_sent {
                        break;
                    }

                    reader.insert(needed);
                    reader.notify_read(0);
                    eof_sent = true;
                    continue;
                }

                let count = needed.min(data.len() - offset);

                reader.insert(needed)[..count].copy_from_slice(&data[offset..offset + count]);

                reader.notify_read(count);
                offset += count;
            }
        }
    }
});
