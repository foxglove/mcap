use assert_matches::assert_matches;
use mcap::sans_io::{LinearReadEvent, LinearReader};

const SOURCE_FILE: &[u8] = include_bytes!("data/weird_bug.mcap");

#[test]
fn test_weird_bug_oneshot() {
    let mut reader = LinearReader::new();
    reader
        .insert(SOURCE_FILE.len())
        .copy_from_slice(SOURCE_FILE);
    reader.notify_read(SOURCE_FILE.len());
    while let Some(event) = reader.next_event() {
        match event.unwrap() {
            LinearReadEvent::ReadRequest(_) => {
                panic!("should not request read because file is complete")
            }
            LinearReadEvent::Record { .. } => {}
        }
    }
}

#[test]
fn test_weird_bug() {
    let mut data = SOURCE_FILE;
    let write_lengths = [50usize + 8185 + 16384 + 32768 + 65536 + 131072 + 59279, 0];
    assert_eq!(data.len(), write_lengths.iter().sum());

    let mut reader = LinearReader::new();
    for write_length in write_lengths {
        let chunk = &data[..write_length];
        if chunk.is_empty() {
            assert_eq!(data, &[]);
            reader.notify_read(0);
            assert_matches!(reader.next_event(), None);
        } else {
            reader.insert(chunk.len()).copy_from_slice(chunk);
            reader.notify_read(chunk.len());
            while let Some(event) = reader.next_event() {
                match event.unwrap() {
                    LinearReadEvent::ReadRequest(_) => break,
                    LinearReadEvent::Record { .. } => {}
                }
            }
        }
        data = &data[write_length..];
    }
    assert_eq!(data, &[]);
}
