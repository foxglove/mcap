use mcap::{
    sans_io::{LinearReadEvent, LinearReader},
    McapResult,
};

#[test]
fn test_weird_bug() -> McapResult<()> {
    let mut data: &[u8] = include_bytes!("data/weird_bug.mcap");
    let mut reader = LinearReader::new();
    let write_lengths = [50usize, 8185, 16384, 32768, 65536, 131072, 59279, 0];
    assert_eq!(data.len(), write_lengths.iter().sum());
    for write_length in write_lengths {
        let chunk = &data[..write_length];
        reader.insert(chunk.len()).copy_from_slice(chunk);
        reader.notify_read(chunk.len());
        while let Some(event) = reader.next_event().transpose()? {
            match event {
                LinearReadEvent::ReadRequest(_) => break,
                LinearReadEvent::Record { .. } => {}
            }
        }
        data = &data[write_length..];
    }
    assert_eq!(data, &[]);
    Ok(())
}
