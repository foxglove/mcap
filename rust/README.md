# Rust MCAP reading library

A library for parsing [MCAP](https://mcap.dev) files in Rust.

## Work in Progress!

This library is a work in progress, and currently only limited record reading is supported.
Check the [library support matrix](../docs/support-matrix.md) for a feature support comparison.

### Usage example

```rust
use mcap::records::Record;
use mcap::record_iterator::RecordIterator;

let mut file = std::fs::File::open("../tests/conformance/data/OneMessage/OneMessage.mcap")
    .expect("file not found");
for rec in RecordIterator::new(&mut file) {
    match rec {
        Ok(rec) => match rec {
            Record::Header(header) => println!("Found a header: {:?}", header),
            Record::Message(message) => println!("Found a message: {:?}", message),
            Record::Footer(_) => println!("Found the footer, expect no more records"),
            _ => println!("Found another record: {:?}", rec),
        },
        Err(err) => {
            eprintln!("failed to read next record: {}", err);
        }
    }
}
```
