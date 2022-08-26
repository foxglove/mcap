use mcap::lexer::LexError;
use mcap::lexer::Lexer;
use mcap::lexer::RawRecord;
use mcap::parse::Record;

use mcap::parse::parse_record;
use serde_json::{json, Value};
use std::env;
use std::process;

fn transform_record_field(value: &Value) -> Value {
    match value {
        Value::Bool(_) => panic!("did not expect any booleans in record fields"),
        Value::Null => panic!("did not expect any nulls in record fields"),
        Value::Number(n) => Value::String(n.to_string()),
        Value::String(_) => value.to_owned(),
        Value::Object(map) => Value::Object(
            map.into_iter()
                .map(|(k, v)| {
                    (
                        k.to_owned(),
                        match v {
                            Value::String(_) => v.to_owned(),
                            _ => Value::String(v.to_string()),
                        },
                    )
                })
                .collect(),
        ),
        Value::Array(items) => Value::Array(
            items
                .into_iter()
                .map(|x| Value::String(x.to_string()))
                .collect(),
        ),
    }
}

fn as_json<'a>(view: &Record<'a>) -> Value {
    let view_value = serde_json::to_value(view).unwrap();
    let (typename, field_object) = match view_value {
        serde_json::Value::Object(map) => map.into_iter().next().unwrap(),
        _ => panic!("expected a map"),
    };
    let field_array: Vec<(String, Value)> = match field_object {
        Value::Object(map) => map
            .into_iter()
            .map(|(field_name, field_value)| {
                (field_name.to_owned(), transform_record_field(&field_value))
            })
            .collect(),
        _ => panic!("expected fields to be a map"),
    };
    json!({"type": typename, "fields": field_array})
}

pub fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("Please supply an mcap file as argument");
        process::exit(1);
    }
    let mut file = std::fs::File::open(&args[1]).expect("file wouldn't open");
    let mut lexer = Lexer::new(&mut file, false);
    let mut decompressed_chunk: Vec<u8> = vec![];
    let mut maybe_chunk_lexer: Option<Lexer<std::io::Cursor<&mut [u8]>>> = None;
    let mut raw_record = RawRecord::new();
    let mut json_records: Vec<Value> = vec![];
    loop {
        if let Some(mut chunk_lexer) = maybe_chunk_lexer.take() {
            match chunk_lexer.read_next(&mut raw_record) {
                Ok(_) => match parse_record(raw_record.opcode.unwrap(), &raw_record.buf) {
                    Ok(view) => {
                        json_records.push(as_json(&view));
                        maybe_chunk_lexer = Some(chunk_lexer);
                    }
                    Err(err) => {
                        eprintln!("failed to parse {:?}: {}", raw_record.opcode, err);
                        process::exit(1);
                    }
                },
                Err(err) => match err {
                    LexError::TruncatedAfterRecord(_) => {
                        maybe_chunk_lexer = None;
                        continue;
                    }
                    _ => {}
                },
            }
            continue;
        };
        match lexer.read_next(&mut raw_record) {
            Ok(more) => {
                match parse_record(raw_record.opcode.unwrap(), &raw_record.buf) {
                    Ok(view) => match view {
                        Record::Chunk {
                            compression: "lz4",
                            uncompressed_size: u,
                            records: r,
                            ..
                        } => {
                            decompressed_chunk.resize(u as usize, 0);
                            let res = lz4_flex::decompress_into(r, &mut decompressed_chunk[..]);
                            if let Err(err) = res {
                                eprintln!("failed to decompress: {}", err);
                                process::exit(1);
                            }
                            maybe_chunk_lexer = Some(Lexer::new(
                                std::io::Cursor::new(&mut decompressed_chunk),
                                true,
                            ));
                        }
                        Record::Chunk {
                            compression: "",
                            records: r,
                            ..
                        } => {
                            decompressed_chunk.resize(r.len(), 0);
                            decompressed_chunk.copy_from_slice(r);
                            maybe_chunk_lexer = Some(Lexer::new(
                                std::io::Cursor::new(&mut decompressed_chunk),
                                true,
                            ));
                        }
                        // TODO: why do we not emit MessageIndex records for conformance tests?
                        Record::MessageIndex { .. } => {}
                        _ => json_records.push(as_json(&view)),
                    },
                    Err(err) => {
                        eprintln!("failed to parse {:?}: {}", raw_record.opcode, err);
                        process::exit(1);
                    }
                }
                if !more {
                    break;
                }
            }
            Err(err) => {
                eprintln!("lexer failed at {}", err);
                process::exit(1);
            }
        };
    }
    let out = json!({ "records": Value::Array(json_records) });
    print!("{}", serde_json::to_string_pretty(&out).unwrap());
}
