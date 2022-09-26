use mcap::records::Record;

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
        Value::Array(items) => {
            Value::Array(items.iter().map(|x| Value::String(x.to_string())).collect())
        }
    }
}

fn as_json(view: &Record<'_>) -> Value {
    let view_value = serde_json::to_value(view).unwrap();
    let (typename, field_object) = match view_value {
        serde_json::Value::Object(map) => map.into_iter().next().unwrap(),
        _ => panic!("expected a map"),
    };
    let field_array: Vec<(String, Value)> = match field_object {
        Value::Object(map) => map
            .into_iter()
            .map(|(field_name, field_value)| (field_name, transform_record_field(&field_value)))
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
    let file = std::fs::read(&args[1]).expect("file wouldn't open");
    let mut json_records: Vec<Value> = vec![];
    for rec in mcap::read::ChunkFlattener::new(&file).expect("Couldn't read file") {
        match rec {
            Ok(rec) => match rec {
                // Skip message indices
                Record::MessageIndex(_) => (),
                _ => {
                    json_records.push(as_json(&rec));
                }
            },
            Err(err) => {
                eprintln!("failed to read next record: {}", err);
                process::exit(1);
            }
        }
    }
    let out = json!({ "records": Value::Array(json_records) });
    print!("{}", serde_json::to_string_pretty(&out).unwrap());
}
