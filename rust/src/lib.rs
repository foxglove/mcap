mod conformance;
pub mod lexer;
pub mod parse;

#[cfg(test)]
mod tests {
    use crate::parse::parse_record;

    use super::*;
    use parse::RecordContentView;
    use std::io::Read;
    use std::path::PathBuf;

    fn test_asset_path(name: &'static str) -> PathBuf {
        let pkg_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        if let Some(repo_root) = pkg_path.parent() {
            let mut asset_path: PathBuf = repo_root.to_owned();
            asset_path.push("tests/conformance/data");
            asset_path.push(name);
            return asset_path;
        }
        panic!("expected CARGO_MANIFEST_DIR to be a real path with a parent")
    }

    fn read_test_asset(name: &'static str) -> Vec<u8> {
        let path = test_asset_path(name);
        let mut file = std::fs::File::open(path).unwrap();
        let mut out: Vec<u8> = Vec::new();
        file.read_to_end(&mut out).unwrap();
        return out;
    }

    #[test]
    fn no_data_read() {
        let mcap_data = read_test_asset("NoData/NoData.mcap");
        let expected: [RecordContentView; 3] = [
            RecordContentView::Header {
                library: "",
                profile: "",
            },
            RecordContentView::DataEnd {
                data_section_crc: 0,
            },
            RecordContentView::Footer {
                crc: 1875167664,
                summary_offset_start: 0,
                summary_start: 0,
            },
        ];
        let mut i: usize = 0;
        let mut raw_record = lexer::RawRecord::new();
        let mut lexer = lexer::Lexer::new(std::io::Cursor::new(mcap_data), false);
        loop {
            match lexer.read(&mut raw_record) {
                Ok(more) => {
                    match parse_record(raw_record.opcode.unwrap(), &raw_record.buf[..]) {
                        Ok(view) => {
                            assert_eq!(view, expected[i]);
                        }
                        Err(err) => {
                            assert!(false, "MCAP failed to parse on record {}: {}", i, err);
                        }
                    }
                    i += 1;
                    if !more {
                        break;
                    }
                }
                Err(err) => {
                    assert!(
                        false,
                        "Mcap failed to lex on expected record {}: {}",
                        i, err
                    );
                }
            };
        }
        assert_eq!(i, 3);
    }

    #[test]
    fn it_works() {
        let result = parse::OpCode::Header;
        assert_eq!(format!("{:?}", result), "Header");
    }
}
