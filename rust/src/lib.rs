pub mod io;
pub mod lexer;
pub mod records;

#[cfg(test)]
mod tests {
    use super::*;
    use records::RecordContentView;
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
        let mut reader = io::BufReader::new(&mcap_data);
        let mut lexer = lexer::Lexer::new(&mut reader);
        while let Some(lex_result) = lexer.next() {
            assert!(
                lex_result.is_ok(),
                "MCAP failed to lex on expected record {}: {}",
                i,
                lex_result.err().unwrap()
            );
            let (opcode, record_buf) = lex_result.unwrap();
            let parse_result = records::parse_record(opcode, record_buf);
            assert!(
                parse_result.is_ok(),
                "Could not parse expected record {}: {}",
                i,
                parse_result.err().unwrap()
            );
            assert_eq!(parse_result.unwrap(), expected[i]);
            i += 1;
        }
        assert_eq!(i, 3);
    }

    #[test]
    fn it_works() {
        let result = records::OpCode::Header;
        assert_eq!(format!("{:?}", result), "Header");
    }
}
