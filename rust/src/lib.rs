#![doc = include_str!("../README.md")]
pub mod lexer;
pub mod parse;
pub mod record_iterator;
pub mod records;

#[cfg(test)]
mod tests {
    use crate::record_iterator::RecordIterator;
    use crate::records::*;

    use super::*;
    use records::Record;
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
        let expected: [Record; 3] = [
            Record::Header(Header {
                library: "".into(),
                profile: "".into(),
            }),
            Record::DataEnd(DataEnd {
                data_section_crc: 0,
            }),
            Record::Footer(Footer {
                summary_crc: 1875167664,
                summary_offset_start: 0,
                summary_start: 0,
            }),
        ];

        let mut count = 0;
        RecordIterator::new(&mcap_data[..])
            .zip(expected.iter())
            .for_each(|(actual, expected)| {
                assert_eq!(expected, &actual.expect("failed to parse"));
                count += 1;
            });
        assert_eq!(3, count);
    }
}
