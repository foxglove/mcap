use std::path::Path;
use walkdir::WalkDir;

use crate::mcap::{read_info, McapInfo};

/// Return an iterator of all the mcap files in tests/conformance/data/
pub fn conformance_mcap_files() -> impl Iterator<Item = String> {
    let test_data_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../tests/conformance/data/");

    let mut output = vec![];

    for entry in WalkDir::new(test_data_dir) {
        let entry = entry.expect("failed to get walkdir entry");

        if !entry.file_name().to_string_lossy().ends_with(".mcap") {
            continue;
        }

        output.push(entry.path().to_string_lossy().to_string());
    }

    output.into_iter()
}

pub fn demo_file() -> String {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../testdata/mcap/demo.mcap")
        .to_string_lossy()
        .to_string()
}

pub async fn read_info_from_file(file: impl AsRef<Path>) -> McapInfo {
    let file = tokio::fs::File::open(file)
        .await
        .expect("failed to open file for info");

    read_info(Box::pin(file))
        .await
        .expect("failed to read info for file")
}
