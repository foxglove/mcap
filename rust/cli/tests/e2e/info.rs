use std::path::Path;

use assert_cmd::Command;
use walkdir::WalkDir;

#[tokio::test]
async fn test_info_passes_conformance_tests() {
    let test_data_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../tests/conformance/data/");

    for entry in WalkDir::new(test_data_dir) {
        let entry = entry.expect("failed to get walkdir entry");

        if !entry.file_name().to_string_lossy().ends_with(".mcap") {
            continue;
        }

        let mut cmd = Command::cargo_bin("cli").expect("failed to create command");

        cmd.args(["info", &entry.path().to_string_lossy()])
            .assert()
            .success();
    }
}
