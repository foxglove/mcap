use assert_cmd::Command;

use super::common::conformance_mcap_files;

#[tokio::test]
async fn test_info_passes_conformance_tests() {
    for file in conformance_mcap_files() {
        let mut cmd = Command::cargo_bin("cli").expect("failed to create command");

        cmd.args(["info", &file]).assert().success();
    }
}
