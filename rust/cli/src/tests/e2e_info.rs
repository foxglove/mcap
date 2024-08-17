use assert_cmd::Command;
use predicates::str::diff;

use crate::tests::common::demo_file;

use super::common::conformance_mcap_files;

#[tokio::test]
async fn test_info_passes_conformance_tests() {
    for file in conformance_mcap_files() {
        let mut cmd = Command::cargo_bin("cli").expect("failed to create command");

        cmd.args(["info", &file]).assert().success();
    }
}

#[tokio::test]
async fn test_info_demo_file_snapshot() {
    let file = demo_file();

    let mut cmd = Command::cargo_bin("cli").expect("failed to create command");

    cmd.args(["info", &file])
        .assert()
        .success()
        // re-create this with `cargo run -p cli -- info <demo file>`
        .stdout(diff(include_str!("./data/e2e_info_demo_file.stdout")));
}
