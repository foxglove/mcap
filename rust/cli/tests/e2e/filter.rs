use assert_cmd::{assert::Assert, Command};

#[tokio::test]
async fn test_filter_create_new_file() {
    let mut cmd = Command::cargo_bin("cli").expect("failed to create command");

    cmd.args(["filter"]);
}
