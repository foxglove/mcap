use std::process::{Command, Stdio};

#[test]
fn fish_completion_allows_downstream_pipe_to_close() {
    let mut child = Command::new(env!("CARGO_BIN_EXE_mcap"))
        .args(["completion", "fish"])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("failed to run mcap completion fish");

    drop(child.stdout.take());

    let output = child
        .wait_with_output()
        .expect("failed to wait for mcap completion fish");
    assert!(
        output.status.success(),
        "completion should exit successfully after a downstream pipe closes; status: {:?}; stderr: {}",
        output.status.code(),
        String::from_utf8_lossy(&output.stderr)
    );
}
