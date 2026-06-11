#[cfg(unix)]
use std::{
    os::{fd::OwnedFd, unix::net::UnixStream},
    process::{Command, Stdio},
};

#[cfg(unix)]
#[test]
fn fish_completion_allows_downstream_pipe_to_close() {
    let (read_end, write_end) = UnixStream::pair().expect("failed to create pipe");
    drop(read_end);

    let output = Command::new(env!("CARGO_BIN_EXE_mcap"))
        .args(["completion", "fish"])
        .stdout(Stdio::from(OwnedFd::from(write_end)))
        .output()
        .expect("failed to run mcap completion fish");

    assert!(
        output.status.success(),
        "completion should exit successfully after a downstream pipe closes; status: {:?}; stderr: {}",
        output.status.code(),
        String::from_utf8_lossy(&output.stderr)
    );
}
