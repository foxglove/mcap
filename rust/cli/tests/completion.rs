//! End-to-end tests for the `mcap completion` command.

use std::process::Command;

#[cfg(unix)]
use std::os::fd::OwnedFd;
#[cfg(unix)]
use std::os::unix::net::UnixStream;
#[cfg(unix)]
use std::process::Stdio;

#[test]
fn completion_outputs_a_script() {
    let output = Command::new(env!("CARGO_BIN_EXE_mcap"))
        .args(["completion", "bash"])
        .output()
        .expect("failed to run mcap completion bash");
    assert!(output.status.success(), "completion bash should succeed");
    assert!(!output.stdout.is_empty(), "completion should emit a script");
}

// Uses a raw closed-pipe stdout fd to reproduce a downstream reader exiting early (e.g.
// `mcap completion fish | head`); the captured-output helpers can't model a closed pipe.
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
