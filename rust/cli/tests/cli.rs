//! End-to-end tests that run the built `mcap` binary as a real process.
//!
//! These cover behavior the in-process unit tests in `src/` cannot reach: real process
//! exit codes (including clap's argument-error code), reading a non-seekable stdin pipe,
//! and writing real output files. Per-command logic stays covered by the unit tests;
//! this file stays at the process boundary.

use std::collections::BTreeMap;
use std::io::{Cursor, Write};
use std::path::Path;
use std::process::{Command, Output, Stdio};

#[cfg(unix)]
use std::os::fd::OwnedFd;
#[cfg(unix)]
use std::os::unix::net::UnixStream;

use mcap::records::MessageHeader;
use tempfile::TempDir;

/// Build a small chunked, indexed MCAP in memory with `num_messages` messages on `/example`.
fn build_mcap(num_messages: u64) -> Vec<u8> {
    build_mcap_with_options(true, &(0..num_messages).map(|i| i + 1).collect::<Vec<_>>())
}

fn build_mcap_with_options(use_chunks: bool, message_log_times: &[u64]) -> Vec<u8> {
    let mut buffer = Vec::new();
    {
        let mut writer = mcap::WriteOptions::new()
            .use_chunks(use_chunks)
            // Small chunks so even a modest message count spans multiple chunks; this makes a
            // mid-file truncation land inside chunk data (needed for the lossy-recover test).
            .chunk_size(Some(256))
            .create(Cursor::new(&mut buffer))
            .expect("create writer");
        let schema_id = writer
            .add_schema("Example", "jsonschema", br#"{"type":"object"}"#)
            .expect("add schema");
        let channel_id = writer
            .add_channel(schema_id, "/example", "json", &BTreeMap::new())
            .expect("add channel");
        for (i, log_time) in message_log_times.iter().copied().enumerate() {
            writer
                .write_to_known_channel(
                    &MessageHeader {
                        channel_id,
                        sequence: i as u32,
                        log_time,
                        publish_time: log_time,
                    },
                    format!("{{\"n\":{i}}}").as_bytes(),
                )
                .expect("write message");
        }
        writer.finish().expect("finish writer");
    }
    buffer
}

/// Build a single-topic, `json`-encoded MCAP with caller-supplied JSON payloads.
///
/// Lets a test control each message's exact shape so it can produce both
/// stable-shape and variable-shape inputs for CSV export.
fn build_single_topic_json_mcap(topic: &str, messages: &[(u32, u64, &[u8])]) -> Vec<u8> {
    let mut buffer = Vec::new();
    {
        let mut writer = mcap::WriteOptions::new()
            .chunk_size(Some(1024))
            .create(Cursor::new(&mut buffer))
            .expect("create writer");
        let schema_id = writer
            .add_schema("Example", "jsonschema", br#"{"type":"object"}"#)
            .expect("add schema");
        let channel_id = writer
            .add_channel(schema_id, topic, "json", &BTreeMap::new())
            .expect("add channel");
        for (sequence, log_time, data) in messages {
            writer
                .write_to_known_channel(
                    &MessageHeader {
                        channel_id,
                        sequence: *sequence,
                        log_time: *log_time,
                        publish_time: *log_time,
                    },
                    data,
                )
                .expect("write message");
        }
        writer.finish().expect("finish writer");
    }
    buffer
}

/// Run the `mcap` binary with `args` and capture its output.
fn mcap(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_mcap"))
        .args(args)
        .output()
        .expect("failed to run mcap")
}

/// Run the `mcap` binary with `args`, feeding `stdin` through a (non-seekable) pipe.
///
/// Writes the whole payload before draining stdout, so it must only be used with small inputs:
/// a payload large enough to fill the OS pipe buffer while the child is blocked
/// writing stdout would deadlock. All current callers pipe a few hundred bytes.
fn mcap_with_stdin(args: &[&str], stdin: &[u8]) -> Output {
    let mut child = Command::new(env!("CARGO_BIN_EXE_mcap"))
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("failed to spawn mcap");
    child
        .stdin
        .take()
        .expect("child stdin")
        .write_all(stdin)
        .expect("failed to write stdin");
    child.wait_with_output().expect("failed to wait for mcap")
}

fn looks_like_mcap(bytes: &[u8]) -> bool {
    bytes.starts_with(mcap::MAGIC) && bytes.ends_with(mcap::MAGIC)
}

fn write_temp(dir: &TempDir, name: &str, bytes: &[u8]) -> std::path::PathBuf {
    let path = dir.path().join(name);
    std::fs::write(&path, bytes).expect("write temp file");
    path
}

fn path_str(path: &Path) -> &str {
    path.to_str().expect("temp path should be valid UTF-8")
}

fn stdout(output: &Output) -> std::borrow::Cow<'_, str> {
    String::from_utf8_lossy(&output.stdout)
}

#[test]
fn exit_code_0_on_valid_file() {
    let dir = TempDir::new().unwrap();
    let path = write_temp(&dir, "in.mcap", &build_mcap(3));
    let output = mcap(&["info", path_str(&path)]);
    assert!(output.status.success());
    assert!(stdout(&output).contains("messages:"));
}

#[test]
fn exit_code_2_on_missing_required_flag() {
    let dir = TempDir::new().unwrap();
    let path = write_temp(&dir, "in.mcap", &build_mcap(1));
    // `get metadata` requires `-n/--name`; clap reports the missing argument with exit code 2.
    assert_eq!(
        mcap(&["get", "metadata", path_str(&path)]).status.code(),
        Some(2)
    );
}

#[test]
fn exit_code_2_on_unknown_global_flag() {
    let dir = TempDir::new().unwrap();
    let path = write_temp(&dir, "in.mcap", &build_mcap(1));
    // Unknown global flags are rejected before command dispatch.
    let output = mcap(&["--not-a-real-flag", "info", path_str(&path)]);
    assert_eq!(output.status.code(), Some(2));
}

#[test]
fn exit_code_2_on_version_subcommand() {
    // `version` is not a subcommand; use the global `--version` flag instead.
    assert_eq!(mcap(&["version"]).status.code(), Some(2));
    assert!(mcap(&["--version"]).status.success());
}

#[test]
fn exit_code_3_on_lossy_recover() {
    let dir = TempDir::new().unwrap();
    let full = build_mcap(20);
    // Truncate mid-file so chunk data is cut off: some messages are lost, which recover signals
    // with exit code 3 while still producing a valid output file.
    let input = write_temp(&dir, "truncated.mcap", &full[..full.len() / 2]);
    let output_path = dir.path().join("recovered.mcap");
    let output = mcap(&["recover", path_str(&input), "-o", path_str(&output_path)]);
    assert_eq!(output.status.code(), Some(3));
    let recovered = std::fs::read(&output_path).expect("recover should write output");
    assert!(
        looks_like_mcap(&recovered),
        "recovered file should be valid MCAP"
    );
}

#[test]
fn exit_code_0_on_cat_csv_stable_shape() {
    let dir = TempDir::new().unwrap();
    let path = write_temp(
        &dir,
        "stable.mcap",
        &build_single_topic_json_mcap(
            "/example",
            &[(1, 10, br#"{"a":1,"b":2}"#), (2, 20, br#"{"a":3,"b":4}"#)],
        ),
    );
    let output = mcap(&[
        "cat",
        path_str(&path),
        "--format=csv",
        "--topics",
        "/example",
    ]);
    assert!(
        output.status.success(),
        "stable-shape csv cat should exit 0; stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    // Cell layout is covered by unit tests; here just confirm the process wrote a CSV header.
    assert!(
        stdout(&output).starts_with("log_time,publish_time,sequence,a,b\n"),
        "unexpected csv stdout: {}",
        stdout(&output)
    );
}

#[test]
fn exit_code_3_on_cat_csv_dropped_columns() {
    let dir = TempDir::new().unwrap();
    let path = write_temp(
        &dir,
        "variable.mcap",
        &build_single_topic_json_mcap(
            "/example",
            &[(1, 10, br#"{"a":1}"#), (2, 20, br#"{"a":2,"b":3}"#)],
        ),
    );
    let output = mcap(&[
        "cat",
        path_str(&path),
        "--format=csv",
        "--topics",
        "/example",
    ]);
    // A later message with extra fields is data-loss: exit 3 with a warning, but stdout
    // is still a valid CSV using the first message's header.
    assert_eq!(output.status.code(), Some(3));
    assert!(
        stdout(&output).starts_with("log_time,publish_time,sequence,a\n"),
        "unexpected csv stdout: {}",
        stdout(&output)
    );
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("extra columns are dropped"),
        "stderr should warn about dropped columns; stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn exit_code_1_on_cat_csv_unknown_topic() {
    let dir = TempDir::new().unwrap();
    let path = write_temp(
        &dir,
        "one_topic.mcap",
        &build_single_topic_json_mcap("/example", &[(1, 10, br#"{"a":1}"#)]),
    );
    // A topic that isn't in the file is almost always a typo, so it's a hard error rather than a
    // silently empty export.
    let output = mcap(&["cat", path_str(&path), "--format=csv", "--topics", "/nope"]);
    assert_eq!(output.status.code(), Some(1));
    assert!(stdout(&output).is_empty(), "stdout: {}", stdout(&output));
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("topic '/nope' not found"),
        "stderr should report the unknown topic; stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn cat_csv_warns_when_existing_topic_has_no_messages() {
    let dir = TempDir::new().unwrap();
    let path = write_temp(
        &dir,
        "one_topic.mcap",
        &build_single_topic_json_mcap("/example", &[(1, 10, br#"{"a":1}"#)]),
    );
    // The topic exists but the time range excludes its only message: warn, but exit 0.
    let output = mcap(&[
        "cat",
        path_str(&path),
        "--format=csv",
        "--topics",
        "/example",
        "--start-secs",
        "9999",
    ]);
    assert!(
        output.status.success(),
        "an existing but empty topic should exit 0; stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(stdout(&output).is_empty(), "stdout: {}", stdout(&output));
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("has no messages to export"),
        "stderr should warn about the empty topic; stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn exit_code_doctor_non_strict_allows_out_of_order_top_level_messages() {
    let dir = TempDir::new().unwrap();
    let path = write_temp(
        &dir,
        "out_of_order.mcap",
        &build_mcap_with_options(false, &[2, 1]),
    );

    let output = mcap(&["doctor", path_str(&path)]);
    assert!(
        output.status.success(),
        "non-strict doctor should warn but exit 0; stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stderr).contains("Warning: Message.log_time"));

    let output = mcap(&["doctor", "--strict-message-order", path_str(&path)]);
    assert_eq!(output.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&output.stderr).contains("Error: Message.log_time"));
}

// Reading from a non-seekable stdin pipe is only reachable end-to-end; the unit tests use
// seekable in-memory buffers.
#[test]
fn stdin_pipe_cat() {
    let output = mcap_with_stdin(&["cat"], &build_mcap(3));
    assert!(output.status.success());
    assert!(stdout(&output).contains("/example"));
}

#[test]
fn stdin_pipe_cat_csv_errors_on_unknown_topic() {
    // A stdin pipe has no summary, so channel existence is learned from Channel records during the
    // scan; an unknown topic is still a hard error rather than a silently empty export.
    let output = mcap_with_stdin(
        &["cat", "--format=csv", "--topics", "/nope"],
        &build_mcap(3),
    );
    assert_eq!(output.status.code(), Some(1));
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("topic '/nope' not found"),
        "stderr should report the unknown topic; stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn stdin_pipe_filter() {
    let dir = TempDir::new().unwrap();
    let out_path = dir.path().join("filtered.mcap");
    let output = mcap_with_stdin(
        &["filter", "-o", path_str(&out_path), "--compression", "none"],
        &build_mcap(3),
    );
    assert!(output.status.success());
    assert!(looks_like_mcap(
        &std::fs::read(&out_path).expect("filter output")
    ));
}

#[test]
fn stdin_pipe_compress() {
    let dir = TempDir::new().unwrap();
    let out_path = dir.path().join("compressed.mcap");
    let output = mcap_with_stdin(
        &[
            "compress",
            "-o",
            path_str(&out_path),
            "--compression",
            "zstd",
        ],
        &build_mcap(3),
    );
    assert!(output.status.success());
    assert!(looks_like_mcap(
        &std::fs::read(&out_path).expect("compress output")
    ));
}

#[test]
fn stdin_pipe_decompress() {
    let dir = TempDir::new().unwrap();
    let out_path = dir.path().join("decompressed.mcap");
    let output = mcap_with_stdin(&["decompress", "-o", path_str(&out_path)], &build_mcap(3));
    assert!(output.status.success());
    assert!(looks_like_mcap(
        &std::fs::read(&out_path).expect("decompress output")
    ));
}

#[test]
fn completion_outputs_a_script() {
    let output = mcap(&["completion", "bash"]);
    assert!(output.status.success(), "completion bash should succeed");
    assert!(!output.stdout.is_empty(), "completion should emit a script");
}

// Uses a raw closed-pipe stdout fd to reproduce a downstream reader exiting early (e.g.
// `mcap completion fish | head`); the captured-output helpers can't model a closed pipe.
#[cfg(unix)]
#[test]
fn completion_survives_closed_downstream_pipe() {
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

// Same closed-pipe pattern for CSV: write_record / final flush must treat BrokenPipe as success
// (e.g. `mcap cat --format=csv … | head`), not exit 1 via csv::Error.
#[cfg(unix)]
#[test]
fn cat_csv_survives_closed_downstream_pipe() {
    let dir = TempDir::new().unwrap();
    // Enough rows to fill csv::Writer's ~8 KiB buffer so EPIPE can surface mid-write, not only
    // on the final flush.
    let path = write_temp(&dir, "pipe.mcap", &build_mcap(500));
    let (read_end, write_end) = UnixStream::pair().expect("failed to create pipe");
    drop(read_end);

    let output = Command::new(env!("CARGO_BIN_EXE_mcap"))
        .args([
            "cat",
            path_str(&path),
            "--format=csv",
            "--topics",
            "/example",
        ])
        .stdout(Stdio::from(OwnedFd::from(write_end)))
        .output()
        .expect("failed to run mcap cat --format=csv");

    assert!(
        output.status.success(),
        "csv cat should exit successfully after a downstream pipe closes; status: {:?}; stderr: {}",
        output.status.code(),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn help_lists_commands() {
    let output = mcap(&["--help"]);
    assert!(output.status.success());
    let stdout = stdout(&output);
    for command in ["convert", "recover", "completion"] {
        assert!(stdout.contains(command), "help should list `{command}`");
    }
}
