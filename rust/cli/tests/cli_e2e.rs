//! End-to-end tests that exercise the built `mcap` binary as a real process.
//!
//! These cover behavior that the in-process unit tests cannot reach: actual process
//! exit codes (including clap's argument-error code), reading a non-seekable stdin
//! pipe, and writing real output files. The per-command logic is covered by the unit
//! tests in `src/`; this file deliberately stays at the process boundary.

use std::collections::BTreeMap;
use std::io::Cursor;

use assert_cmd::Command;
use mcap::records::MessageHeader;
use predicates::prelude::*;
use tempfile::TempDir;

/// Build a small chunked, indexed MCAP in memory with `num_messages` messages on `/example`.
fn build_mcap(num_messages: u64) -> Vec<u8> {
    let mut buffer = Vec::new();
    {
        let mut writer = mcap::WriteOptions::new()
            .use_chunks(true)
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
        for i in 0..num_messages {
            writer
                .write_to_known_channel(
                    &MessageHeader {
                        channel_id,
                        sequence: i as u32,
                        log_time: i + 1,
                        publish_time: i + 1,
                    },
                    format!("{{\"n\":{i}}}").as_bytes(),
                )
                .expect("write message");
        }
        writer.finish().expect("finish writer");
    }
    buffer
}

fn mcap_cmd() -> Command {
    Command::cargo_bin("mcap").expect("mcap binary should build")
}

fn looks_like_mcap(bytes: &[u8]) -> bool {
    bytes.starts_with(mcap::MAGIC) && bytes.ends_with(mcap::MAGIC)
}

fn write_temp(dir: &TempDir, name: &str, bytes: &[u8]) -> std::path::PathBuf {
    let path = dir.path().join(name);
    std::fs::write(&path, bytes).expect("write temp file");
    path
}

// ---------------------------------------------------------------------------
// Exit codes (process-level; not covered by the in-process unit tests)
// ---------------------------------------------------------------------------

#[test]
fn info_on_valid_file_exits_zero() {
    let dir = TempDir::new().unwrap();
    let path = write_temp(&dir, "in.mcap", &build_mcap(3));
    mcap_cmd()
        .arg("info")
        .arg(&path)
        .assert()
        .success()
        .stdout(predicate::str::contains("messages:"));
}

#[test]
fn missing_required_flag_exits_2() {
    let dir = TempDir::new().unwrap();
    let path = write_temp(&dir, "in.mcap", &build_mcap(1));
    // `sort` requires `--output-file`; clap reports the missing argument with exit code 2.
    mcap_cmd().arg("sort").arg(&path).assert().code(2);
}

#[test]
fn unknown_global_flag_exits_2() {
    let dir = TempDir::new().unwrap();
    let path = write_temp(&dir, "in.mcap", &build_mcap(1));
    // `--config` was a Go-only global flag; the Rust CLI rejects it as an unknown argument.
    mcap_cmd()
        .args(["--config", "x.yaml", "info"])
        .arg(&path)
        .assert()
        .code(2);
}

#[test]
fn version_subcommand_is_rejected() {
    // The Go CLI had a `version` subcommand; the Rust CLI exposes `--version` instead, so the
    // subcommand is an unrecognized argument (exit 2).
    mcap_cmd().arg("version").assert().code(2);
    mcap_cmd().arg("--version").assert().success();
}

#[test]
fn recover_truncated_file_exits_3_and_writes_output() {
    let dir = TempDir::new().unwrap();
    let full = build_mcap(20);
    // Truncate mid-file so chunk data is cut off: some messages are lost, which recover signals
    // with exit code 3 while still producing a valid output file.
    let truncated = &full[..full.len() / 2];
    let input = write_temp(&dir, "truncated.mcap", truncated);
    let output = dir.path().join("recovered.mcap");

    mcap_cmd()
        .arg("recover")
        .arg(&input)
        .arg("-o")
        .arg(&output)
        .assert()
        .code(3);

    let recovered = std::fs::read(&output).expect("recover should write output");
    assert!(
        looks_like_mcap(&recovered),
        "recovered file should be valid MCAP"
    );
}

// ---------------------------------------------------------------------------
// Non-seekable stdin pipes (unit tests only use seekable in-memory buffers)
// ---------------------------------------------------------------------------

#[test]
fn cat_reads_stdin_pipe() {
    mcap_cmd()
        .arg("cat")
        .write_stdin(build_mcap(3))
        .assert()
        .success()
        .stdout(predicate::str::contains("/example"));
}

#[test]
fn filter_reads_stdin_pipe() {
    let dir = TempDir::new().unwrap();
    let output = dir.path().join("filtered.mcap");
    mcap_cmd()
        .args(["filter", "-o"])
        .arg(&output)
        .args(["--output-compression", "none"])
        .write_stdin(build_mcap(3))
        .assert()
        .success();
    let out = std::fs::read(&output).expect("filter should write output");
    assert!(looks_like_mcap(&out));
}

#[test]
fn compress_reads_stdin_pipe() {
    let dir = TempDir::new().unwrap();
    let output = dir.path().join("compressed.mcap");
    mcap_cmd()
        .args(["compress", "-o"])
        .arg(&output)
        .args(["--compression", "zstd"])
        .write_stdin(build_mcap(3))
        .assert()
        .success();
    assert!(looks_like_mcap(
        &std::fs::read(&output).expect("compress output")
    ));
}

#[test]
fn decompress_reads_stdin_pipe() {
    let dir = TempDir::new().unwrap();
    let output = dir.path().join("decompressed.mcap");
    mcap_cmd()
        .args(["decompress", "-o"])
        .arg(&output)
        .write_stdin(build_mcap(3))
        .assert()
        .success();
    assert!(looks_like_mcap(
        &std::fs::read(&output).expect("decompress output")
    ));
}

// ---------------------------------------------------------------------------
// Surface smoke tests
// ---------------------------------------------------------------------------

#[test]
fn help_lists_commands() {
    mcap_cmd()
        .arg("--help")
        .assert()
        .success()
        .stdout(predicate::str::contains("convert"))
        .stdout(predicate::str::contains("recover"))
        .stdout(predicate::str::contains("completion"));
}

#[test]
fn completion_generates_a_script() {
    mcap_cmd()
        .args(["completion", "bash"])
        .assert()
        .success()
        .stdout(predicate::str::is_empty().not());
}
