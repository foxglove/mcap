use assert_cmd::Command;
use predicates::prelude::*;
use std::path::PathBuf;
use tempfile::NamedTempFile;

fn mcap_test_file() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("testdata/mcap/demo.mcap")
}

#[test]
fn test_version_command() {
    let mut cmd = Command::cargo_bin("mcap-rs").unwrap();
    cmd.arg("version")
        .assert()
        .success()
        .stdout(predicate::str::contains("0.1.0"));
}

#[test]
fn test_help_command() {
    let mut cmd = Command::cargo_bin("mcap-rs").unwrap();
    cmd.arg("--help")
        .assert()
        .success()
        .stdout(predicate::str::contains("mcap"));
}

#[test]
fn test_info_command_with_nonexistent_file() {
    let mut cmd = Command::cargo_bin("mcap-rs").unwrap();
    cmd.arg("info").arg("nonexistent.mcap").assert().failure();
}

#[test]
fn test_info_command_with_demo_file() {
    let test_file = mcap_test_file();
    if !test_file.exists() {
        // Skip test if demo file doesn't exist
        return;
    }

    let mut cmd = Command::cargo_bin("mcap-rs").unwrap();
    cmd.arg("info").arg(&test_file).assert().success();
}

#[test]
fn test_list_channels_command() {
    let test_file = mcap_test_file();
    if !test_file.exists() {
        return;
    }

    let mut cmd = Command::cargo_bin("mcap-rs").unwrap();
    cmd.arg("list")
        .arg("channels")
        .arg(&test_file)
        .assert()
        .success();
}

#[test]
fn test_list_chunks_command() {
    let test_file = mcap_test_file();
    if !test_file.exists() {
        return;
    }

    let mut cmd = Command::cargo_bin("mcap-rs").unwrap();
    cmd.arg("list")
        .arg("chunks")
        .arg(&test_file)
        .assert()
        .success();
}

#[test]
fn test_doctor_command() {
    let test_file = mcap_test_file();
    if !test_file.exists() {
        return;
    }

    let mut cmd = Command::cargo_bin("mcap-rs").unwrap();
    cmd.arg("doctor").arg(&test_file).assert().success();
}
