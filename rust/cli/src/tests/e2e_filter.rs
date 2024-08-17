use assert_cmd::Command;
use tempfile::NamedTempFile;

use super::common::{conformance_mcap_files, demo_file, read_info_from_file};

// Times extracted from the demo file with `mcap info`
const DEMO_START_TIME_NSECS: u64 = 1490149580103843113;
const DEMO_END_TIME_NSECS: u64 = 1490149587884601617;

#[tokio::test]
async fn test_filter_passes_conformance_tests() {
    for file in conformance_mcap_files() {
        let mut cmd = Command::cargo_bin("cli").expect("failed to create command");

        let tmp = NamedTempFile::new()
            .expect("failed to create temp file")
            .into_temp_path();

        println!(
            "$ mcap filter {file} -o {}",
            tmp.as_os_str().to_string_lossy()
        );

        cmd.args(["filter", &file, "-o", &tmp.as_os_str().to_string_lossy()])
            .assert()
            .success();
    }
}

#[tokio::test]
async fn test_filter_include_single_channel() {
    let file = demo_file();

    let mut cmd = Command::cargo_bin("cli").expect("failed to create command");

    let tmp = NamedTempFile::new()
        .expect("failed to create temp file")
        .into_temp_path();

    let out = tmp.as_os_str().to_string_lossy().to_string();

    cmd.args([
        "filter",
        &file,
        "-o",
        &out,
        "--include-topic-regex",
        r"/diagnostics",
    ])
    .assert()
    .success();

    let info = read_info_from_file(&out).await;

    assert_eq!(info.channels.len(), 1);
    assert_eq!(info.channels[0].topic, "/diagnostics");
}

#[tokio::test]
async fn test_filter_include_multiple_channels() {
    let file = demo_file();

    let mut cmd = Command::cargo_bin("cli").expect("failed to create command");

    let tmp = NamedTempFile::new()
        .expect("failed to create temp file")
        .into_temp_path();

    let out = tmp.as_os_str().to_string_lossy().to_string();

    cmd.args([
        "filter",
        &file,
        "-o",
        &out,
        "--include-topic-regex",
        r"/diagnostics",
        "--include-topic-regex",
        r"/tf",
    ])
    .assert()
    .success();

    let info = read_info_from_file(&out).await;

    assert_eq!(info.channels.len(), 2);
    assert_eq!(info.channels[0].topic, "/diagnostics");
    assert_eq!(info.channels[1].topic, "/tf");
    assert_eq!(info.channels[1].topic, "/tf");
}

#[tokio::test]
async fn test_filter_nothing() {
    let file = demo_file();

    let mut cmd = Command::cargo_bin("cli").expect("failed to create command");

    let tmp = NamedTempFile::new()
        .expect("failed to create temp file")
        .into_temp_path();

    let out = tmp.as_os_str().to_string_lossy().to_string();

    cmd.args(["filter", &file, "-o", &out]).assert().success();

    let info = read_info_from_file(&out).await;
    let stats = info.statistics.expect("info.statistics should be defined");

    assert_eq!(stats.message_start_time, DEMO_START_TIME_NSECS);
    assert_eq!(stats.message_end_time, DEMO_END_TIME_NSECS);
    assert_eq!(stats.message_count, 1606);

    assert_eq!(info.header.profile, "ros1");

    // all chunks are using zstd by default
    assert!(info.chunk_indexes.iter().all(|x| x.compression == "zstd"));

    assert_eq!(info.channels.len(), 7);

    assert_eq!(info.attachment_indexes.len(), 0);
    assert_eq!(info.metadata_indexes.len(), 0);
}

#[tokio::test]
async fn test_filter_start_time() {
    let file = demo_file();

    let mut cmd = Command::cargo_bin("cli").expect("failed to create command");

    let tmp = NamedTempFile::new()
        .expect("failed to create temp file")
        .into_temp_path();

    let out = tmp.as_os_str().to_string_lossy().to_string();

    cmd.args([
        "filter",
        &file,
        "-o",
        &out,
        "--start-nsecs",
        "1490149585000000000",
    ])
    .assert()
    .success();

    let info = read_info_from_file(&out).await;
    let stats = info.statistics.expect("info.statistics should be defined");

    // the first message after 1490149585000000000
    assert_eq!(stats.message_start_time, 1490149585005254282);
    assert_eq!(stats.message_end_time, DEMO_END_TIME_NSECS);
    assert_eq!(stats.message_count, 598);
}

#[tokio::test]
async fn test_filter_end_time() {
    let file = demo_file();

    let mut cmd = Command::cargo_bin("cli").expect("failed to create command");

    let tmp = NamedTempFile::new()
        .expect("failed to create temp file")
        .into_temp_path();

    let out = tmp.as_os_str().to_string_lossy().to_string();

    cmd.args([
        "filter",
        &file,
        "-o",
        &out,
        "--end-nsecs",
        "1490149585000000000",
    ])
    .assert()
    .success();

    let info = read_info_from_file(&out).await;
    let stats = info.statistics.expect("info.statistics should be defined");

    // Last message before 1490149585000000000
    assert_eq!(stats.message_end_time, 1490149584995081366);

    assert_eq!(stats.message_start_time, DEMO_START_TIME_NSECS);
    assert_eq!(stats.message_count, 1008);
}
