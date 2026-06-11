mod ros1_bag;
mod ros2_db3;

use std::fs::File;
use std::io::Read;
use std::path::Path;

use anyhow::{bail, ensure, Context, Result};
use mcap::{Compression, WriteOptions};

use crate::cli::{CompressionFormat, ConvertCommand};
use crate::context::CommandContext;

const GIT_LFS_POINTER_PREFIX: &[u8] = b"version https://git-lfs.github.com";

pub fn run(ctx: &CommandContext, args: ConvertCommand) -> Result<()> {
    let input = ConvertInput::detect(&args.input)?;
    let is_remote = crate::source::is_remote_url(&args.input);
    if !is_remote {
        reject_lfs_pointer(&args.input)?;
    }
    let materialized_input = crate::source::materialize_input(
        &args.input,
        crate::source::SourceOptions::new(ctx.allow_remote_scan()),
    )?;
    if !is_remote {
        ensure_distinct_paths(materialized_input.path(), &args.output)?;
    }
    let opts = build_write_options(
        args.compression,
        args.chunk_size,
        !args.no_crc,
        !args.no_chunks,
        input.profile(),
    );

    input.convert(materialized_input.path(), &args.output, opts)
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ConvertInput {
    Ros1Bag,
    Ros2Db3,
}

impl ConvertInput {
    fn detect(path: &Path) -> Result<Self> {
        let extension = crate::source::remote_or_local_extension(path).unwrap_or_default();

        if extension.eq_ignore_ascii_case("bag") {
            return Ok(Self::Ros1Bag);
        }
        if extension.eq_ignore_ascii_case("db3") {
            return Ok(Self::Ros2Db3);
        }

        bail!(
            "unsupported input file extension for '{}' (expected .bag for ROS 1 bag or .db3 for ROS 2 SQLite db3 input)",
            path.display()
        );
    }

    fn profile(&self) -> &'static str {
        match self {
            Self::Ros1Bag => "ros1",
            Self::Ros2Db3 => "ros2",
        }
    }

    fn convert(self, input_path: &Path, output_path: &Path, opts: WriteOptions) -> Result<()> {
        match self {
            Self::Ros1Bag => ros1_bag::convert_ros1_bag_file(input_path, output_path, opts),
            Self::Ros2Db3 => ros2_db3::convert_ros2_db3_file(input_path, output_path, opts),
        }
    }
}

fn build_write_options(
    compression: CompressionFormat,
    chunk_size: u64,
    include_crc: bool,
    chunked: bool,
    profile: &str,
) -> WriteOptions {
    let compression = match compression {
        CompressionFormat::Zstd => Some(Compression::Zstd),
        CompressionFormat::Lz4 => Some(Compression::Lz4),
        CompressionFormat::None => None,
    };

    WriteOptions::new()
        .profile(profile)
        .library(crate::cli::WRITER_LIBRARY.clone())
        .use_chunks(chunked)
        .chunk_size(Some(chunk_size))
        .compression(compression)
        .calculate_chunk_crcs(include_crc)
        .calculate_data_section_crc(include_crc)
        .calculate_summary_section_crc(include_crc)
        .calculate_attachment_crcs(include_crc)
}

fn reject_lfs_pointer(path: &Path) -> Result<()> {
    let mut input =
        File::open(path).with_context(|| format!("failed to open input '{}'", path.display()))?;
    let mut magic = [0u8; GIT_LFS_POINTER_PREFIX.len()];
    let bytes_read = read_prefix(&mut input, &mut magic)
        .with_context(|| format!("failed to read input prefix from '{}'", path.display()))?;
    let magic = &magic[..bytes_read];

    if magic.starts_with(GIT_LFS_POINTER_PREFIX) {
        bail!(
            "input '{}' appears to be a Git LFS pointer; run `git lfs pull` and try again",
            path.display()
        );
    }
    Ok(())
}

fn read_prefix(input: &mut File, buffer: &mut [u8]) -> std::io::Result<usize> {
    let mut bytes_read = 0;
    while bytes_read < buffer.len() {
        match input.read(&mut buffer[bytes_read..]) {
            Ok(0) => break,
            Ok(read) => bytes_read += read,
            Err(err) if err.kind() == std::io::ErrorKind::Interrupted => {}
            Err(err) => return Err(err),
        }
    }
    Ok(bytes_read)
}

fn ensure_distinct_paths(input: &Path, output: &Path) -> Result<()> {
    let input_path = input
        .canonicalize()
        .with_context(|| format!("failed to resolve input path '{}'", input.display()))?;
    let output_path = match output.canonicalize() {
        Ok(path) => path,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            let parent = output
                .parent()
                .filter(|parent| !parent.as_os_str().is_empty())
                .unwrap_or_else(|| Path::new("."));
            let file_name = output
                .file_name()
                .with_context(|| format!("invalid output path '{}'", output.display()))?;
            let parent_path = match parent.canonicalize() {
                Ok(path) => path,
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                    return Ok(());
                }
                Err(err) => {
                    return Err(err).with_context(|| {
                        format!("failed to resolve output parent '{}'", parent.display())
                    });
                }
            };
            parent_path.join(file_name)
        }
        Err(err) => {
            return Err(err)
                .with_context(|| format!("failed to resolve output path '{}'", output.display()));
        }
    };

    ensure!(
        input_path != output_path,
        "input and output paths must be different"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::fs;
    use std::io::{Cursor, Read, Seek, SeekFrom};
    use std::path::{Path, PathBuf};

    use anyhow::Result;

    use super::{build_write_options, ensure_distinct_paths, reject_lfs_pointer, ConvertInput};
    use crate::cli::{CompressionFormat, ConvertCommand};
    use crate::context::CommandContext;

    fn temp_input(name: &str, bytes: &[u8]) -> std::path::PathBuf {
        let path = std::env::temp_dir().join(format!(
            "mcap-cli-convert-test-{}-{name}",
            std::process::id()
        ));
        std::fs::write(&path, bytes).expect("write temp input");
        path
    }

    fn temp_path(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "mcap-cli-convert-test-{}-{name}",
            std::process::id()
        ))
    }

    fn build_sample_mcap(include_crc: bool) -> Vec<u8> {
        let mut output = Cursor::new(Vec::new());
        let opts = build_write_options(CompressionFormat::None, 1024, include_crc, true, "ros1");
        {
            let mut writer = opts.create(&mut output).expect("writer");
            let schema_id = writer
                .add_schema("demo", "ros1msg", b"uint8 data\n")
                .expect("schema");
            let channel_id = writer
                .add_channel(
                    schema_id,
                    "/demo",
                    "ros1",
                    &BTreeMap::from([(String::from("md5sum"), String::from("abc"))]),
                )
                .expect("channel");
            writer
                .write_to_known_channel(
                    &mcap::records::MessageHeader {
                        channel_id,
                        sequence: 0,
                        log_time: 1,
                        publish_time: 1,
                    },
                    b"\x11\x22",
                )
                .expect("message");
            writer.finish().expect("finish");
        }
        output.seek(SeekFrom::Start(0)).expect("seek");
        let mut bytes = Vec::new();
        output.read_to_end(&mut bytes).expect("read");
        bytes
    }

    fn data_end_crc(mcap_bytes: &[u8]) -> u32 {
        for record in mcap::read::LinearReader::new(mcap_bytes).expect("reader") {
            if let mcap::records::Record::DataEnd(data_end) = record.expect("record") {
                return data_end.data_section_crc;
            }
        }
        panic!("missing DataEnd record");
    }

    fn fixture_path(relative_from_repo_root: &str) -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .join(relative_from_repo_root)
    }

    struct TempOutput {
        path: PathBuf,
    }

    impl TempOutput {
        fn new(name: &str) -> Self {
            let path = std::env::temp_dir().join(format!(
                "mcap-rust-convert-{name}-{}.mcap",
                std::process::id()
            ));
            let _ = fs::remove_file(&path);
            Self { path }
        }
    }

    impl Drop for TempOutput {
        fn drop(&mut self) {
            let _ = fs::remove_file(&self.path);
        }
    }

    #[test]
    fn detects_ros1_bag_extension() {
        let path = temp_input("ros1.bag", b"not validated until conversion");
        let input = ConvertInput::detect(&path).expect("detect");
        std::fs::remove_file(path).expect("remove temp input");
        assert!(matches!(input, ConvertInput::Ros1Bag));
    }

    #[test]
    fn detects_ros2_db3_extension() {
        let path = temp_input("ros2.db3", b"not validated until conversion");
        let input = ConvertInput::detect(&path).expect("detect");
        std::fs::remove_file(path).expect("remove temp input");
        assert!(matches!(input, ConvertInput::Ros2Db3));
    }

    #[test]
    fn detects_remote_cloud_extensions() {
        assert!(matches!(
            ConvertInput::detect(Path::new("s3://bucket/path/demo.bag?token=secret"))
                .expect("detect s3 bag"),
            ConvertInput::Ros1Bag
        ));
        assert!(matches!(
            ConvertInput::detect(Path::new("gs://bucket/path/demo.db3#fragment"))
                .expect("detect gcs db3"),
            ConvertInput::Ros2Db3
        ));
    }

    #[test]
    fn rejects_unknown_extension() {
        let path = temp_input("unknown", b"not an mcap input");
        let err = ConvertInput::detect(&path).expect_err("unknown input should fail");
        std::fs::remove_file(path).expect("remove temp input");
        assert!(err.to_string().contains("unsupported input file extension"));
    }

    #[test]
    fn rejects_git_lfs_pointer_before_output_creation() {
        let path = temp_input(
            "lfs-pointer.db3",
            b"version https://git-lfs.github.com/spec/v1\n\
oid sha256:0000000000000000000000000000000000000000000000000000000000000000\n\
size 123\n",
        );
        let err = reject_lfs_pointer(&path).expect_err("LFS pointer should fail");
        std::fs::remove_file(path).expect("remove temp input");
        assert!(err.to_string().contains("Git LFS pointer"));
        assert!(err.to_string().contains("git lfs pull"));
    }

    #[test]
    fn distinct_path_check_allows_single_component_missing_output() {
        let input_path = temp_input("distinct-input.db3", b"placeholder");
        let output_path = temp_path("single-component-output.mcap");
        let file_name = output_path.file_name().expect("file name");
        let current_dir_output = std::env::current_dir()
            .expect("current dir")
            .join(file_name);
        let _ = std::fs::remove_file(&current_dir_output);

        ensure_distinct_paths(&input_path, Path::new(file_name))
            .expect("single-component output should resolve through current directory");
        std::fs::remove_file(input_path).expect("remove temp input");
    }

    #[test]
    fn distinct_path_check_rejects_same_file_paths() {
        let path = temp_input("same-path.db3", b"placeholder");
        let err = ensure_distinct_paths(&path, &path).expect_err("same input/output should fail");
        assert!(err.to_string().contains("input and output paths"));
        std::fs::remove_file(path).expect("remove temp input");
    }

    #[test]
    fn run_reports_missing_input_as_open_error() {
        let err = super::run(
            &CommandContext::default(),
            ConvertCommand {
                input: PathBuf::from("/tmp/mcap-cli-missing-input-does-not-exist.db3"),
                output: PathBuf::from("/tmp/mcap-cli-missing-output.mcap"),
                compression: CompressionFormat::None,
                chunk_size: 8 * 1024 * 1024,
                no_crc: true,
                no_chunks: false,
            },
        )
        .expect_err("missing input should fail");

        assert!(err.to_string().contains("failed to open input"));
    }

    #[test]
    fn remote_input_requires_allow_remote_scan_before_download() {
        let err = super::run(
            &CommandContext::default(),
            ConvertCommand {
                input: PathBuf::from("https://example.com/demo.bag?token=secret"),
                output: PathBuf::from("/tmp/mcap-cli-remote-output.mcap"),
                compression: CompressionFormat::None,
                chunk_size: 8 * 1024 * 1024,
                no_crc: true,
                no_chunks: false,
            },
        )
        .expect_err("remote convert should require opt-in");

        assert!(err.to_string().contains("--allow-remote-scan"));
        assert!(!err.to_string().contains("token=secret"));
    }

    #[test]
    fn cloud_input_requires_allow_remote_scan_before_download() {
        let err = super::run(
            &CommandContext::default(),
            ConvertCommand {
                input: PathBuf::from("s3://bucket/demo.bag?token=secret"),
                output: PathBuf::from("/tmp/mcap-cli-cloud-output.mcap"),
                compression: CompressionFormat::None,
                chunk_size: 8 * 1024 * 1024,
                no_crc: true,
                no_chunks: false,
            },
        )
        .expect_err("cloud convert should require opt-in");

        assert!(err.to_string().contains("--allow-remote-scan"));
        assert!(!err.to_string().contains("token=secret"));
    }

    #[test]
    fn include_crc_false_zeros_data_end_and_footer_crc() {
        let bytes = build_sample_mcap(false);

        let footer = mcap::read::footer(&bytes).expect("footer");
        assert_eq!(footer.summary_crc, 0);
        assert_eq!(data_end_crc(&bytes), 0);
    }

    #[test]
    fn include_crc_true_enables_data_and_summary_crc() {
        let bytes = build_sample_mcap(true);
        let footer = mcap::read::footer(&bytes).expect("footer");
        assert_ne!(footer.summary_crc, 0);
        assert_ne!(data_end_crc(&bytes), 0);
    }

    #[test]
    fn rejects_invalid_ros1_bag_magic_without_truncating_existing_output() {
        let input_path = temp_input("invalid.bag", b"not a bag file for sure");
        let output_path = temp_input("existing-ros1-output.mcap", b"keep me");

        let err = super::run(
            &CommandContext::default(),
            ConvertCommand {
                input: input_path.clone(),
                output: output_path.clone(),
                compression: CompressionFormat::None,
                chunk_size: 8 * 1024 * 1024,
                no_crc: true,
                no_chunks: false,
            },
        )
        .expect_err("invalid ROS 1 bag should fail");

        assert!(err.to_string().contains("invalid ROS1 bag magic"));
        assert_eq!(
            std::fs::read(&output_path).expect("read existing output"),
            b"keep me"
        );
        std::fs::remove_file(input_path).expect("remove temp input");
        std::fs::remove_file(output_path).expect("remove temp output");
    }

    #[test]
    fn convert_command_handles_noetic_generated_ros1_bags() -> Result<()> {
        let cases = [
            ("noetic-empty.bag", 0usize, 0usize, 0usize),
            ("noetic-multitopic-none.bag", 3, 2, 2),
            ("noetic-multitopic-bz2.bag", 3, 2, 2),
            ("noetic-multitopic-lz4.bag", 3, 2, 2),
        ];

        for (fixture, expected_messages, expected_channels, expected_schemas) in cases {
            let input = fixture_path(&format!("testdata/bags/generated/{fixture}"));
            let output = TempOutput::new(fixture.trim_end_matches(".bag"));

            super::run(
                &CommandContext::default(),
                ConvertCommand {
                    input: input.clone(),
                    output: output.path.clone(),
                    compression: CompressionFormat::None,
                    chunk_size: 8 * 1024 * 1024,
                    no_crc: true,
                    no_chunks: false,
                },
            )?;

            let bytes = fs::read(&output.path)?;
            let mut records = mcap::read::LinearReader::new(&bytes)?;
            match records.next() {
                Some(Ok(mcap::records::Record::Header(header))) => {
                    assert_eq!(header.profile, "ros1", "{fixture}");
                    // convert authors a fresh MCAP, so it stamps the CLI writer identity.
                    assert_eq!(header.library, *crate::cli::WRITER_LIBRARY, "{fixture}");
                }
                other => panic!("{fixture}: expected MCAP header as first record, got {other:?}"),
            }
            let summary = mcap::Summary::read(&bytes)?.expect("expected summary");
            assert_eq!(summary.channels.len(), expected_channels, "{fixture}");
            assert_eq!(summary.schemas.len(), expected_schemas, "{fixture}");
            assert!(summary
                .channels
                .values()
                .all(|channel| channel.message_encoding == "ros1"));
            assert!(summary
                .schemas
                .values()
                .all(|schema| schema.encoding == "ros1msg"));

            let messages =
                mcap::MessageStream::new(&bytes)?.collect::<mcap::McapResult<Vec<_>>>()?;
            assert_eq!(messages.len(), expected_messages, "{fixture}");
            if expected_messages > 0 {
                let chatter = summary
                    .channels
                    .values()
                    .find(|channel| channel.topic == "/chatter")
                    .unwrap_or_else(|| panic!("{fixture}: missing /chatter channel"));
                assert_eq!(chatter.message_encoding, "ros1", "{fixture}");
                assert_eq!(
                    chatter
                        .schema
                        .as_ref()
                        .expect("/chatter should have a schema")
                        .name,
                    "std_msgs/String",
                    "{fixture}"
                );

                let numbers = summary
                    .channels
                    .values()
                    .find(|channel| channel.topic == "/numbers")
                    .unwrap_or_else(|| panic!("{fixture}: missing /numbers channel"));
                assert_eq!(numbers.message_encoding, "ros1", "{fixture}");
                assert_eq!(
                    numbers
                        .schema
                        .as_ref()
                        .expect("/numbers should have a schema")
                        .name,
                    "std_msgs/UInt32",
                    "{fixture}"
                );

                assert_eq!(messages[0].channel.topic, "/chatter", "{fixture}");
                assert_eq!(
                    messages[0]
                        .channel
                        .schema
                        .as_ref()
                        .expect("first message schema")
                        .name,
                    "std_msgs/String",
                    "{fixture}"
                );
                assert_eq!(messages[0].log_time, 1_000_000_002, "{fixture}");
                assert_eq!(messages[0].data.as_ref(), b"\x05\0\0\0hello", "{fixture}");

                assert_eq!(messages[1].channel.topic, "/numbers", "{fixture}");
                assert_eq!(
                    messages[1]
                        .channel
                        .schema
                        .as_ref()
                        .expect("second message schema")
                        .name,
                    "std_msgs/UInt32",
                    "{fixture}"
                );
                assert_eq!(messages[1].log_time, 2_000_000_003, "{fixture}");
                assert_eq!(messages[1].data.as_ref(), &42u32.to_le_bytes(), "{fixture}");

                assert_eq!(messages[2].channel.topic, "/chatter", "{fixture}");
                assert_eq!(messages[2].log_time, 3_000_000_004, "{fixture}");
                assert_eq!(messages[2].data.as_ref(), b"\x05\0\0\0world", "{fixture}");
            }
        }

        Ok(())
    }
}
