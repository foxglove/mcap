mod ros1_bag;
mod ros2_db3;

use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};

use anyhow::{bail, ensure, Context, Result};
use mcap::{Compression, WriteOptions};

use crate::cli::{CompressionFormat, ConvertCommand};
use crate::context::CommandContext;

const GIT_LFS_POINTER_PREFIX: &[u8] = b"version https://git-lfs.github.com";

pub fn run(_ctx: &CommandContext, args: ConvertCommand) -> Result<()> {
    let input = ConvertInput::detect(args.input)?;
    ensure_distinct_paths(input.path(), &args.output)?;
    let opts = build_write_options(
        args.compression,
        args.chunk_size,
        args.include_crc,
        args.chunked,
        input.profile(),
    );

    input.convert(&args.output, opts)
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ConvertInput {
    Ros1Bag(PathBuf),
    Ros2Db3(PathBuf),
}

impl ConvertInput {
    fn detect(path: PathBuf) -> Result<Self> {
        let extension = path
            .extension()
            .and_then(|extension| extension.to_str())
            .unwrap_or_default();

        if extension.eq_ignore_ascii_case("bag") {
            reject_lfs_pointer(&path)?;
            return Ok(Self::Ros1Bag(path));
        }
        if extension.eq_ignore_ascii_case("db3") {
            reject_lfs_pointer(&path)?;
            return Ok(Self::Ros2Db3(path));
        }

        bail!(
            "unsupported input file extension for '{}' (expected .bag for ROS 1 bag or .db3 for ROS 2 SQLite db3 input)",
            path.display()
        );
    }

    fn path(&self) -> &Path {
        match self {
            Self::Ros1Bag(path) | Self::Ros2Db3(path) => path,
        }
    }

    fn profile(&self) -> &'static str {
        match self {
            Self::Ros1Bag(_) => "ros1",
            Self::Ros2Db3(_) => "ros2",
        }
    }

    fn convert(self, output_path: &Path, opts: WriteOptions) -> Result<()> {
        match self {
            Self::Ros1Bag(input_path) => {
                ros1_bag::convert_ros1_bag_file(&input_path, output_path, opts)
            }
            Self::Ros2Db3(input_path) => {
                ros2_db3::convert_ros2_db3_file(&input_path, output_path, opts)
            }
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
    let mut magic = [0u8; 64];
    let bytes_read = input
        .read(&mut magic)
        .with_context(|| format!("failed to read input magic from '{}'", path.display()))?;
    let magic = &magic[..bytes_read];

    if magic.starts_with(GIT_LFS_POINTER_PREFIX) {
        bail!(
            "input '{}' appears to be a Git LFS pointer, not a bag file; run `git lfs pull` and try again",
            path.display()
        );
    }
    Ok(())
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

    use super::{build_write_options, ensure_distinct_paths, ros2_db3, ConvertInput};
    use crate::cli::{CompressionFormat, ConvertCommand};
    use crate::context::CommandContext;

    const IRON_TALKER_DB3: &str = "../../testdata/db3/talker-iron.db3";
    const HUMBLE_TALKER_DB3: &str = "../../testdata/db3/talker-humble.db3";

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

    fn convert_ros2_db3_fixture(path: &Path) -> Vec<u8> {
        let output = TempOutput::new("ros2-db3");
        let opts = build_write_options(CompressionFormat::None, 1024, true, true, "ros2");
        ros2_db3::convert_ros2_db3_file(path, &output.path, opts).expect("convert ROS 2 db3");
        fs::read(&output.path).expect("read converted MCAP")
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
        let input = ConvertInput::detect(path.clone()).expect("detect");
        std::fs::remove_file(path).expect("remove temp input");
        assert!(matches!(input, ConvertInput::Ros1Bag(_)));
    }

    #[test]
    fn detects_ros2_db3_extension() {
        let input = ConvertInput::detect(PathBuf::from(IRON_TALKER_DB3)).expect("detect");
        assert!(matches!(input, ConvertInput::Ros2Db3(_)));
    }

    #[test]
    fn rejects_unknown_extension() {
        let path = temp_input("unknown", b"not an mcap input");
        let err = ConvertInput::detect(path.clone()).expect_err("unknown input should fail");
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
        let err = ConvertInput::detect(path.clone()).expect_err("LFS pointer should fail");
        std::fs::remove_file(path).expect("remove temp input");
        assert!(err.to_string().contains("Git LFS pointer"));
        assert!(err.to_string().contains("git lfs pull"));
    }

    #[test]
    fn distinct_path_check_allows_single_component_missing_output() {
        let output_path = temp_path("single-component-output.mcap");
        let file_name = output_path.file_name().expect("file name");
        let current_dir_output = std::env::current_dir()
            .expect("current dir")
            .join(file_name);
        let _ = std::fs::remove_file(&current_dir_output);

        ensure_distinct_paths(Path::new(IRON_TALKER_DB3), Path::new(file_name))
            .expect("single-component output should resolve through current directory");
    }

    #[test]
    fn distinct_path_check_rejects_same_file_paths() {
        let path = Path::new(IRON_TALKER_DB3);
        let err = ensure_distinct_paths(path, path).expect_err("same input/output should fail");
        assert!(err.to_string().contains("input and output paths"));
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
                include_crc: false,
                chunked: true,
            },
        )
        .expect_err("missing input should fail");

        assert!(err.to_string().contains("failed to open input"));
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
    fn converts_iron_talker_db3_with_embedded_schemas() {
        let bytes = convert_ros2_db3_fixture(Path::new(IRON_TALKER_DB3));
        let summary = mcap::Summary::read(&bytes)
            .expect("summary read")
            .expect("summary present");
        let stats = summary.stats.expect("statistics");

        assert_eq!(stats.message_count, 20);
        assert_eq!(summary.channels.len(), 3);
        assert_eq!(summary.schemas.len(), 3);
        assert!(summary
            .schemas
            .values()
            .all(|schema| schema.encoding == "ros2msg"));
        let topic_channel = summary
            .channels
            .values()
            .find(|channel| channel.topic == "/topic")
            .expect("topic channel");
        assert_eq!(topic_channel.message_encoding, "cdr");
        assert!(topic_channel.metadata.contains_key("offered_qos_profiles"));
    }

    #[test]
    fn rejects_humble_talker_db3_without_embedded_schemas() {
        let output = TempOutput::new("humble-ros2-db3");
        let opts = build_write_options(CompressionFormat::None, 1024, true, true, "ros2");

        let err = ros2_db3::convert_ros2_db3_file(Path::new(HUMBLE_TALKER_DB3), &output.path, opts)
            .expect_err("humble db3 should fail");

        assert!(err
            .to_string()
            .contains("does not contain embedded message definitions"));
        assert!(err.to_string().contains("ros2 bag convert"));
    }

    #[test]
    fn rejects_db3_extension_with_invalid_sqlite_magic_during_conversion() {
        let path = temp_input("invalid.db3", b"not sqlite");
        let output = TempOutput::new("invalid-ros2-db3");
        let opts = build_write_options(CompressionFormat::None, 1024, true, true, "ros2");

        let err = ros2_db3::convert_ros2_db3_file(&path, &output.path, opts)
            .expect_err("invalid sqlite magic should fail");

        assert!(err.to_string().contains("invalid ROS 2 db3 magic"));
        std::fs::remove_file(path).expect("remove temp input");
    }

    #[test]
    fn rejects_humble_talker_db3_without_truncating_existing_output() {
        let output_path = temp_input("existing-output.mcap", b"keep me");
        let opts = build_write_options(CompressionFormat::None, 1024, true, true, "ros2");

        let err = ros2_db3::convert_ros2_db3_file(Path::new(HUMBLE_TALKER_DB3), &output_path, opts)
            .expect_err("humble db3 should fail");

        assert!(err
            .to_string()
            .contains("does not contain embedded message definitions"));
        assert_eq!(
            std::fs::read(&output_path).expect("read existing output"),
            b"keep me"
        );
        std::fs::remove_file(output_path).expect("remove temp output");
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
                include_crc: false,
                chunked: true,
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
    fn rejects_sqlite_database_without_rosbag2_topics_table() {
        let sqlite_path = temp_path("not-a-rosbag2.db3");
        let _ = std::fs::remove_file(&sqlite_path);
        {
            let db = rusqlite::Connection::open(&sqlite_path).expect("create sqlite db");
            db.execute("CREATE TABLE unrelated(id INTEGER PRIMARY KEY)", [])
                .expect("create unrelated table");
        }
        let output = TempOutput::new("not-a-rosbag2");
        let opts = build_write_options(CompressionFormat::None, 1024, true, true, "ros2");

        let err = ros2_db3::convert_ros2_db3_file(&sqlite_path, &output.path, opts)
            .expect_err("non-rosbag2 sqlite should fail");

        assert!(err
            .to_string()
            .contains("does not look like a ROS 2 db3 bag"));
        std::fs::remove_file(sqlite_path).expect("remove temp sqlite");
    }

    #[test]
    fn rejects_sqlite_database_without_rosbag2_messages_table() {
        let sqlite_path = temp_path("not-a-rosbag2-no-messages.db3");
        let _ = std::fs::remove_file(&sqlite_path);
        {
            let db = rusqlite::Connection::open(&sqlite_path).expect("create sqlite db");
            db.execute(
                "CREATE TABLE topics(
                    id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    type TEXT NOT NULL,
                    serialization_format TEXT NOT NULL
                )",
                [],
            )
            .expect("create topics table");
        }
        let output = TempOutput::new("not-a-rosbag2-no-messages");
        let opts = build_write_options(CompressionFormat::None, 1024, true, true, "ros2");

        let err = ros2_db3::convert_ros2_db3_file(&sqlite_path, &output.path, opts)
            .expect_err("sqlite without messages should fail");

        assert!(err.to_string().contains("missing 'messages' table"));
        std::fs::remove_file(sqlite_path).expect("remove temp sqlite");
    }

    #[test]
    fn converts_non_msg_topics_when_embedded_schema_exists() {
        let sqlite_path = temp_path("service-event.db3");
        let _ = std::fs::remove_file(&sqlite_path);
        {
            let db = rusqlite::Connection::open(&sqlite_path).expect("create sqlite db");
            db.execute_batch(
                "CREATE TABLE topics(
                    id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    type TEXT NOT NULL,
                    serialization_format TEXT NOT NULL
                );
                CREATE TABLE message_definitions(
                    id INTEGER PRIMARY KEY,
                    topic_type TEXT NOT NULL,
                    encoding TEXT NOT NULL,
                    encoded_message_definition TEXT NOT NULL,
                    type_description_hash TEXT NOT NULL
                );
                CREATE TABLE messages(
                    id INTEGER PRIMARY KEY,
                    topic_id INTEGER NOT NULL,
                    timestamp INTEGER NOT NULL,
                    data BLOB NOT NULL
                );",
            )
            .expect("create db3 tables");
            db.execute(
                "INSERT INTO topics(id, name, type, serialization_format) VALUES(1, '/add_two_ints/_service_event', 'example_interfaces/srv/AddTwoInts_Event', 'cdr')",
                [],
            )
            .expect("insert topic");
            db.execute(
                "INSERT INTO message_definitions(id, topic_type, encoding, encoded_message_definition, type_description_hash) VALUES(1, 'example_interfaces/srv/AddTwoInts_Event', 'ros2msg', 'int64 a\nint64 b\n', '')",
                [],
            )
            .expect("insert message definition");
            db.execute(
                "INSERT INTO messages(topic_id, timestamp, data) VALUES(1, 42, x'010203')",
                [],
            )
            .expect("insert message");
        }
        let bytes = convert_ros2_db3_fixture(&sqlite_path);
        let summary = mcap::Summary::read(&bytes)
            .expect("summary read")
            .expect("summary present");
        assert!(summary
            .channels
            .values()
            .any(|channel| channel.topic == "/add_two_ints/_service_event"));
        assert!(summary
            .schemas
            .values()
            .any(|schema| schema.name == "example_interfaces/srv/AddTwoInts_Event"));
        std::fs::remove_file(sqlite_path).expect("remove temp sqlite");
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
                    include_crc: false,
                    chunked: true,
                },
            )?;

            let bytes = fs::read(&output.path)?;
            let mut records = mcap::read::LinearReader::new(&bytes)?;
            match records.next() {
                Some(Ok(mcap::records::Record::Header(header))) => {
                    assert_eq!(header.profile, "ros1", "{fixture}")
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
