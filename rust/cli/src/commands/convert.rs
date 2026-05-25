mod ros1_bag;
mod ros2_db3;

use std::fs::File;
use std::io::{BufWriter, Read};
use std::path::Path;

use anyhow::{bail, ensure, Context, Result};
use mcap::{Compression, WriteOptions};

use crate::cli::{CompressionFormat, ConvertCommand};
use crate::context::CommandContext;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum InputFileType {
    Ros1Bag,
    Ros2Db3,
}

pub fn run(_ctx: &CommandContext, args: ConvertCommand) -> Result<()> {
    ensure!(
        args.input != args.output,
        "input and output paths must be different"
    );
    let file_type = detect_file_type(&args.input)?;

    let output = File::create(&args.output)
        .with_context(|| format!("failed to open output '{}'", args.output.display()))?;
    let writer = BufWriter::new(output);

    let opts = build_write_options(
        args.compression,
        args.chunk_size,
        args.include_crc,
        args.chunked,
        file_type.profile(),
    );

    match file_type {
        InputFileType::Ros1Bag => {
            let mut input = File::open(&args.input)
                .with_context(|| format!("failed to open input '{}'", args.input.display()))?;
            validate_input(file_type, &mut input)?;
            ros1_bag::convert_ros1_bag(writer, input, opts)
        }
        InputFileType::Ros2Db3 => ros2_db3::convert_ros2_db3(writer, &args.input, opts),
    }
}

fn validate_input(file_type: InputFileType, input: &mut File) -> Result<()> {
    match file_type {
        InputFileType::Ros1Bag => ros1_bag::validate_ros1_bag_magic(input),
        InputFileType::Ros2Db3 => Ok(()),
    }
}

impl InputFileType {
    fn profile(self) -> &'static str {
        match self {
            InputFileType::Ros1Bag => "ros1",
            InputFileType::Ros2Db3 => "ros2",
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

fn detect_file_type(path: &Path) -> Result<InputFileType> {
    const ROS1_BAG_MAGIC: &[u8] = b"#ROSBAG V2.0";
    const SQLITE_MAGIC: &[u8] = b"SQLite format 3\0";

    let mut input =
        File::open(path).with_context(|| format!("failed to open input '{}'", path.display()))?;
    let mut magic = [0u8; 16];
    let bytes_read = input
        .read(&mut magic)
        .with_context(|| format!("failed to read input magic from '{}'", path.display()))?;

    if bytes_read >= ROS1_BAG_MAGIC.len() && &magic[..ROS1_BAG_MAGIC.len()] == ROS1_BAG_MAGIC {
        return Ok(InputFileType::Ros1Bag);
    }
    if bytes_read >= SQLITE_MAGIC.len() && &magic[..SQLITE_MAGIC.len()] == SQLITE_MAGIC {
        return Ok(InputFileType::Ros2Db3);
    }

    bail!(
        "unsupported input file type for '{}' (expected ROS1 bag or ROS2 SQLite db3 input)",
        path.display()
    );
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::io::{Cursor, Read, Seek, SeekFrom};
    use std::path::Path;

    use super::{build_write_options, detect_file_type, ros2_db3, InputFileType};
    use crate::cli::CompressionFormat;

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

    #[test]
    fn detects_ros1_bag_magic() {
        let path = temp_input("ros1-not-a-bag-extension", b"#ROSBAG V2.0\n");
        let file_type = detect_file_type(&path).expect("detect");
        std::fs::remove_file(path).expect("remove temp input");
        assert_eq!(file_type, InputFileType::Ros1Bag);
    }

    #[test]
    fn detects_ros2_db3_magic() {
        let file_type = detect_file_type(Path::new(IRON_TALKER_DB3)).expect("detect");
        assert_eq!(file_type, InputFileType::Ros2Db3);
    }

    #[test]
    fn rejects_unknown_magic() {
        let path = temp_input("unknown", b"not an mcap input");
        let err = detect_file_type(&path).expect_err("unknown input should fail");
        std::fs::remove_file(path).expect("remove temp input");
        assert!(err.to_string().contains("unsupported input file type"));
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
        let mut output = Cursor::new(Vec::new());
        let opts = build_write_options(CompressionFormat::None, 1024, true, true, "ros2");

        ros2_db3::convert_ros2_db3(&mut output, Path::new(IRON_TALKER_DB3), opts)
            .expect("convert iron db3");

        let bytes = output.into_inner();
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
        let mut output = Cursor::new(Vec::new());
        let opts = build_write_options(CompressionFormat::None, 1024, true, true, "ros2");

        let err = ros2_db3::convert_ros2_db3(&mut output, Path::new(HUMBLE_TALKER_DB3), opts)
            .expect_err("humble db3 should fail");

        assert!(err
            .to_string()
            .contains("does not contain embedded message definitions"));
        assert!(err.to_string().contains("ros2 bag convert"));
    }
}
