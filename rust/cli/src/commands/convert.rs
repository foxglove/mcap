mod ros1_bag;

use std::fs::File;
use std::io::BufWriter;
use std::path::Path;

use anyhow::{bail, Context, Result};
use mcap::{Compression, WriteOptions};

use crate::cli::{CompressionFormat, ConvertCommand};
use crate::context::CommandContext;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum InputFileType {
    Ros1Bag,
}

pub fn run(_ctx: &CommandContext, args: ConvertCommand) -> Result<()> {
    let file_type = detect_file_type(&args.input)?;
    let mut input = File::open(&args.input)
        .with_context(|| format!("failed to open input '{}'", args.input.display()))?;
    validate_input(file_type, &mut input)?;

    let output = File::create(&args.output)
        .with_context(|| format!("failed to open output '{}'", args.output.display()))?;
    let writer = BufWriter::new(output);

    let opts = build_write_options(
        args.compression,
        args.chunk_size,
        args.include_crc,
        args.chunked,
    );

    match file_type {
        InputFileType::Ros1Bag => ros1_bag::convert_ros1_bag(writer, input, opts),
    }
}

fn validate_input(file_type: InputFileType, input: &mut File) -> Result<()> {
    match file_type {
        InputFileType::Ros1Bag => ros1_bag::validate_ros1_bag_magic(input),
    }
}

fn build_write_options(
    compression: CompressionFormat,
    chunk_size: u64,
    include_crc: bool,
    chunked: bool,
) -> WriteOptions {
    let compression = match compression {
        CompressionFormat::Zstd => Some(Compression::Zstd),
        CompressionFormat::Lz4 => Some(Compression::Lz4),
        CompressionFormat::None => None,
    };

    WriteOptions::new()
        .profile("ros1")
        .use_chunks(chunked)
        .chunk_size(Some(chunk_size))
        .compression(compression)
        .calculate_chunk_crcs(include_crc)
        .calculate_data_section_crc(include_crc)
        .calculate_summary_section_crc(include_crc)
        .calculate_attachment_crcs(include_crc)
}

fn detect_file_type(path: &Path) -> Result<InputFileType> {
    if path
        .extension()
        .and_then(|ext| ext.to_str())
        .is_some_and(|ext| ext.eq_ignore_ascii_case("bag"))
    {
        return Ok(InputFileType::Ros1Bag);
    }

    bail!(
        "unsupported input file extension '{}' (expected .bag for ROS1 bag input)",
        path.extension()
            .and_then(|ext| ext.to_str())
            .unwrap_or("<none>")
    );
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::fs;
    use std::io::{Cursor, Read, Seek, SeekFrom};
    use std::path::{Path, PathBuf};

    use anyhow::Result;

    use super::{build_write_options, detect_file_type, InputFileType};
    use crate::cli::{CompressionFormat, ConvertCommand};
    use crate::context::CommandContext;

    fn build_sample_mcap(include_crc: bool) -> Vec<u8> {
        let mut output = Cursor::new(Vec::new());
        let opts = build_write_options(CompressionFormat::None, 1024, include_crc, true);
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
        let file_type = detect_file_type(Path::new("/tmp/input.bag")).expect("detect");
        assert_eq!(file_type, InputFileType::Ros1Bag);
    }

    #[test]
    fn detects_ros1_bag_extension_case_insensitive() {
        let file_type = detect_file_type(Path::new("/tmp/input.BAG")).expect("detect");
        assert_eq!(file_type, InputFileType::Ros1Bag);
    }

    #[test]
    fn rejects_non_bag_extension() {
        let err = detect_file_type(Path::new("/tmp/input.mcap")).expect_err("non-bag should fail");
        assert!(err.to_string().contains("unsupported input file extension"));
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
