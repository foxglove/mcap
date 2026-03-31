mod ros1_bag;

use std::fs::File;
use std::io::{BufWriter, Read, Seek};

use anyhow::{bail, Context, Result};
use mcap::{Compression, WriteOptions};

use crate::cli::{ConvertCommand, ConvertCompression};
use crate::context::CommandContext;

const ROS1_BAG_MAGIC: &[u8] = b"#ROSBAG V2.0";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum InputFileType {
    Ros1Bag,
}

pub fn run(_ctx: &CommandContext, args: ConvertCommand) -> Result<()> {
    let mut input = File::open(&args.input)
        .with_context(|| format!("failed to open input '{}'", args.input.display()))?;
    let file_type = detect_file_type(&mut input)?;

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

fn build_write_options(
    compression: ConvertCompression,
    chunk_size: u64,
    include_crc: bool,
    chunked: bool,
) -> WriteOptions {
    let compression = match compression {
        ConvertCompression::Zstd => Some(Compression::Zstd),
        ConvertCompression::Lz4 => Some(Compression::Lz4),
        ConvertCompression::None => None,
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

fn detect_file_type<R: Read + Seek>(reader: &mut R) -> Result<InputFileType> {
    let mut magic = vec![0u8; ROS1_BAG_MAGIC.len()];
    reader
        .read_exact(&mut magic)
        .context("failed to read input magic bytes")?;
    reader
        .rewind()
        .context("failed to rewind input after magic check")?;

    if magic == ROS1_BAG_MAGIC {
        return Ok(InputFileType::Ros1Bag);
    }

    let rendered = String::from_utf8_lossy(&magic);
    bail!("unsupported input format (expected ROS1 bag '#ROSBAG V2.0', got prefix '{rendered}')");
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::io::{Cursor, Read, Seek, SeekFrom};

    use super::{build_write_options, detect_file_type, InputFileType};
    use crate::cli::ConvertCompression;

    fn build_sample_mcap(include_crc: bool) -> Vec<u8> {
        let mut output = Cursor::new(Vec::new());
        let opts = build_write_options(ConvertCompression::None, 1024, include_crc, true);
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
        let mut cursor = Cursor::new(b"#ROSBAG V2.0\nrest".to_vec());
        let file_type = detect_file_type(&mut cursor).expect("magic should parse");
        assert_eq!(file_type, InputFileType::Ros1Bag);
    }

    #[test]
    fn rejects_unknown_magic() {
        let mut cursor = Cursor::new(b"not_a_rosbag".to_vec());
        let err = detect_file_type(&mut cursor).expect_err("format should be rejected");
        assert!(
            err.to_string().contains("unsupported input format"),
            "actual error: {err:#}"
        );
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
}
