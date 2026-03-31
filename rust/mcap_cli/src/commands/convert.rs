mod ros1_bag;

use std::fs::File;
use std::io::{BufWriter, Read, Seek};

use anyhow::{Context, Result, bail};
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

    let compression = match args.compression {
        ConvertCompression::Zstd => Some(Compression::Zstd),
        ConvertCompression::Lz4 => Some(Compression::Lz4),
        ConvertCompression::None => None,
    };

    let opts = WriteOptions::new()
        .profile("ros1")
        .use_chunks(args.chunked)
        .chunk_size(Some(args.chunk_size))
        .compression(compression)
        .calculate_chunk_crcs(args.include_crc);

    match file_type {
        InputFileType::Ros1Bag => ros1_bag::convert_ros1_bag(writer, input, opts),
    }
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
    bail!(
        "unsupported input format (expected ROS1 bag '#ROSBAG V2.0', got prefix '{rendered}')"
    );
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::{InputFileType, detect_file_type};

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
}
