use std::borrow::Cow;
use std::collections::BTreeMap;
use std::io::{Cursor, Read, Seek};
use std::sync::Arc;

use anyhow::{bail, ensure, Context, Result};

const BAG_MAGIC: &[u8] = b"#ROSBAG V2.0\n";

const OP_BAG_HEADER: u8 = 0x03;
const OP_BAG_CHUNK: u8 = 0x05;
const OP_BAG_CONNECTION: u8 = 0x07;
const OP_BAG_MESSAGE_DATA: u8 = 0x02;
const OP_BAG_INDEX_DATA: u8 = 0x04;
const OP_BAG_CHUNK_INFO: u8 = 0x06;

const KEY_OP: &str = "op";
const KEY_COMPRESSION: &str = "compression";
const KEY_SIZE: &str = "size";
const KEY_CONN: &str = "conn";
const KEY_TOPIC: &str = "topic";
const KEY_TIME: &str = "time";
const KEY_TYPE: &str = "type";
const KEY_MD5SUM: &str = "md5sum";
const KEY_MESSAGE_DEFINITION: &str = "message_definition";
const MAX_RECORD_SECTION_SIZE: u32 = 1 << 30; // 1 GiB

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct SchemaKey {
    msg_type: String,
    md5sum: String,
}

#[derive(Debug, Clone)]
struct ConnectionInfo {
    channel: Arc<mcap::Channel<'static>>,
}

#[derive(Debug, Clone)]
struct ConversionState {
    schemas: BTreeMap<SchemaKey, u16>,
    connections: BTreeMap<u32, ConnectionInfo>,
    sequence: u32,
}

impl ConversionState {
    fn new() -> Self {
        Self {
            schemas: BTreeMap::new(),
            connections: BTreeMap::new(),
            sequence: 0,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct BagRecord {
    header: Vec<u8>,
    data: Vec<u8>,
}

pub fn convert_ros1_bag<W: std::io::Write + Seek, R: Read + Seek>(
    output: W,
    mut input: R,
    write_options: mcap::WriteOptions,
) -> Result<()> {
    let mut magic = vec![0u8; BAG_MAGIC.len()];
    input
        .read_exact(&mut magic)
        .context("failed to read ROS1 bag magic")?;
    ensure!(
        magic == BAG_MAGIC,
        "invalid ROS1 bag magic (expected '#ROSBAG V2.0\\n')"
    );

    let mut writer = write_options
        .create(output)
        .context("failed to create MCAP writer")?;
    let mut state = ConversionState::new();

    process_records(input, |record| {
        process_record(&mut writer, &mut state, record)
    })?;

    writer.finish().context("failed to finalize MCAP writer")?;
    Ok(())
}

pub fn validate_ros1_bag_magic<R: Read + Seek>(input: &mut R) -> Result<()> {
    let mut magic = vec![0u8; BAG_MAGIC.len()];
    input
        .read_exact(&mut magic)
        .context("failed to read ROS1 bag magic")?;
    ensure!(
        magic == BAG_MAGIC,
        "invalid ROS1 bag magic (expected '#ROSBAG V2.0\\n')"
    );
    input
        .rewind()
        .context("failed to rewind input after ROS1 bag magic check")?;
    Ok(())
}

fn process_record<W: std::io::Write + Seek>(
    writer: &mut mcap::Writer<W>,
    state: &mut ConversionState,
    record: BagRecord,
) -> Result<()> {
    let header_fields = parse_header_fields(&record.header)?;
    let op = header_u8(&header_fields, KEY_OP)?;

    match op {
        OP_BAG_HEADER | OP_BAG_INDEX_DATA | OP_BAG_CHUNK_INFO => Ok(()),
        OP_BAG_CONNECTION => process_connection(writer, state, &header_fields, &record.data),
        OP_BAG_MESSAGE_DATA => process_message(writer, state, &header_fields, &record.data),
        OP_BAG_CHUNK => process_chunk(writer, state, &header_fields, &record.data),
        _ => Ok(()),
    }
}

fn process_connection<W: std::io::Write + Seek>(
    writer: &mut mcap::Writer<W>,
    state: &mut ConversionState,
    header_fields: &BTreeMap<String, Vec<u8>>,
    data: &[u8],
) -> Result<()> {
    let conn_id = header_u32(header_fields, KEY_CONN)?;
    let topic = header_string(header_fields, KEY_TOPIC)?;
    let mut conn_data = parse_header_fields(data)?;

    let msg_type = take_required_string(&mut conn_data, KEY_TYPE)?;
    let md5sum = required_string(&conn_data, KEY_MD5SUM)?;
    let message_definition = take_required_bytes(&mut conn_data, KEY_MESSAGE_DEFINITION)?;
    let schema_key = SchemaKey {
        msg_type: msg_type.clone(),
        md5sum,
    };

    let schema_id = if let Some(id) = state.schemas.get(&schema_key) {
        *id
    } else {
        let id = writer
            .add_schema(&msg_type, "ros1msg", &message_definition)
            .context("failed to write ROS1 schema")?;
        state.schemas.insert(schema_key, id);
        id
    };

    let metadata = to_string_map(conn_data)?;
    let channel_id = conn_id_to_channel_id(conn_id)?;
    let schema = mcap::Schema {
        id: schema_id,
        name: msg_type,
        encoding: "ros1msg".to_string(),
        data: Cow::Owned(message_definition),
    };
    let channel = mcap::Channel {
        id: channel_id,
        topic: topic.to_string(),
        schema: Some(Arc::new(schema)),
        message_encoding: "ros1".to_string(),
        metadata,
    };
    state.connections.insert(
        conn_id,
        ConnectionInfo {
            channel: Arc::new(channel),
        },
    );
    Ok(())
}

fn process_message<W: std::io::Write + Seek>(
    writer: &mut mcap::Writer<W>,
    state: &mut ConversionState,
    header_fields: &BTreeMap<String, Vec<u8>>,
    data: &[u8],
) -> Result<()> {
    let conn_id = header_u32(header_fields, KEY_CONN)?;
    let time = header_bytes(header_fields, KEY_TIME)?;
    ensure!(
        time.len() == 8,
        "invalid ROS time field size for conn {conn_id}: expected 8 bytes, got {}",
        time.len()
    );
    let log_time = ros_time_to_nanos(time);
    let Some(conn_info) = state.connections.get(&conn_id) else {
        bail!("message references unknown connection id {conn_id}");
    };
    // We intentionally write via the high-level `Writer::write()` path (instead of
    // `write_to_known_channel`) so we can preserve ROS `conn` IDs as MCAP channel IDs.
    // `add_channel()` auto-assigns channel IDs, and `write_to_known_channel()` requires
    // those writer-assigned IDs. We build the explicit channel once per ROS connection
    // and Arc-clone it here, avoiding per-message topic/schema/metadata allocations
    // while still keeping Go parity for conn->channel mapping.

    writer
        .write(&mcap::Message {
            channel: Arc::clone(&conn_info.channel),
            sequence: state.sequence,
            log_time,
            publish_time: log_time,
            data: Cow::Borrowed(data),
        })
        .context("failed to write converted ROS1 message")?;
    state.sequence = state.sequence.wrapping_add(1);
    Ok(())
}

fn process_chunk<W: std::io::Write + Seek>(
    writer: &mut mcap::Writer<W>,
    state: &mut ConversionState,
    header_fields: &BTreeMap<String, Vec<u8>>,
    data: &[u8],
) -> Result<()> {
    let compression = header_string(header_fields, KEY_COMPRESSION)?;
    let expected_decompressed_size = header_u32(header_fields, KEY_SIZE)? as usize;
    ensure!(
        expected_decompressed_size <= MAX_RECORD_SECTION_SIZE as usize,
        "chunk uncompressed size {expected_decompressed_size} exceeds maximum {MAX_RECORD_SECTION_SIZE}"
    );
    let decompressed = decompress_chunk(compression, data, expected_decompressed_size)?;
    process_records(Cursor::new(decompressed), |record| {
        process_record(writer, state, record)
    })
}

fn process_records<R: Read, F: FnMut(BagRecord) -> Result<()>>(
    mut reader: R,
    mut on_record: F,
) -> Result<()> {
    loop {
        let Some(record) = read_record(&mut reader)? else {
            return Ok(());
        };
        on_record(record)?;
    }
}

fn read_record<R: Read>(reader: &mut R) -> Result<Option<BagRecord>> {
    let Some(header_len) =
        read_u32_maybe_eof(reader).context("failed to read record header length")?
    else {
        return Ok(None);
    };
    ensure!(
        header_len <= MAX_RECORD_SECTION_SIZE,
        "record header length {header_len} exceeds maximum {MAX_RECORD_SECTION_SIZE}"
    );
    let mut header = vec![0u8; header_len as usize];
    reader
        .read_exact(&mut header)
        .context("failed to read record header bytes")?;

    let data_len = read_u32(reader).context("failed to read record data length")?;
    ensure!(
        data_len <= MAX_RECORD_SECTION_SIZE,
        "record data length {data_len} exceeds maximum {MAX_RECORD_SECTION_SIZE}"
    );
    let mut data = vec![0u8; data_len as usize];
    reader
        .read_exact(&mut data)
        .context("failed to read record payload")?;

    Ok(Some(BagRecord { header, data }))
}

fn read_u32<R: Read>(reader: &mut R) -> Result<u32> {
    let mut bytes = [0u8; 4];
    reader.read_exact(&mut bytes)?;
    Ok(u32::from_le_bytes(bytes))
}

fn read_u32_maybe_eof<R: Read>(reader: &mut R) -> Result<Option<u32>> {
    let mut bytes = [0u8; 4];
    let mut filled = 0usize;
    while filled < bytes.len() {
        let read = reader.read(&mut bytes[filled..])?;
        if read == 0 {
            if filled == 0 {
                return Ok(None);
            }
            bail!("unexpected EOF while reading u32");
        }
        filled += read;
    }
    Ok(Some(u32::from_le_bytes(bytes)))
}

fn parse_header_fields(buf: &[u8]) -> Result<BTreeMap<String, Vec<u8>>> {
    let mut out = BTreeMap::new();
    let mut offset = 0usize;
    while offset < buf.len() {
        ensure!(
            offset + 4 <= buf.len(),
            "invalid header field length prefix at offset {offset}"
        );
        let field_len = u32::from_le_bytes(
            buf[offset..offset + 4]
                .try_into()
                .expect("slice length verified above"),
        ) as usize;
        offset += 4;
        ensure!(
            offset + field_len <= buf.len(),
            "header field exceeds available bytes"
        );
        let field = &buf[offset..offset + field_len];
        offset += field_len;
        let Some(eq_idx) = field.iter().position(|b| *b == b'=') else {
            bail!("header field missing '=' separator");
        };
        let key = String::from_utf8(field[..eq_idx].to_vec()).context("invalid utf8 header key")?;
        let value = field[eq_idx + 1..].to_vec();
        out.insert(key, value);
    }
    Ok(out)
}

fn to_string_map(fields: BTreeMap<String, Vec<u8>>) -> Result<BTreeMap<String, String>> {
    fields
        .into_iter()
        .map(|(k, v)| {
            let value = String::from_utf8(v)
                .with_context(|| format!("invalid utf8 value for metadata field '{k}'"))?;
            Ok((k, value))
        })
        .collect()
}

fn header_bytes<'a>(header_fields: &'a BTreeMap<String, Vec<u8>>, key: &str) -> Result<&'a [u8]> {
    header_fields
        .get(key)
        .map(|v| v.as_slice())
        .with_context(|| format!("missing required header field '{key}'"))
}

fn header_string<'a>(header_fields: &'a BTreeMap<String, Vec<u8>>, key: &str) -> Result<&'a str> {
    std::str::from_utf8(header_bytes(header_fields, key)?)
        .with_context(|| format!("header field '{key}' is not valid utf8"))
}

fn header_u8(header_fields: &BTreeMap<String, Vec<u8>>, key: &str) -> Result<u8> {
    let bytes = header_bytes(header_fields, key)?;
    ensure!(
        bytes.len() == 1,
        "header field '{key}' expected 1 byte, got {}",
        bytes.len()
    );
    Ok(bytes[0])
}

fn header_u32(header_fields: &BTreeMap<String, Vec<u8>>, key: &str) -> Result<u32> {
    let bytes = header_bytes(header_fields, key)?;
    ensure!(
        bytes.len() == 4,
        "header field '{key}' expected 4 bytes, got {}",
        bytes.len()
    );
    Ok(u32::from_le_bytes(
        bytes.try_into().expect("length verified"),
    ))
}

fn required_string(fields: &BTreeMap<String, Vec<u8>>, key: &str) -> Result<String> {
    let bytes = fields
        .get(key)
        .with_context(|| format!("missing required field '{key}'"))?;
    String::from_utf8(bytes.clone()).with_context(|| format!("field '{key}' is not valid utf8"))
}

fn take_required_string(fields: &mut BTreeMap<String, Vec<u8>>, key: &str) -> Result<String> {
    let bytes = fields
        .remove(key)
        .with_context(|| format!("missing required field '{key}'"))?;
    String::from_utf8(bytes).with_context(|| format!("field '{key}' is not valid utf8"))
}

fn take_required_bytes(fields: &mut BTreeMap<String, Vec<u8>>, key: &str) -> Result<Vec<u8>> {
    fields
        .remove(key)
        .with_context(|| format!("missing required field '{key}'"))
}

fn ros_time_to_nanos(raw: &[u8]) -> u64 {
    let secs = u32::from_le_bytes(raw[0..4].try_into().expect("len checked by caller"));
    let nsecs = u32::from_le_bytes(raw[4..8].try_into().expect("len checked by caller"));
    u64::from(secs) * 1_000_000_000 + u64::from(nsecs)
}

fn conn_id_to_channel_id(conn_id: u32) -> Result<u16> {
    u16::try_from(conn_id).with_context(|| format!("connection id {conn_id} exceeds u16 range"))
}

fn decompress_chunk(
    compression: &str,
    compressed: &[u8],
    expected_decompressed_size: usize,
) -> Result<Vec<u8>> {
    match compression {
        "none" => {
            ensure!(
                compressed.len() == expected_decompressed_size,
                "chunk size mismatch for uncompressed chunk: header declared {expected_decompressed_size} bytes, found {} bytes",
                compressed.len()
            );
            Ok(compressed.to_vec())
        }
        "bz2" => {
            let mut decoder = bzip2::read::BzDecoder::new(Cursor::new(compressed));
            let out = read_limited(
                &mut decoder,
                expected_decompressed_size,
                "failed to decompress bz2 ROS1 bag chunk",
            )?;
            Ok(out)
        }
        "lz4" => {
            let mut decoder = lz4::Decoder::new(Cursor::new(compressed))
                .context("failed to initialize lz4 decoder for ROS1 bag chunk")?;
            let out = read_limited(
                &mut decoder,
                expected_decompressed_size,
                "failed to decompress lz4 ROS1 bag chunk",
            )?;
            let (_reader, result) = decoder.finish();
            result.context("failed finalizing lz4 ROS1 bag chunk decoder")?;
            Ok(out)
        }
        other => bail!("unsupported ROS1 bag chunk compression '{other}'"),
    }
}

fn read_limited<R: Read>(
    reader: &mut R,
    expected_decompressed_size: usize,
    context: &'static str,
) -> Result<Vec<u8>> {
    let mut out = Vec::with_capacity(expected_decompressed_size.min(1024 * 1024));
    let mut buf = [0u8; 8192];
    loop {
        let read = reader.read(&mut buf).context(context)?;
        if read == 0 {
            break;
        }
        ensure!(
            out.len() + read <= expected_decompressed_size,
            "decompressed ROS1 chunk exceeded declared size {expected_decompressed_size}"
        );
        out.extend_from_slice(&buf[..read]);
    }
    ensure!(
        out.len() == expected_decompressed_size,
        "decompressed ROS1 chunk size mismatch: declared {expected_decompressed_size}, got {}",
        out.len()
    );
    Ok(out)
}

#[cfg(test)]
mod tests {
    use std::io::{Cursor, Read, Seek, SeekFrom, Write};
    use std::path::PathBuf;

    use anyhow::{Context, Result};

    use super::{
        decompress_chunk, parse_header_fields, process_records, read_record, BAG_MAGIC, KEY_CONN,
        KEY_MD5SUM, KEY_MESSAGE_DEFINITION, KEY_OP, KEY_TIME, KEY_TOPIC, KEY_TYPE,
        MAX_RECORD_SECTION_SIZE, OP_BAG_CONNECTION, OP_BAG_HEADER, OP_BAG_MESSAGE_DATA,
    };

    fn encode_field_bytes(key: &str, value: &[u8]) -> Vec<u8> {
        let mut field = Vec::new();
        let field_len = key.len() + 1 + value.len();
        field.extend_from_slice(&(field_len as u32).to_le_bytes());
        field.extend_from_slice(key.as_bytes());
        field.push(b'=');
        field.extend_from_slice(value);
        field
    }

    fn encode_field_string(key: &str, value: &str) -> Vec<u8> {
        encode_field_bytes(key, value.as_bytes())
    }

    fn encode_record(header_fields: Vec<Vec<u8>>, data: &[u8]) -> Vec<u8> {
        let header = header_fields.into_iter().flatten().collect::<Vec<_>>();
        let mut out = Vec::new();
        out.extend_from_slice(&(header.len() as u32).to_le_bytes());
        out.extend_from_slice(&header);
        out.extend_from_slice(&(data.len() as u32).to_le_bytes());
        out.extend_from_slice(data);
        out
    }

    fn encode_ros_time(secs: u32, nsecs: u32) -> [u8; 8] {
        let mut out = [0u8; 8];
        out[0..4].copy_from_slice(&secs.to_le_bytes());
        out[4..8].copy_from_slice(&nsecs.to_le_bytes());
        out
    }

    fn synthetic_bag_with_two_connections_same_schema() -> Vec<u8> {
        let mut bag = Vec::new();
        bag.extend_from_slice(BAG_MAGIC);
        bag.extend(encode_record(
            vec![encode_field_bytes(KEY_OP, &[OP_BAG_HEADER])],
            &[],
        ));

        let conn0_topic = "/demo0";
        let conn0_data = [
            encode_field_string(KEY_TOPIC, conn0_topic),
            encode_field_string(KEY_TYPE, "demo_msgs/Msg"),
            encode_field_string(KEY_MD5SUM, "abc123"),
            encode_field_string(KEY_MESSAGE_DEFINITION, "uint8 data\n"),
        ]
        .concat();
        bag.extend(encode_record(
            vec![
                encode_field_bytes(KEY_OP, &[OP_BAG_CONNECTION]),
                encode_field_bytes(KEY_CONN, &0u32.to_le_bytes()),
                encode_field_string(KEY_TOPIC, conn0_topic),
            ],
            &conn0_data,
        ));

        let conn1_topic = "/demo1";
        let conn1_data = [
            encode_field_string(KEY_TOPIC, conn1_topic),
            encode_field_string(KEY_TYPE, "demo_msgs/Msg"),
            encode_field_string(KEY_MD5SUM, "abc123"),
            encode_field_string(KEY_MESSAGE_DEFINITION, "uint8 data\n"),
        ]
        .concat();
        bag.extend(encode_record(
            vec![
                encode_field_bytes(KEY_OP, &[OP_BAG_CONNECTION]),
                encode_field_bytes(KEY_CONN, &1u32.to_le_bytes()),
                encode_field_string(KEY_TOPIC, conn1_topic),
            ],
            &conn1_data,
        ));

        bag.extend(encode_record(
            vec![
                encode_field_bytes(KEY_OP, &[OP_BAG_MESSAGE_DATA]),
                encode_field_bytes(KEY_CONN, &0u32.to_le_bytes()),
                encode_field_bytes(KEY_TIME, &encode_ros_time(1, 2)),
            ],
            &[0x11, 0x22],
        ));
        bag.extend(encode_record(
            vec![
                encode_field_bytes(KEY_OP, &[OP_BAG_MESSAGE_DATA]),
                encode_field_bytes(KEY_CONN, &1u32.to_le_bytes()),
                encode_field_bytes(KEY_TIME, &encode_ros_time(1, 3)),
            ],
            &[0x33, 0x44],
        ));
        bag
    }

    fn fixture_path(relative_from_repo_root: &str) -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .join(relative_from_repo_root)
    }

    #[test]
    fn parses_header_fields_with_binary_values() {
        let mut encoded = Vec::new();
        encoded.extend(encode_field_bytes("op", &[0x02]));
        encoded.extend(encode_field_bytes("topic", b"/demo"));
        let parsed = parse_header_fields(&encoded).expect("header should parse");
        assert_eq!(parsed.get("op").expect("op present"), &vec![0x02]);
        assert_eq!(parsed.get("topic").expect("topic present"), b"/demo");
    }

    #[test]
    fn read_record_returns_none_at_eof() {
        let mut reader = Cursor::new(Vec::<u8>::new());
        let record = read_record(&mut reader).expect("read should succeed");
        assert!(record.is_none());
    }

    #[test]
    fn process_records_iterates_all_records() {
        let mut bytes = Vec::new();
        for i in [1u8, 2u8] {
            let header = encode_field_bytes("op", &[i]);
            bytes.extend_from_slice(&(header.len() as u32).to_le_bytes());
            bytes.extend_from_slice(&header);
            bytes.extend_from_slice(&0u32.to_le_bytes());
        }
        let mut seen = Vec::new();
        process_records(Cursor::new(bytes), |record| {
            let parsed = parse_header_fields(&record.header)?;
            seen.push(parsed.get("op").expect("op present")[0]);
            Ok(())
        })
        .expect("process should succeed");
        assert_eq!(seen, vec![1, 2]);
    }

    #[test]
    fn read_record_rejects_oversized_header_length() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&(MAX_RECORD_SECTION_SIZE + 1).to_le_bytes());
        let mut reader = Cursor::new(bytes);
        let err = read_record(&mut reader).expect_err("oversized header should fail");
        assert!(err.to_string().contains("record header length"));
    }

    #[test]
    fn read_record_rejects_oversized_data_length() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&0u32.to_le_bytes());
        bytes.extend_from_slice(&(MAX_RECORD_SECTION_SIZE + 1).to_le_bytes());
        let mut reader = Cursor::new(bytes);
        let err = read_record(&mut reader).expect_err("oversized data should fail");
        assert!(err.to_string().contains("record data length"));
    }

    #[test]
    fn decompress_none_is_identity() {
        let data = vec![1, 2, 3, 4];
        let out = decompress_chunk("none", &data, data.len()).expect("none should succeed");
        assert_eq!(out, data);
    }

    #[test]
    fn decompress_bz2_round_trip() {
        let payload = b"hello rosbag bz2";
        let mut encoded = Vec::new();
        {
            let mut encoder =
                bzip2::write::BzEncoder::new(&mut encoded, bzip2::Compression::best());
            encoder
                .write_all(payload)
                .expect("failed to write bz2 payload");
            encoder.finish().expect("failed to finalize bz2 payload");
        }
        let out =
            decompress_chunk("bz2", &encoded, payload.len()).expect("bz2 decode should succeed");
        assert_eq!(out, payload);
    }

    #[test]
    fn decompress_lz4_round_trip() {
        let payload = b"hello rosbag lz4";
        let mut encoded = Vec::new();
        {
            let mut encoder = lz4::EncoderBuilder::new()
                .build(&mut encoded)
                .expect("failed to build lz4 encoder");
            encoder
                .write_all(payload)
                .expect("failed to write lz4 payload");
            let (_writer, result) = encoder.finish();
            result.expect("failed to finalize lz4 payload");
        }
        let out =
            decompress_chunk("lz4", &encoded, payload.len()).expect("lz4 decode should succeed");
        assert_eq!(out, payload);
    }

    #[test]
    fn rejects_unknown_compression() {
        let err = decompress_chunk("snappy", &[1, 2, 3], 3).expect_err("unsupported expected");
        assert!(err
            .to_string()
            .contains("unsupported ROS1 bag chunk compression"));
    }

    #[test]
    fn rejects_none_chunk_size_mismatch() {
        let err = decompress_chunk("none", &[1, 2, 3], 2).expect_err("size mismatch expected");
        assert!(err.to_string().contains("chunk size mismatch"));
    }

    #[test]
    fn rejects_decompressed_chunk_exceeding_declared_size() {
        let payload = b"oversized";
        let mut encoded = Vec::new();
        {
            let mut encoder =
                bzip2::write::BzEncoder::new(&mut encoded, bzip2::Compression::best());
            encoder
                .write_all(payload)
                .expect("failed to write bz2 payload");
            encoder.finish().expect("failed to finalize bz2 payload");
        }
        let err = decompress_chunk("bz2", &encoded, payload.len() - 1)
            .expect_err("oversized decompression should fail");
        assert!(err.to_string().contains("exceeded declared size"));
    }

    #[test]
    fn convert_synthetic_bag_deduplicates_schema_key() -> Result<()> {
        let input = Cursor::new(synthetic_bag_with_two_connections_same_schema());
        let mut output = Cursor::new(Vec::<u8>::new());
        super::convert_ros1_bag(
            &mut output,
            input,
            mcap::WriteOptions::new().profile("ros1").compression(None),
        )?;
        output.seek(SeekFrom::Start(0))?;
        let mut bytes = Vec::new();
        output.read_to_end(&mut bytes)?;

        let summary = mcap::Summary::read(&bytes)?.expect("expected summary");
        assert_eq!(summary.schemas.len(), 1);
        assert_eq!(summary.channels.len(), 2);
        assert!(summary.channels.contains_key(&0));
        assert!(summary.channels.contains_key(&1));
        let channel = summary
            .channels
            .values()
            .next()
            .expect("channel should exist");
        assert_eq!(channel.message_encoding, "ros1");
        assert!(channel.metadata.contains_key("md5sum"));

        let messages = mcap::MessageStream::new(&bytes)?.collect::<mcap::McapResult<Vec<_>>>()?;
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0].log_time, 1_000_000_002);
        assert_eq!(messages[1].log_time, 1_000_000_003);
        Ok(())
    }

    #[test]
    fn convert_real_bz2_ros1_bag_fixture() -> Result<()> {
        let fixture = fixture_path("go/ros/testdata/markers.bz2.bag");
        let input =
            std::fs::File::open(&fixture).context("failed to open markers.bz2.bag fixture")?;
        let mut output = Cursor::new(Vec::<u8>::new());
        super::convert_ros1_bag(
            &mut output,
            input,
            mcap::WriteOptions::new().profile("ros1").compression(None),
        )?;
        output.seek(SeekFrom::Start(0))?;
        let mut bytes = Vec::new();
        output.read_to_end(&mut bytes)?;

        let mut records = mcap::read::LinearReader::new(&bytes)?;
        match records.next() {
            Some(Ok(mcap::records::Record::Header(header))) => assert_eq!(header.profile, "ros1"),
            other => panic!("expected MCAP header as first record, got {other:?}"),
        }

        let summary = mcap::Summary::read(&bytes)?.expect("expected summary");
        assert!(!summary.channels.is_empty());
        assert!(summary
            .schemas
            .values()
            .all(|schema| schema.encoding == "ros1msg"));
        assert!(summary
            .channels
            .values()
            .all(|channel| channel.message_encoding == "ros1"));

        let message_count = mcap::MessageStream::new(&bytes)?
            .collect::<mcap::McapResult<Vec<_>>>()?
            .len();
        assert_eq!(message_count, 10);
        Ok(())
    }

    #[test]
    fn convert_real_uncompressed_ros1_bag_fixture() -> Result<()> {
        let fixture = fixture_path("testdata/bags/demo.bag");
        let input = std::fs::File::open(&fixture).context("failed to open demo.bag fixture")?;
        let mut output = Cursor::new(Vec::<u8>::new());
        super::convert_ros1_bag(
            &mut output,
            input,
            mcap::WriteOptions::new().profile("ros1").compression(None),
        )?;
        output.seek(SeekFrom::Start(0))?;
        let mut bytes = Vec::new();
        output.read_to_end(&mut bytes)?;

        let summary = mcap::Summary::read(&bytes)?.expect("expected summary");
        assert!(!summary.channels.is_empty());
        assert!(!summary.schemas.is_empty());
        assert!(!mcap::MessageStream::new(&bytes)?
            .collect::<mcap::McapResult<Vec<_>>>()?
            .is_empty());
        Ok(())
    }
}
