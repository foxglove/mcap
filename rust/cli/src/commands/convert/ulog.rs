//! Convert PX4 ULog (`.ulg`) files to MCAP.
//!
//! ULog is a self-describing binary stream: a fixed header, a definitions
//! section (message formats, parameters, info), and a data section (a flat
//! stream of typed messages). See the format spec at
//! <https://docs.px4.io/main/en/dev_log/ulog_file_format>.
//!
//! The conversion maps ULog concepts onto MCAP as follows:
//! - uORB data topics (`D` records, described by `F` format definitions) become
//!   protobuf messages on a channel per subscription. The schema is named
//!   `px4.<message_name>` and the topic is `<message_name>/<multi_id>`.
//! - Logged strings (`L`/`C` records) are funneled onto a single `log_message`
//!   topic using the `px4.log_message` schema, preserving the raw kernel
//!   `severity` (0-7) and the tagged-message `tag`.
//! - Parameters (`P`/`Q` records) become `px4.parameter` messages on a
//!   `parameters` topic: a snapshot of the initial values at log start, plus a
//!   message per runtime change.
//! - Info (`I`/`M` records) is written to a single MCAP metadata record.

use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufWriter, Read, Seek, Write};
use std::path::Path;

use anyhow::{bail, ensure, Context, Result};
use mcap::records::Metadata;
use prost_reflect::prost::Message as _;
use prost_reflect::prost_types::{
    field_descriptor_proto::{Label, Type},
    DescriptorProto, FieldDescriptorProto, FileDescriptorProto, FileDescriptorSet,
};
use prost_reflect::{DescriptorPool, DynamicMessage, Kind, MessageDescriptor, Value};

const ULOG_MAGIC: [u8; 7] = [0x55, 0x4c, 0x6f, 0x67, 0x01, 0x12, 0x35];
const HEADER_LEN: usize = 16;

const PACKAGE: &str = "px4";
const PROTO_FILE_NAME: &str = "px4_ulog.proto";

const LOG_TOPIC: &str = "log_message";
const LOG_MESSAGE_NAME: &str = "log_message";
const PARAM_TOPIC: &str = "parameters";
const PARAM_MESSAGE_NAME: &str = "parameter";
const INFO_METADATA_NAME: &str = "info";

const PROTOBUF_ENCODING: &str = "protobuf";

/// A single field in a ULog message format definition.
#[derive(Debug, Clone, PartialEq, Eq)]
struct FieldDef {
    ulog_type: String,
    /// Fixed array length, or `0` for a scalar field.
    array_len: usize,
    name: String,
}

impl FieldDef {
    fn is_padding(&self) -> bool {
        self.name.starts_with("_padding")
    }
}

/// A ULog message format (`F` record): a named composite type.
#[derive(Debug, Clone, PartialEq, Eq)]
struct FormatDef {
    name: String,
    fields: Vec<FieldDef>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Primitive {
    Int8,
    UInt8,
    Int16,
    UInt16,
    Int32,
    UInt32,
    Int64,
    UInt64,
    Float,
    Double,
    Bool,
    Char,
}

impl Primitive {
    fn parse(ulog_type: &str) -> Option<Self> {
        Some(match ulog_type {
            "int8_t" => Self::Int8,
            "uint8_t" => Self::UInt8,
            "int16_t" => Self::Int16,
            "uint16_t" => Self::UInt16,
            "int32_t" => Self::Int32,
            "uint32_t" => Self::UInt32,
            "int64_t" => Self::Int64,
            "uint64_t" => Self::UInt64,
            "float" => Self::Float,
            "double" => Self::Double,
            "bool" => Self::Bool,
            "char" => Self::Char,
            _ => return None,
        })
    }

    fn size(self) -> usize {
        match self {
            Self::Int8 | Self::UInt8 | Self::Bool | Self::Char => 1,
            Self::Int16 | Self::UInt16 => 2,
            Self::Int32 | Self::UInt32 | Self::Float => 4,
            Self::Int64 | Self::UInt64 | Self::Double => 8,
        }
    }

    /// The proto3 field type. `char` is mapped at the field level to `string`,
    /// so it is reported here as its byte representation only for sizing.
    fn proto_type(self) -> Type {
        match self {
            // proto3 lacks 8/16-bit integers, so narrow widths widen to 32-bit.
            Self::Int8 | Self::Int16 | Self::Int32 => Type::Int32,
            Self::UInt8 | Self::UInt16 | Self::UInt32 => Type::Uint32,
            Self::Int64 => Type::Int64,
            Self::UInt64 => Type::Uint64,
            Self::Float => Type::Float,
            Self::Double => Type::Double,
            Self::Bool => Type::Bool,
            Self::Char => Type::String,
        }
    }
}

pub fn convert_ulog_file(
    input_path: &Path,
    output_path: &Path,
    write_options: mcap::WriteOptions,
) -> Result<()> {
    let mut input = File::open(input_path)
        .with_context(|| format!("failed to open input '{}'", input_path.display()))?;
    let start_timestamp = read_and_validate_header(&mut input)?;
    let output = File::create(output_path)
        .with_context(|| format!("failed to open output '{}'", output_path.display()))?;
    convert_validated_ulog(
        BufWriter::new(output),
        input,
        start_timestamp,
        write_options,
    )
}

fn read_and_validate_header<R: Read>(input: &mut R) -> Result<u64> {
    let mut header = [0u8; HEADER_LEN];
    input
        .read_exact(&mut header)
        .context("failed to read ULog header")?;
    ensure!(
        header[..ULOG_MAGIC.len()] == ULOG_MAGIC,
        "invalid ULog magic (expected a PX4 .ulg file)"
    );
    let start_timestamp = u64::from_le_bytes(
        header[8..16]
            .try_into()
            .expect("slice length verified above"),
    );
    Ok(start_timestamp)
}

/// Tracks conversion state as the ULog message stream is consumed.
struct Converter<W: Write + Seek> {
    writer: mcap::Writer<W>,
    /// Microseconds since system start at which logging began.
    start_timestamp_us: u64,
    /// The highest data/log timestamp seen so far, used to stamp parameter
    /// changes (which carry no timestamp of their own).
    last_timestamp_us: u64,
    in_definitions: bool,
    formats: BTreeMap<String, FormatDef>,
    /// Built once when the definitions section ends.
    pool: Option<DescriptorPool>,
    /// Serialized `FileDescriptorSet` reused as schema `data` for every schema.
    descriptor_set: Vec<u8>,
    subscriptions: BTreeMap<u16, Subscription>,
    schema_ids: BTreeMap<String, u16>,
    log_channel: Option<(u16, MessageDescriptor)>,
    param_channel: Option<(u16, MessageDescriptor)>,
    initial_params: Vec<(String, f64)>,
    sequences: BTreeMap<u16, u32>,
    info: BTreeMap<String, String>,
}

struct Subscription {
    channel_id: u16,
    descriptor: MessageDescriptor,
    format_name: String,
}

fn convert_validated_ulog<W: Write + Seek, R: Read>(
    output: W,
    mut input: R,
    start_timestamp_us: u64,
    write_options: mcap::WriteOptions,
) -> Result<()> {
    let writer = write_options
        .create(output)
        .context("failed to create MCAP writer")?;
    let mut converter = Converter {
        writer,
        start_timestamp_us,
        last_timestamp_us: start_timestamp_us,
        in_definitions: true,
        formats: BTreeMap::new(),
        pool: None,
        descriptor_set: Vec::new(),
        subscriptions: BTreeMap::new(),
        schema_ids: BTreeMap::new(),
        log_channel: None,
        param_channel: None,
        initial_params: Vec::new(),
        sequences: BTreeMap::new(),
        info: BTreeMap::new(),
    };

    while let Some((msg_type, body)) = read_message(&mut input)? {
        converter.handle_message(msg_type, &body)?;
    }

    // Flush definitions even if the file had no data-section messages, so that
    // initial parameters and info are still emitted.
    converter.finalize_definitions()?;
    converter.write_info_metadata()?;
    converter
        .writer
        .finish()
        .context("failed to finalize MCAP writer")?;
    Ok(())
}

impl<W: Write + Seek> Converter<W> {
    fn handle_message(&mut self, msg_type: u8, body: &[u8]) -> Result<()> {
        match msg_type {
            b'B' => parse_flag_bits(body)?,
            b'F' => {
                let format = parse_format(body)?;
                self.formats.insert(format.name.clone(), format);
            }
            b'I' => {
                if let (key, Some(value)) = parse_info(body)? {
                    self.info.insert(key, value);
                }
            }
            b'M' => {
                let (key, value, is_continued) = parse_info_multi(body)?;
                if let Some(value) = value {
                    if is_continued {
                        self.info.entry(key).or_default().push_str(&value);
                    } else {
                        self.info.insert(key, value);
                    }
                }
            }
            b'P' | b'Q' => {
                let (name, value) = parse_param(msg_type, body)?;
                if self.in_definitions {
                    self.initial_params.push((name, value));
                } else {
                    let ts = self.last_timestamp_us;
                    self.write_param(name, value, ts)?;
                }
            }
            b'A' => {
                self.finalize_definitions()?;
                self.add_subscription(body)?;
            }
            b'D' => {
                self.finalize_definitions()?;
                self.write_data(body)?;
            }
            b'L' => {
                self.finalize_definitions()?;
                self.write_log(body, false)?;
            }
            b'C' => {
                self.finalize_definitions()?;
                self.write_log(body, true)?;
            }
            // Unsubscribe, sync, dropout, and any unknown/future type carry no
            // data we convert; per the spec, unknown types are ignored.
            _ => {}
        }
        Ok(())
    }

    /// Build the descriptor pool from the collected formats and flush initial
    /// parameters. Idempotent: only the first call does work.
    fn finalize_definitions(&mut self) -> Result<()> {
        if !self.in_definitions {
            return Ok(());
        }
        self.in_definitions = false;

        let file = build_file_descriptor(&self.formats)?;
        self.descriptor_set = FileDescriptorSet {
            file: vec![file.clone()],
        }
        .encode_to_vec();
        let pool = DescriptorPool::from_file_descriptor_set(FileDescriptorSet { file: vec![file] })
            .context("failed to build protobuf descriptors from ULog formats")?;
        self.pool = Some(pool);

        let initial = std::mem::take(&mut self.initial_params);
        let start = self.start_timestamp_us;
        for (name, value) in initial {
            self.write_param(name, value, start)?;
        }
        Ok(())
    }

    fn pool(&self) -> &DescriptorPool {
        self.pool.as_ref().expect("pool built before data section")
    }

    fn message_descriptor(&self, full_name: &str) -> Result<MessageDescriptor> {
        self.pool()
            .get_message_by_name(full_name)
            .with_context(|| format!("missing protobuf descriptor for '{full_name}'"))
    }

    fn add_subscription(&mut self, body: &[u8]) -> Result<()> {
        ensure!(body.len() >= 3, "ULog subscription message too short");
        let multi_id = body[0];
        let msg_id = u16::from_le_bytes([body[1], body[2]]);
        let message_name = String::from_utf8(body[3..].to_vec())
            .context("ULog subscription message_name is not valid utf8")?;

        ensure!(
            self.formats.contains_key(&message_name),
            "ULog subscription references unknown message format '{message_name}'"
        );

        let full_name = schema_full_name(&message_name);
        let schema_id = if let Some(id) = self.schema_ids.get(&full_name) {
            *id
        } else {
            let id = self
                .writer
                .add_schema(&full_name, PROTOBUF_ENCODING, &self.descriptor_set)
                .with_context(|| format!("failed to write schema for '{message_name}'"))?;
            self.schema_ids.insert(full_name.clone(), id);
            id
        };

        let topic = format!("{message_name}/{multi_id}");
        let metadata = BTreeMap::from([("multi_id".to_string(), multi_id.to_string())]);
        let channel_id = self
            .writer
            .add_channel(schema_id, &topic, PROTOBUF_ENCODING, &metadata)
            .with_context(|| format!("failed to write channel for topic '{topic}'"))?;

        let descriptor = self.message_descriptor(&full_name)?;
        self.subscriptions.insert(
            msg_id,
            Subscription {
                channel_id,
                descriptor,
                format_name: message_name,
            },
        );
        Ok(())
    }

    fn write_data(&mut self, body: &[u8]) -> Result<()> {
        ensure!(body.len() >= 2, "ULog data message too short");
        let msg_id = u16::from_le_bytes([body[0], body[1]]);
        let payload = &body[2..];

        let subscription = self
            .subscriptions
            .get(&msg_id)
            .with_context(|| format!("ULog data references unknown subscription id {msg_id}"))?;
        let channel_id = subscription.channel_id;
        let descriptor = subscription.descriptor.clone();
        let format_name = subscription.format_name.clone();
        let format = self
            .formats
            .get(&format_name)
            .expect("subscription format validated at subscription time")
            .clone();

        let mut reader = ByteReader::new(payload);
        let message = decode_message(&self.formats, &descriptor, &format, &mut reader, true)
            .with_context(|| format!("failed to decode ULog message '{format_name}'"))?;

        let timestamp_us = message
            .get_field_by_name("timestamp")
            .and_then(|value| value.as_u64())
            .unwrap_or(self.last_timestamp_us);
        self.last_timestamp_us = self.last_timestamp_us.max(timestamp_us);

        let encoded = message.encode_to_vec();
        self.write_message(channel_id, timestamp_us, &encoded)
    }

    fn write_log(&mut self, body: &[u8], tagged: bool) -> Result<()> {
        // Untagged 'L': log_level(1) timestamp(8) message
        // Tagged   'C': log_level(1) tag(2) timestamp(8) message
        let header_len = if tagged { 11 } else { 9 };
        ensure!(
            body.len() >= header_len,
            "ULog logged string message too short"
        );
        // ULog stores log_level as the ASCII digit '0'-'7'; PX4's uORB
        // log_message.severity is the integer kernel level 0-7, so normalize.
        let severity = kernel_severity(body[0]);
        let (tag, ts_offset) = if tagged {
            (u16::from_le_bytes([body[1], body[2]]), 3)
        } else {
            (0u16, 1)
        };
        let timestamp_us = u64::from_le_bytes(
            body[ts_offset..ts_offset + 8]
                .try_into()
                .expect("8 bytes verified above"),
        );
        let text = String::from_utf8_lossy(&body[header_len..]).into_owned();
        self.last_timestamp_us = self.last_timestamp_us.max(timestamp_us);

        let (channel_id, descriptor) = self.ensure_log_channel()?;
        let mut message = DynamicMessage::new(descriptor);
        set_field(&mut message, "timestamp", Value::U64(timestamp_us))?;
        set_field(&mut message, "severity", Value::U32(severity))?;
        set_field(&mut message, "text", Value::String(text))?;
        set_field(&mut message, "tag", Value::U32(u32::from(tag)))?;

        let encoded = message.encode_to_vec();
        self.write_message(channel_id, timestamp_us, &encoded)
    }

    fn write_param(&mut self, name: String, value: f64, timestamp_us: u64) -> Result<()> {
        let (channel_id, descriptor) = self.ensure_param_channel()?;
        let mut message = DynamicMessage::new(descriptor);
        set_field(&mut message, "name", Value::String(name))?;
        set_field(&mut message, "value", Value::F64(value))?;
        let encoded = message.encode_to_vec();
        self.write_message(channel_id, timestamp_us, &encoded)
    }

    fn ensure_log_channel(&mut self) -> Result<(u16, MessageDescriptor)> {
        if let Some((channel_id, descriptor)) = &self.log_channel {
            return Ok((*channel_id, descriptor.clone()));
        }
        let full_name = schema_full_name(LOG_MESSAGE_NAME);
        let schema_id = self
            .writer
            .add_schema(&full_name, PROTOBUF_ENCODING, &self.descriptor_set)
            .context("failed to write px4.log_message schema")?;
        let channel_id = self
            .writer
            .add_channel(schema_id, LOG_TOPIC, PROTOBUF_ENCODING, &BTreeMap::new())
            .context("failed to write log_message channel")?;
        let descriptor = self.message_descriptor(&full_name)?;
        self.log_channel = Some((channel_id, descriptor.clone()));
        Ok((channel_id, descriptor))
    }

    fn ensure_param_channel(&mut self) -> Result<(u16, MessageDescriptor)> {
        if let Some((channel_id, descriptor)) = &self.param_channel {
            return Ok((*channel_id, descriptor.clone()));
        }
        let full_name = schema_full_name(PARAM_MESSAGE_NAME);
        let schema_id = self
            .writer
            .add_schema(&full_name, PROTOBUF_ENCODING, &self.descriptor_set)
            .context("failed to write px4.parameter schema")?;
        let channel_id = self
            .writer
            .add_channel(schema_id, PARAM_TOPIC, PROTOBUF_ENCODING, &BTreeMap::new())
            .context("failed to write parameters channel")?;
        let descriptor = self.message_descriptor(&full_name)?;
        self.param_channel = Some((channel_id, descriptor.clone()));
        Ok((channel_id, descriptor))
    }

    fn write_message(&mut self, channel_id: u16, timestamp_us: u64, data: &[u8]) -> Result<()> {
        let log_time = timestamp_us.saturating_mul(1000);
        let sequence = self.sequences.entry(channel_id).or_insert(0);
        let current = *sequence;
        *sequence = sequence.wrapping_add(1);
        self.writer
            .write_to_known_channel(
                &mcap::records::MessageHeader {
                    channel_id,
                    sequence: current,
                    log_time,
                    publish_time: log_time,
                },
                data,
            )
            .context("failed to write converted ULog message")
    }

    fn write_info_metadata(&mut self) -> Result<()> {
        if self.info.is_empty() {
            return Ok(());
        }
        let metadata = Metadata {
            name: INFO_METADATA_NAME.to_string(),
            metadata: self.info.clone().into_iter().collect(),
        };
        self.writer
            .write_metadata(&metadata)
            .context("failed to write ULog info metadata")
    }
}

/// Build a single proto3 file containing a message per ULog format plus the
/// synthetic `log_message` and `parameter` messages.
fn build_file_descriptor(formats: &BTreeMap<String, FormatDef>) -> Result<FileDescriptorProto> {
    let mut message_types = Vec::new();
    for format in formats.values() {
        message_types.push(build_descriptor_proto(format)?);
    }
    message_types.push(build_log_descriptor());
    message_types.push(build_param_descriptor());

    Ok(FileDescriptorProto {
        name: Some(PROTO_FILE_NAME.to_string()),
        package: Some(PACKAGE.to_string()),
        message_type: message_types,
        syntax: Some("proto3".to_string()),
        ..Default::default()
    })
}

fn build_descriptor_proto(format: &FormatDef) -> Result<DescriptorProto> {
    let mut fields = Vec::new();
    let mut number = 1i32;
    for field in &format.fields {
        if field.is_padding() {
            continue;
        }
        let mut proto = FieldDescriptorProto {
            name: Some(field.name.clone()),
            number: Some(number),
            ..Default::default()
        };
        number += 1;

        if let Some(primitive) = Primitive::parse(&field.ulog_type) {
            proto.set_type(primitive.proto_type());
            // char[] (and a lone char) map to a single proto3 string.
            let repeated = field.array_len > 0 && primitive != Primitive::Char;
            proto.set_label(if repeated {
                Label::Repeated
            } else {
                Label::Optional
            });
        } else {
            proto.set_type(Type::Message);
            proto.type_name = Some(format!(".{}", schema_full_name(&field.ulog_type)));
            proto.set_label(if field.array_len > 0 {
                Label::Repeated
            } else {
                Label::Optional
            });
        }
        fields.push(proto);
    }

    Ok(DescriptorProto {
        name: Some(sanitize_identifier(&format.name)),
        field: fields,
        ..Default::default()
    })
}

fn build_log_descriptor() -> DescriptorProto {
    DescriptorProto {
        name: Some(sanitize_identifier(LOG_MESSAGE_NAME)),
        field: vec![
            scalar_field("timestamp", 1, Type::Uint64),
            scalar_field("severity", 2, Type::Uint32),
            scalar_field("text", 3, Type::String),
            scalar_field("tag", 4, Type::Uint32),
        ],
        ..Default::default()
    }
}

fn build_param_descriptor() -> DescriptorProto {
    DescriptorProto {
        name: Some(sanitize_identifier(PARAM_MESSAGE_NAME)),
        field: vec![
            scalar_field("name", 1, Type::String),
            scalar_field("value", 2, Type::Double),
        ],
        ..Default::default()
    }
}

fn scalar_field(name: &str, number: i32, ty: Type) -> FieldDescriptorProto {
    let mut proto = FieldDescriptorProto {
        name: Some(name.to_string()),
        number: Some(number),
        ..Default::default()
    };
    proto.set_type(ty);
    proto.set_label(Label::Optional);
    proto
}

fn schema_full_name(message_name: &str) -> String {
    format!("{PACKAGE}.{}", sanitize_identifier(message_name))
}

/// Replace characters not allowed in protobuf identifiers. ULog message names
/// allow `-` and `/`, which protobuf does not.
fn sanitize_identifier(name: &str) -> String {
    name.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

/// Decode a ULog binary payload into a `DynamicMessage` using the format
/// definition for layout and the descriptor for proto field types.
fn decode_message(
    formats: &BTreeMap<String, FormatDef>,
    descriptor: &MessageDescriptor,
    format: &FormatDef,
    reader: &mut ByteReader,
    top_level: bool,
) -> Result<DynamicMessage> {
    let mut message = DynamicMessage::new(descriptor.clone());
    for field in &format.fields {
        if field.is_padding() {
            let size = padding_size(formats, field);
            if top_level && reader.remaining() < size {
                // Trailing padding may be trimmed from the logged data.
                reader.skip(reader.remaining());
            } else {
                reader.skip_exact(size).with_context(|| {
                    format!("not enough bytes for padding field '{}'", field.name)
                })?;
            }
            continue;
        }

        if let Some(primitive) = Primitive::parse(&field.ulog_type) {
            let value = decode_primitive_field(primitive, field.array_len, reader)
                .with_context(|| format!("failed to decode field '{}'", field.name))?;
            set_field(&mut message, &field.name, value)?;
        } else {
            let nested_format = formats.get(&field.ulog_type).with_context(|| {
                format!(
                    "field '{}' references unknown type '{}'",
                    field.name, field.ulog_type
                )
            })?;
            let nested_descriptor =
                match descriptor.get_field_by_name(&field.name).map(|f| f.kind()) {
                    Some(Kind::Message(md)) => md,
                    _ => bail!(
                        "field '{}' is not a nested message in the descriptor",
                        field.name
                    ),
                };
            let value = if field.array_len > 0 {
                let mut items = Vec::with_capacity(field.array_len);
                for _ in 0..field.array_len {
                    let nested =
                        decode_message(formats, &nested_descriptor, nested_format, reader, false)?;
                    items.push(Value::Message(nested));
                }
                Value::List(items)
            } else {
                Value::Message(decode_message(
                    formats,
                    &nested_descriptor,
                    nested_format,
                    reader,
                    false,
                )?)
            };
            set_field(&mut message, &field.name, value)?;
        }
    }
    Ok(message)
}

fn decode_primitive_field(
    primitive: Primitive,
    array_len: usize,
    reader: &mut ByteReader,
) -> Result<Value> {
    if primitive == Primitive::Char {
        // A lone char and char[] both become a single string.
        let len = array_len.max(1);
        let bytes = reader.read_bytes(len)?;
        let end = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());
        return Ok(Value::String(
            String::from_utf8_lossy(&bytes[..end]).into_owned(),
        ));
    }
    if array_len > 0 {
        let mut items = Vec::with_capacity(array_len);
        for _ in 0..array_len {
            items.push(decode_scalar(primitive, reader)?);
        }
        Ok(Value::List(items))
    } else {
        decode_scalar(primitive, reader)
    }
}

fn decode_scalar(primitive: Primitive, reader: &mut ByteReader) -> Result<Value> {
    Ok(match primitive {
        Primitive::Int8 => Value::I32(i32::from(reader.read_bytes(1)?[0] as i8)),
        Primitive::UInt8 => Value::U32(u32::from(reader.read_bytes(1)?[0])),
        Primitive::Int16 => Value::I32(i32::from(i16::from_le_bytes(reader.read_array()?))),
        Primitive::UInt16 => Value::U32(u32::from(u16::from_le_bytes(reader.read_array()?))),
        Primitive::Int32 => Value::I32(i32::from_le_bytes(reader.read_array()?)),
        Primitive::UInt32 => Value::U32(u32::from_le_bytes(reader.read_array()?)),
        Primitive::Int64 => Value::I64(i64::from_le_bytes(reader.read_array()?)),
        Primitive::UInt64 => Value::U64(u64::from_le_bytes(reader.read_array()?)),
        Primitive::Float => Value::F32(f32::from_le_bytes(reader.read_array()?)),
        Primitive::Double => Value::F64(f64::from_le_bytes(reader.read_array()?)),
        Primitive::Bool => Value::Bool(reader.read_bytes(1)?[0] != 0),
        Primitive::Char => unreachable!("char handled by decode_primitive_field"),
    })
}

fn padding_size(formats: &BTreeMap<String, FormatDef>, field: &FieldDef) -> usize {
    let count = field.array_len.max(1);
    if let Some(primitive) = Primitive::parse(&field.ulog_type) {
        primitive.size() * count
    } else if let Some(nested) = formats.get(&field.ulog_type) {
        nested
            .fields
            .iter()
            .map(|f| padding_size(formats, f))
            .sum::<usize>()
            * count
    } else {
        0
    }
}

fn set_field(message: &mut DynamicMessage, name: &str, value: Value) -> Result<()> {
    message
        .try_set_field_by_name(name, value)
        .with_context(|| format!("failed to set protobuf field '{name}'"))
}

/// ULog logged-string records store the kernel log level as the ASCII digit
/// `'0'`-`'7'`. Normalize to the integer 0-7 used by PX4's `log_message`
/// uORB topic; pass through any other byte unchanged.
fn kernel_severity(raw: u8) -> u32 {
    if raw.is_ascii_digit() {
        u32::from(raw - b'0')
    } else {
        u32::from(raw)
    }
}

fn parse_flag_bits(body: &[u8]) -> Result<()> {
    // compat_flags[8], incompat_flags[8], appended_offsets[3]; trailing bytes
    // from future revisions are ignored.
    ensure!(body.len() >= 16, "ULog flag bits message too short");
    let incompat = &body[8..16];
    // Bit 0 of incompat_flags[0] is DATA_APPENDED, which we tolerate by reading
    // the stream sequentially. Any other set bit is an unknown breaking change.
    let unknown = incompat[0] & !0x01 != 0 || incompat[1..].iter().any(|&b| b != 0);
    ensure!(
        !unknown,
        "ULog file sets unsupported incompatible flag bits and cannot be converted"
    );
    Ok(())
}

fn parse_format(body: &[u8]) -> Result<FormatDef> {
    let text = std::str::from_utf8(body).context("ULog format message is not valid utf8")?;
    let (name, fields_str) = text
        .split_once(':')
        .with_context(|| format!("ULog format message missing ':' separator: '{text}'"))?;
    ensure!(!name.is_empty(), "ULog format message has empty name");

    let mut fields = Vec::new();
    for field_spec in fields_str.split(';') {
        if field_spec.trim().is_empty() {
            continue;
        }
        fields.push(parse_field(field_spec)?);
    }
    ensure!(!fields.is_empty(), "ULog format '{name}' has no fields");
    Ok(FormatDef {
        name: name.to_string(),
        fields,
    })
}

/// Parse a `type field_name` or `type[N] field_name` spec (used by formats,
/// info keys, and parameter keys).
fn parse_field(spec: &str) -> Result<FieldDef> {
    let mut tokens = spec.split_whitespace();
    let type_token = tokens
        .next()
        .with_context(|| format!("ULog field spec missing type: '{spec}'"))?;
    let name = tokens
        .next()
        .with_context(|| format!("ULog field spec missing name: '{spec}'"))?;

    let (ulog_type, array_len) = match type_token.split_once('[') {
        Some((base, rest)) => {
            let len_str = rest
                .strip_suffix(']')
                .with_context(|| format!("ULog field spec has malformed array: '{spec}'"))?;
            let len: usize = len_str
                .parse()
                .with_context(|| format!("ULog field spec has invalid array length: '{spec}'"))?;
            (base.to_string(), len)
        }
        None => (type_token.to_string(), 0),
    };

    Ok(FieldDef {
        ulog_type,
        array_len,
        name: name.to_string(),
    })
}

fn parse_info(body: &[u8]) -> Result<(String, Option<String>)> {
    ensure!(!body.is_empty(), "ULog info message too short");
    let key_len = body[0] as usize;
    ensure!(body.len() > key_len, "ULog info message truncated key");
    let key =
        std::str::from_utf8(&body[1..1 + key_len]).context("ULog info key is not valid utf8")?;
    let value_bytes = &body[1 + key_len..];
    decode_keyed_value(key, value_bytes)
}

fn parse_info_multi(body: &[u8]) -> Result<(String, Option<String>, bool)> {
    ensure!(body.len() >= 2, "ULog multi info message too short");
    let is_continued = body[0] != 0;
    let key_len = body[1] as usize;
    ensure!(
        body.len() >= 2 + key_len,
        "ULog multi info message truncated key"
    );
    let key = std::str::from_utf8(&body[2..2 + key_len])
        .context("ULog multi info key is not valid utf8")?;
    let value_bytes = &body[2 + key_len..];
    let (name, value) = decode_keyed_value(key, value_bytes)?;
    Ok((name, value, is_continued))
}

/// Decode a `key`/`value` pair where the key is `type name`. Returns the bare
/// name and a stringified value, or `None` for values without a useful textual
/// representation (e.g. binary `uint8[]` blobs like `metadata_events`).
fn decode_keyed_value(key: &str, value_bytes: &[u8]) -> Result<(String, Option<String>)> {
    let field = parse_field(key)?;
    let value = stringify_value(&field, value_bytes)?;
    Ok((field.name, value))
}

fn stringify_value(field: &FieldDef, bytes: &[u8]) -> Result<Option<String>> {
    let Some(primitive) = Primitive::parse(&field.ulog_type) else {
        return Ok(None);
    };
    if primitive == Primitive::Char {
        let end = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());
        return Ok(Some(String::from_utf8_lossy(&bytes[..end]).into_owned()));
    }
    // Numeric arrays are typically binary blobs (e.g. `metadata_events`) with no
    // useful textual form; only scalars are kept as metadata.
    if field.array_len > 1 {
        return Ok(None);
    }
    let mut reader = ByteReader::new(bytes);
    Ok(Some(scalar_to_string(decode_scalar(
        primitive,
        &mut reader,
    )?)))
}

fn scalar_to_string(value: Value) -> String {
    match value {
        Value::I32(v) => v.to_string(),
        Value::U32(v) => v.to_string(),
        Value::I64(v) => v.to_string(),
        Value::U64(v) => v.to_string(),
        Value::F32(v) => v.to_string(),
        Value::F64(v) => v.to_string(),
        Value::Bool(v) => v.to_string(),
        other => format!("{other:?}"),
    }
}

fn parse_param(msg_type: u8, body: &[u8]) -> Result<(String, f64)> {
    // 'P' shares the info layout; 'Q' (default parameter) has an extra leading
    // default_types byte.
    let (key_len_offset, value_offset_extra) = if msg_type == b'Q' { (1, 1) } else { (0, 0) };
    ensure!(
        body.len() > key_len_offset,
        "ULog parameter message too short"
    );
    let key_len = body[key_len_offset] as usize;
    let key_start = key_len_offset + 1;
    ensure!(
        body.len() >= key_start + key_len,
        "ULog parameter message truncated key"
    );
    let key = std::str::from_utf8(&body[key_start..key_start + key_len])
        .context("ULog parameter key is not valid utf8")?;
    let value_bytes = &body[key_start + key_len..];
    let _ = value_offset_extra;

    let field = parse_field(key)?;
    let value = match field.ulog_type.as_str() {
        "int32_t" => {
            ensure!(value_bytes.len() >= 4, "ULog int32 parameter too short");
            i32::from_le_bytes(value_bytes[..4].try_into().expect("4 bytes verified")) as f64
        }
        "float" => {
            ensure!(value_bytes.len() >= 4, "ULog float parameter too short");
            f32::from_le_bytes(value_bytes[..4].try_into().expect("4 bytes verified")) as f64
        }
        other => bail!(
            "unsupported ULog parameter type '{other}' for '{}'",
            field.name
        ),
    };
    Ok((field.name, value))
}

fn read_message<R: Read>(reader: &mut R) -> Result<Option<(u8, Vec<u8>)>> {
    let mut header = [0u8; 3];
    match read_full(reader, &mut header)? {
        0 => return Ok(None),
        3 => {}
        n => bail!("truncated ULog message header ({n} of 3 bytes)"),
    }
    let msg_size = u16::from_le_bytes([header[0], header[1]]) as usize;
    let msg_type = header[2];
    let mut body = vec![0u8; msg_size];
    reader
        .read_exact(&mut body)
        .context("failed to read ULog message body")?;
    Ok(Some((msg_type, body)))
}

fn read_full<R: Read>(reader: &mut R, buffer: &mut [u8]) -> Result<usize> {
    let mut filled = 0;
    while filled < buffer.len() {
        match reader.read(&mut buffer[filled..]) {
            Ok(0) => break,
            Ok(read) => filled += read,
            Err(err) if err.kind() == std::io::ErrorKind::Interrupted => {}
            Err(err) => return Err(err).context("failed to read ULog stream"),
        }
    }
    Ok(filled)
}

/// A cursor over a byte slice for decoding fixed-layout binary payloads.
struct ByteReader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> ByteReader<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    fn remaining(&self) -> usize {
        self.buf.len() - self.pos
    }

    fn read_bytes(&mut self, n: usize) -> Result<&'a [u8]> {
        ensure!(
            self.remaining() >= n,
            "unexpected end of ULog message payload (needed {n}, had {})",
            self.remaining()
        );
        let slice = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(slice)
    }

    fn read_array<const N: usize>(&mut self) -> Result<[u8; N]> {
        Ok(self
            .read_bytes(N)?
            .try_into()
            .expect("slice length verified by read_bytes"))
    }

    fn skip(&mut self, n: usize) {
        self.pos = (self.pos + n).min(self.buf.len());
    }

    fn skip_exact(&mut self, n: usize) -> Result<()> {
        self.read_bytes(n)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::io::{Cursor, Read, Seek, SeekFrom};
    use std::path::PathBuf;

    use anyhow::Result;
    use prost_reflect::{DescriptorPool, DynamicMessage};

    use super::{convert_validated_ulog, read_and_validate_header, ULOG_MAGIC};

    fn write_options() -> mcap::WriteOptions {
        mcap::WriteOptions::new()
            .profile("px4")
            .compression(None)
            .chunk_size(Some(1024))
    }

    fn ulog_header(start_timestamp_us: u64) -> Vec<u8> {
        let mut header = ULOG_MAGIC.to_vec();
        header.push(1); // version
        header.extend_from_slice(&start_timestamp_us.to_le_bytes());
        header
    }

    fn message(msg_type: u8, body: &[u8]) -> Vec<u8> {
        let mut out = (body.len() as u16).to_le_bytes().to_vec();
        out.push(msg_type);
        out.extend_from_slice(body);
        out
    }

    fn format_message(definition: &str) -> Vec<u8> {
        message(b'F', definition.as_bytes())
    }

    fn subscription_message(multi_id: u8, msg_id: u16, name: &str) -> Vec<u8> {
        let mut body = vec![multi_id];
        body.extend_from_slice(&msg_id.to_le_bytes());
        body.extend_from_slice(name.as_bytes());
        message(b'A', &body)
    }

    fn data_message(msg_id: u16, payload: &[u8]) -> Vec<u8> {
        let mut body = msg_id.to_le_bytes().to_vec();
        body.extend_from_slice(payload);
        message(b'D', &body)
    }

    fn logged_string_message(log_level: u8, timestamp_us: u64, text: &str) -> Vec<u8> {
        let mut body = vec![log_level];
        body.extend_from_slice(&timestamp_us.to_le_bytes());
        body.extend_from_slice(text.as_bytes());
        message(b'L', &body)
    }

    fn tagged_logged_string_message(
        log_level: u8,
        tag: u16,
        timestamp_us: u64,
        text: &str,
    ) -> Vec<u8> {
        let mut body = vec![log_level];
        body.extend_from_slice(&tag.to_le_bytes());
        body.extend_from_slice(&timestamp_us.to_le_bytes());
        body.extend_from_slice(text.as_bytes());
        message(b'C', &body)
    }

    fn parameter_message(key: &str, value: &[u8]) -> Vec<u8> {
        let mut body = vec![key.len() as u8];
        body.extend_from_slice(key.as_bytes());
        body.extend_from_slice(value);
        message(b'P', &body)
    }

    fn convert(ulog: Vec<u8>) -> Vec<u8> {
        let mut input = Cursor::new(ulog);
        let start = read_and_validate_header(&mut input).expect("valid header");
        let mut output = Cursor::new(Vec::new());
        convert_validated_ulog(&mut output, &mut input, start, write_options()).expect("convert");
        output.seek(SeekFrom::Start(0)).expect("seek");
        let mut bytes = Vec::new();
        output.read_to_end(&mut bytes).expect("read output");
        bytes
    }

    struct DecodedMessage {
        topic: String,
        schema_name: String,
        log_time: u64,
        message: DynamicMessage,
    }

    fn decode_messages(bytes: &[u8]) -> Vec<DecodedMessage> {
        let mut pools: BTreeMap<u16, DescriptorPool> = BTreeMap::new();
        mcap::MessageStream::new(bytes)
            .expect("message stream")
            .map(|message| {
                let message = message.expect("message");
                let schema = message.channel.schema.as_ref().expect("schema");
                let pool = pools
                    .entry(schema.id)
                    .or_insert_with(|| {
                        DescriptorPool::decode(schema.data.as_ref()).expect("descriptor pool")
                    })
                    .clone();
                let descriptor = pool
                    .get_message_by_name(&schema.name)
                    .expect("descriptor for schema");
                let decoded =
                    DynamicMessage::decode(descriptor, message.data.as_ref()).expect("decode");
                DecodedMessage {
                    topic: message.channel.topic.clone(),
                    schema_name: schema.name.clone(),
                    log_time: message.log_time,
                    message: decoded,
                }
            })
            .collect()
    }

    fn fixture_path(relative_from_repo_root: &str) -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .join(relative_from_repo_root)
    }

    #[test]
    fn rejects_invalid_magic() {
        let mut input = Cursor::new(b"not a ulog file at all".to_vec());
        let err = read_and_validate_header(&mut input).expect_err("invalid magic should fail");
        assert!(err.to_string().contains("invalid ULog magic"));
    }

    #[test]
    fn converts_data_topic_with_nested_array_char_and_padding() -> Result<()> {
        let mut ulog = ulog_header(0);
        ulog.extend(format_message("inner:uint8_t a;int16_t b;"));
        ulog.extend(format_message(
            "outer:uint64_t timestamp;float value;float[3] vec;char[4] label;inner nested;uint8_t flag;uint8_t[2] _padding0;",
        ));
        ulog.extend(subscription_message(0, 0, "outer"));

        let mut payload = Vec::new();
        payload.extend_from_slice(&1_000u64.to_le_bytes());
        payload.extend_from_slice(&1.5f32.to_le_bytes());
        for v in [1.0f32, 2.0, 3.0] {
            payload.extend_from_slice(&v.to_le_bytes());
        }
        payload.extend_from_slice(b"hi\0\0");
        payload.push(7); // inner.a
        payload.extend_from_slice(&(-3i16).to_le_bytes()); // inner.b
        payload.push(1); // flag
        payload.extend_from_slice(&[0, 0]); // _padding0
        ulog.extend(data_message(0, &payload));

        let bytes = convert(ulog);
        let summary = mcap::Summary::read(&bytes)?.expect("summary");
        assert!(summary
            .schemas
            .values()
            .any(|schema| schema.name == "px4.outer" && schema.encoding == "protobuf"));
        // The nested `inner` type is embedded in `px4.outer`'s descriptor set
        // rather than written as its own MCAP schema record; the nested-field
        // decode below verifies it resolves.

        let messages = decode_messages(&bytes);
        assert_eq!(messages.len(), 1);
        let decoded = &messages[0];
        assert_eq!(decoded.topic, "outer/0");
        assert_eq!(decoded.schema_name, "px4.outer");
        assert_eq!(decoded.log_time, 1_000_000);

        let m = &decoded.message;
        assert_eq!(
            m.get_field_by_name("timestamp").unwrap().as_u64(),
            Some(1000)
        );
        assert_eq!(m.get_field_by_name("value").unwrap().as_f32(), Some(1.5));
        let vec = m.get_field_by_name("vec").unwrap();
        let vec = vec.as_list().expect("repeated field");
        let vec: Vec<f32> = vec.iter().map(|v| v.as_f32().unwrap()).collect();
        assert_eq!(vec, vec![1.0, 2.0, 3.0]);
        assert_eq!(m.get_field_by_name("label").unwrap().as_str(), Some("hi"));
        assert_eq!(m.get_field_by_name("flag").unwrap().as_u32(), Some(1));

        let nested = m.get_field_by_name("nested").unwrap();
        let nested = nested.as_message().expect("nested message");
        assert_eq!(nested.get_field_by_name("a").unwrap().as_u32(), Some(7));
        assert_eq!(nested.get_field_by_name("b").unwrap().as_i32(), Some(-3));

        // Padding fields are not present in the schema.
        assert!(m.get_field_by_name("_padding0").is_none());
        Ok(())
    }

    #[test]
    fn writes_logged_strings_to_log_channel_with_normalized_severity() {
        let mut ulog = ulog_header(0);
        // ULog stores log_level as the ASCII digit, so '4' -> WARNING(4), '3' -> ERR(3).
        ulog.extend(logged_string_message(b'4', 2_000, "warning text"));
        ulog.extend(tagged_logged_string_message(
            b'3',
            42,
            3_000,
            "tagged error",
        ));

        let bytes = convert(ulog);
        let messages = decode_messages(&bytes);
        assert_eq!(messages.len(), 2);

        assert!(messages.iter().all(|m| m.topic == "log_message"));
        assert!(messages.iter().all(|m| m.schema_name == "px4.log_message"));

        let first = &messages[0];
        assert_eq!(first.log_time, 2_000_000);
        assert_eq!(
            first
                .message
                .get_field_by_name("severity")
                .unwrap()
                .as_u32(),
            Some(4)
        );
        assert_eq!(
            first.message.get_field_by_name("text").unwrap().as_str(),
            Some("warning text")
        );
        assert_eq!(
            first.message.get_field_by_name("tag").unwrap().as_u32(),
            Some(0)
        );

        let second = &messages[1];
        assert_eq!(second.log_time, 3_000_000);
        assert_eq!(
            second
                .message
                .get_field_by_name("severity")
                .unwrap()
                .as_u32(),
            Some(3)
        );
        assert_eq!(
            second.message.get_field_by_name("tag").unwrap().as_u32(),
            Some(42)
        );
        assert_eq!(
            second.message.get_field_by_name("text").unwrap().as_str(),
            Some("tagged error")
        );
    }

    #[test]
    fn separates_multi_instance_topics_and_records_multi_id_metadata() -> Result<()> {
        let mut ulog = ulog_header(0);
        ulog.extend(format_message("simple:uint64_t timestamp;float x;"));
        ulog.extend(subscription_message(0, 0, "simple"));
        ulog.extend(subscription_message(1, 1, "simple"));

        let mut payload0 = 10u64.to_le_bytes().to_vec();
        payload0.extend_from_slice(&4.0f32.to_le_bytes());
        ulog.extend(data_message(0, &payload0));

        let mut payload1 = 20u64.to_le_bytes().to_vec();
        payload1.extend_from_slice(&8.0f32.to_le_bytes());
        ulog.extend(data_message(1, &payload1));

        let bytes = convert(ulog);
        let summary = mcap::Summary::read(&bytes)?.expect("summary");
        // Both subscriptions share one schema.
        assert_eq!(
            summary
                .schemas
                .values()
                .filter(|s| s.name == "px4.simple")
                .count(),
            1
        );
        let topics: BTreeMap<&str, &BTreeMap<String, String>> = summary
            .channels
            .values()
            .map(|c| (c.topic.as_str(), &c.metadata))
            .collect();
        assert_eq!(
            topics.get("simple/0").unwrap().get("multi_id").unwrap(),
            "0"
        );
        assert_eq!(
            topics.get("simple/1").unwrap().get("multi_id").unwrap(),
            "1"
        );
        Ok(())
    }

    #[test]
    fn writes_initial_parameters_snapshot() {
        let mut ulog = ulog_header(5_000);
        ulog.extend(parameter_message("int32_t SYS_TEST", &42i32.to_le_bytes()));
        ulog.extend(parameter_message("float GAIN", &1.5f32.to_le_bytes()));

        let bytes = convert(ulog);
        let messages = decode_messages(&bytes);
        assert_eq!(messages.len(), 2);
        assert!(messages.iter().all(|m| m.topic == "parameters"));
        assert!(messages.iter().all(|m| m.schema_name == "px4.parameter"));
        // Initial parameters are stamped at the log start timestamp.
        assert!(messages.iter().all(|m| m.log_time == 5_000_000));

        let params: BTreeMap<String, f64> = messages
            .iter()
            .map(|m| {
                (
                    m.message
                        .get_field_by_name("name")
                        .unwrap()
                        .as_str()
                        .unwrap()
                        .to_string(),
                    m.message
                        .get_field_by_name("value")
                        .unwrap()
                        .as_f64()
                        .unwrap(),
                )
            })
            .collect();
        assert_eq!(params.get("SYS_TEST"), Some(&42.0));
        assert_eq!(params.get("GAIN"), Some(&1.5));
    }

    #[test]
    fn changed_parameter_uses_last_data_timestamp() {
        let mut ulog = ulog_header(0);
        ulog.extend(format_message("simple:uint64_t timestamp;float x;"));
        ulog.extend(subscription_message(0, 0, "simple"));
        let mut payload = 7_000u64.to_le_bytes().to_vec();
        payload.extend_from_slice(&1.0f32.to_le_bytes());
        ulog.extend(data_message(0, &payload));
        // A parameter change in the data section carries no timestamp; it should
        // be stamped with the most recent data timestamp.
        ulog.extend(parameter_message("float GAIN", &2.0f32.to_le_bytes()));

        let bytes = convert(ulog);
        let messages = decode_messages(&bytes);
        let param = messages
            .iter()
            .find(|m| m.topic == "parameters")
            .expect("parameter message");
        assert_eq!(param.log_time, 7_000_000);
        assert_eq!(
            param.message.get_field_by_name("value").unwrap().as_f64(),
            Some(2.0)
        );
    }

    #[test]
    fn converts_real_px4_ulog_fixture() -> Result<()> {
        let fixture = fixture_path("python/examples/ulog2mcap/fixtures/test_ulog.ulg");
        let ulog = std::fs::read(&fixture).expect("read fixture");
        let bytes = convert(ulog);

        let mut records = mcap::read::LinearReader::new(&bytes)?;
        match records.next() {
            Some(Ok(mcap::records::Record::Header(header))) => assert_eq!(header.profile, "px4"),
            other => panic!("expected MCAP header as first record, got {other:?}"),
        }

        let summary = mcap::Summary::read(&bytes)?.expect("summary");
        assert_eq!(summary.channels.len(), 116);
        assert!(summary
            .channels
            .values()
            .all(|c| c.message_encoding == "protobuf"));

        let topics: Vec<&str> = summary
            .channels
            .values()
            .map(|c| c.topic.as_str())
            .collect();
        for expected in [
            "log_message",
            "parameters",
            "sensor_accel/0",
            "sensor_accel/1",
            "vehicle_attitude/0",
        ] {
            assert!(topics.contains(&expected), "missing topic {expected}");
        }
        assert!(summary
            .schemas
            .values()
            .any(|s| s.name == "px4.log_message"));

        // Every logged-string severity should be a normalized kernel level 0-7.
        let log_messages = decode_messages(&bytes);
        let severities: Vec<u32> = log_messages
            .iter()
            .filter(|m| m.topic == "log_message")
            .map(|m| {
                m.message
                    .get_field_by_name("severity")
                    .unwrap()
                    .as_u32()
                    .unwrap()
            })
            .collect();
        assert!(!severities.is_empty());
        assert!(severities.iter().all(|&s| s <= 7));
        Ok(())
    }
}
