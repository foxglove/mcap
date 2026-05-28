use std::borrow::Cow;
use std::collections::{BTreeSet, HashMap};
use std::io::{self, IsTerminal as _, Write as _};
use std::sync::Arc;

use anyhow::{bail, Context as _, Result};
use mcap::sans_io::indexed_reader::ReadOrder;
use prost_reflect::{DescriptorPool, DynamicMessage, MessageDescriptor};

use crate::cli::CatCommand;
use crate::commands::common;
use crate::context::CommandContext;

const MESSAGE_PREVIEW_LEN: usize = 10;

pub fn run(_ctx: &CommandContext, args: CatCommand) -> Result<()> {
    let opts = CatOptions::from_args(&args)?;
    let stdout = std::io::stdout();
    let mut writer = std::io::BufWriter::new(stdout.lock());

    if args.files.is_empty() {
        let stdin = std::io::stdin();
        if stdin.is_terminal() {
            bail!("supply a file");
        }
        if cat_streaming(&mut writer, stdin.lock(), &opts)? {
            return Ok(());
        }
    } else {
        for file in args.files {
            if let Some(mut remote) = common::try_open_remote_mcap(_ctx, &file)? {
                let mut json_transcoders = JsonTranscoders::default();
                if let Some(broken_pipe) =
                    cat_remote_indexed(&mut writer, &mut remote, &opts, &mut json_transcoders)?
                {
                    if broken_pipe {
                        return Ok(());
                    }
                    continue;
                }
            }
            let mcap = common::load_path(_ctx, &file, "mcap cat")?;
            if cat_mcap(&mut writer, &mcap, &opts)? {
                return Ok(());
            }
        }
    }

    flush_or_ignore_broken_pipe(&mut writer)
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct CatOptions {
    topics: Vec<String>,
    start: u64,
    end: Option<u64>,
    json: bool,
}

impl CatOptions {
    fn from_args(args: &CatCommand) -> Result<Self> {
        let topics = args
            .topics
            .split(',')
            .filter(|topic| !topic.is_empty())
            .map(str::to_string)
            .collect();
        let mut start = args.start_nsecs;
        if args.start_secs > 0 {
            start = args
                .start_secs
                .checked_mul(1_000_000_000)
                .context("start seconds timestamp overflows nanoseconds")?;
        }
        let mut end = args.end_nsecs;
        if args.end_secs > 0 {
            end = args
                .end_secs
                .checked_mul(1_000_000_000)
                .context("end seconds timestamp overflows nanoseconds")?;
        }
        Ok(Self {
            topics,
            start,
            end: (end != 0).then_some(end),
            json: args.json,
        })
    }

    fn include_topic(&self, topic: &str) -> bool {
        self.topics.is_empty() || self.topics.iter().any(|included| included == topic)
    }

    fn include_time(&self, log_time: u64) -> bool {
        log_time >= self.start && self.end.is_none_or(|end| log_time < end)
    }
}

fn cat_mcap(writer: &mut impl std::io::Write, mcap: &[u8], opts: &CatOptions) -> Result<bool> {
    let mut json_transcoders = JsonTranscoders::default();
    if let Some(broken_pipe) = cat_indexed(writer, mcap, opts, &mut json_transcoders)? {
        return Ok(broken_pipe);
    }
    cat_linear(writer, mcap, opts, &mut json_transcoders)
}

fn cat_indexed(
    writer: &mut impl std::io::Write,
    mcap: &[u8],
    opts: &CatOptions,
    json_transcoders: &mut JsonTranscoders,
) -> Result<Option<bool>> {
    let Some(summary) = mcap::Summary::read(mcap)? else {
        return Ok(None);
    };
    if summary.chunk_indexes.is_empty() {
        return Ok(None);
    }

    let included_topics: BTreeSet<String> = summary
        .channels
        .values()
        .filter(|channel| opts.include_topic(&channel.topic))
        .map(|channel| channel.topic.clone())
        .collect();
    if !opts.topics.is_empty() && included_topics.is_empty() {
        return Ok(Some(false));
    }

    let mut indexed_opts =
        mcap::sans_io::IndexedReaderOptions::new().with_order(ReadOrder::LogTime);
    if opts.start != 0 {
        indexed_opts = indexed_opts.log_time_on_or_after(opts.start);
    }
    if let Some(end) = opts.end {
        indexed_opts = indexed_opts.log_time_before(end);
    }
    if !opts.topics.is_empty() {
        indexed_opts = indexed_opts.include_topics(included_topics.iter().cloned());
    }

    let mut reader = mcap::sans_io::IndexedReader::new_with_options(&summary, indexed_opts)?;

    while let Some(event) = reader.next_event() {
        match event? {
            mcap::sans_io::IndexedReadEvent::ReadChunkRequest { offset, length } => {
                let start = offset as usize;
                let end = start
                    .checked_add(length)
                    .ok_or_else(|| anyhow::anyhow!("chunk read overflow at offset {offset}"))?;
                if end > mcap.len() {
                    anyhow::bail!("chunk read out of bounds at offset {offset} length {length}");
                }
                reader.insert_chunk_record_data(offset, &mcap[start..end])?;
            }
            mcap::sans_io::IndexedReadEvent::Message { header, data } => {
                let channel = summary
                    .channels
                    .get(&header.channel_id)
                    .ok_or_else(|| anyhow::anyhow!("unknown channel {}", header.channel_id))?;
                let message = CatMessage {
                    channel,
                    sequence: header.sequence,
                    log_time: header.log_time,
                    publish_time: header.publish_time,
                    data,
                };
                if write_message(writer, message, opts, json_transcoders)? {
                    return Ok(Some(true));
                }
            }
        }
    }

    Ok(Some(false))
}

fn cat_remote_indexed(
    writer: &mut impl std::io::Write,
    remote: &mut common::RemoteMcap,
    opts: &CatOptions,
    json_transcoders: &mut JsonTranscoders,
) -> Result<Option<bool>> {
    let summary = remote.summary();
    if summary.chunk_indexes.is_empty() {
        return Ok(None);
    }

    let included_topics: BTreeSet<String> = summary
        .channels
        .values()
        .filter(|channel| opts.include_topic(&channel.topic))
        .map(|channel| channel.topic.clone())
        .collect();
    if !opts.topics.is_empty() && included_topics.is_empty() {
        return Ok(Some(false));
    }

    let mut indexed_opts =
        mcap::sans_io::IndexedReaderOptions::new().with_order(ReadOrder::LogTime);
    if opts.start != 0 {
        indexed_opts = indexed_opts.log_time_on_or_after(opts.start);
    }
    if let Some(end) = opts.end {
        indexed_opts = indexed_opts.log_time_before(end);
    }
    if !opts.topics.is_empty() {
        indexed_opts = indexed_opts.include_topics(included_topics.iter().cloned());
    }

    let mut reader = mcap::sans_io::IndexedReader::new_with_options(summary, indexed_opts)?;
    while let Some(event) = reader.next_event() {
        match event? {
            mcap::sans_io::IndexedReadEvent::ReadChunkRequest { offset, length } => {
                let chunk = remote.read_range(offset, length)?;
                reader.insert_chunk_record_data(offset, &chunk)?;
            }
            mcap::sans_io::IndexedReadEvent::Message { header, data } => {
                let channel = summary
                    .channels
                    .get(&header.channel_id)
                    .ok_or_else(|| anyhow::anyhow!("unknown channel {}", header.channel_id))?;
                let message = CatMessage {
                    channel,
                    sequence: header.sequence,
                    log_time: header.log_time,
                    publish_time: header.publish_time,
                    data,
                };
                if write_message(writer, message, opts, json_transcoders)? {
                    return Ok(Some(true));
                }
            }
        }
    }

    Ok(Some(false))
}

fn cat_linear(
    writer: &mut impl std::io::Write,
    mcap: &[u8],
    opts: &CatOptions,
    json_transcoders: &mut JsonTranscoders,
) -> Result<bool> {
    for message in mcap::MessageStream::new(mcap)? {
        let message = message?;
        if !opts.include_time(message.log_time) || !opts.include_topic(&message.channel.topic) {
            continue;
        }
        let message = CatMessage {
            channel: &message.channel,
            sequence: message.sequence,
            log_time: message.log_time,
            publish_time: message.publish_time,
            data: message.data.as_ref(),
        };
        if write_message(writer, message, opts, json_transcoders)? {
            return Ok(true);
        }
    }
    Ok(false)
}

fn cat_streaming(
    writer: &mut impl std::io::Write,
    mut source: impl std::io::Read,
    opts: &CatOptions,
) -> Result<bool> {
    let mut reader = mcap::sans_io::LinearReader::new();
    let mut schemas = HashMap::<u16, Arc<mcap::Schema<'static>>>::new();
    let mut channel_defs = HashMap::<u16, mcap::records::Channel>::new();
    let mut channels = HashMap::<u16, Arc<mcap::Channel<'static>>>::new();
    let mut json_transcoders = JsonTranscoders::default();

    while let Some(event) = reader.next_event() {
        match event? {
            mcap::sans_io::LinearReadEvent::ReadRequest(need) => {
                let read = source
                    .read(reader.insert(need))
                    .context("failed to read input from stdin")?;
                reader.notify_read(read);
            }
            mcap::sans_io::LinearReadEvent::Record { data, opcode } => {
                let record = mcap::parse_record(opcode, data)?;
                if handle_linear_record(
                    writer,
                    record,
                    opts,
                    &mut schemas,
                    &mut channel_defs,
                    &mut channels,
                    &mut json_transcoders,
                )? {
                    return Ok(true);
                }
            }
        }
    }

    Ok(false)
}

fn handle_linear_record(
    writer: &mut impl std::io::Write,
    record: mcap::records::Record<'_>,
    opts: &CatOptions,
    schemas: &mut HashMap<u16, Arc<mcap::Schema<'static>>>,
    channel_defs: &mut HashMap<u16, mcap::records::Channel>,
    channels: &mut HashMap<u16, Arc<mcap::Channel<'static>>>,
    json_transcoders: &mut JsonTranscoders,
) -> Result<bool> {
    match record {
        mcap::records::Record::Schema { header, data } => {
            let schema = Arc::new(mcap::Schema {
                id: header.id,
                name: header.name,
                encoding: header.encoding,
                data: Cow::Owned(data.into_owned()),
            });
            schemas.insert(schema.id, schema);
        }
        mcap::records::Record::Channel(channel) => {
            if channel.schema_id == 0 || schemas.contains_key(&channel.schema_id) {
                let resolved = build_channel(&channel, schemas)?;
                channels.insert(channel.id, resolved);
            }
            channel_defs.insert(channel.id, channel);
        }
        mcap::records::Record::Message { header, data } => {
            if !opts.include_time(header.log_time) {
                return Ok(false);
            }

            let channel = if let Some(channel) = channels.get(&header.channel_id) {
                channel.clone()
            } else {
                let Some(channel_def) = channel_defs.get(&header.channel_id) else {
                    bail!("message references unknown channel {}", header.channel_id);
                };
                let resolved = build_channel(channel_def, schemas)?;
                channels.insert(header.channel_id, resolved.clone());
                resolved
            };

            if !opts.include_topic(&channel.topic) {
                return Ok(false);
            }

            let message = CatMessage {
                channel: &channel,
                sequence: header.sequence,
                log_time: header.log_time,
                publish_time: header.publish_time,
                data: data.as_ref(),
            };
            return write_message(writer, message, opts, json_transcoders);
        }
        _ => {}
    }

    Ok(false)
}

fn build_channel(
    channel: &mcap::records::Channel,
    schemas: &HashMap<u16, Arc<mcap::Schema<'static>>>,
) -> Result<Arc<mcap::Channel<'static>>> {
    let schema = if channel.schema_id == 0 {
        None
    } else {
        Some(schemas.get(&channel.schema_id).cloned().ok_or_else(|| {
            anyhow::anyhow!(
                "encountered channel with topic {} with unknown schema ID {}",
                channel.topic,
                channel.schema_id
            )
        })?)
    };

    Ok(Arc::new(mcap::Channel {
        id: channel.id,
        topic: channel.topic.clone(),
        schema,
        message_encoding: channel.message_encoding.clone(),
        metadata: channel.metadata.clone(),
    }))
}

struct CatMessage<'a, 'schema, 'data> {
    channel: &'a mcap::Channel<'schema>,
    sequence: u32,
    log_time: u64,
    publish_time: u64,
    data: &'data [u8],
}

fn write_message(
    writer: &mut impl std::io::Write,
    message: CatMessage<'_, '_, '_>,
    opts: &CatOptions,
    json_transcoders: &mut JsonTranscoders,
) -> Result<bool> {
    if opts.json {
        write_json_message(
            writer,
            message.channel,
            message.sequence,
            message.log_time,
            message.publish_time,
            message.data,
            json_transcoders,
        )
    } else {
        let schema_name = message
            .channel
            .schema
            .as_ref()
            .map(|schema| schema.name.as_str())
            .unwrap_or("no schema");
        write_message_fields(
            writer,
            message.log_time,
            &message.channel.topic,
            schema_name,
            message.data,
            MESSAGE_PREVIEW_LEN,
        )
    }
}

fn write_message_fields(
    writer: &mut impl std::io::Write,
    log_time: u64,
    topic: &str,
    schema_name: &str,
    data: &[u8],
    max_preview_bytes: usize,
) -> Result<bool> {
    let result: io::Result<()> = (|| {
        common::write_raw_time(writer, log_time)?;
        write!(writer, " {} [{}] ", topic, schema_name)?;
        write_payload_preview(writer, data, max_preview_bytes)?;
        writeln!(writer)
    })();
    io_result_to_broken_pipe(result)
}

fn write_json_message(
    writer: &mut impl std::io::Write,
    channel: &mcap::Channel<'_>,
    sequence: u32,
    log_time: u64,
    publish_time: u64,
    data: &[u8],
    json_transcoders: &mut JsonTranscoders,
) -> Result<bool> {
    let encoded_data = json_transcoders.encode(channel, data)?;
    // Unlike the Go CLI's current manual string concatenation, escaping here keeps
    // JSON valid for topics containing quotes or backslashes.
    let topic = serde_json::to_string(&channel.topic).context("failed to encode topic")?;
    let result: io::Result<()> = (|| {
        write!(
            writer,
            "{{\"topic\":{topic},\"sequence\":{sequence},\"log_time\":"
        )?;
        writer.write_all(common::decimal_time(log_time).as_bytes())?;
        write!(writer, ",\"publish_time\":")?;
        writer.write_all(common::decimal_time(publish_time).as_bytes())?;
        writer.write_all(b",\"data\":")?;
        writer.write_all(encoded_data.as_ref())?;
        writer.write_all(b"}\n")
    })();
    io_result_to_broken_pipe(result)
}

fn io_result_to_broken_pipe(result: io::Result<()>) -> Result<bool> {
    match result {
        Ok(()) => Ok(false),
        Err(err) if err.kind() == io::ErrorKind::BrokenPipe => Ok(true),
        Err(err) => Err(err.into()),
    }
}

fn flush_or_ignore_broken_pipe(writer: &mut impl std::io::Write) -> Result<()> {
    if let Err(err) = writer.flush() {
        if err.kind() == io::ErrorKind::BrokenPipe {
            return Ok(());
        }
        return Err(err.into());
    }
    Ok(())
}

#[derive(Default)]
struct JsonTranscoders {
    protobuf_descriptors: HashMap<u16, MessageDescriptor>,
    ros1_transcoders: HashMap<u16, Ros1MessageDef>,
}

impl JsonTranscoders {
    fn encode<'a>(&mut self, channel: &mcap::Channel<'_>, data: &'a [u8]) -> Result<Cow<'a, [u8]>> {
        let Some(schema) = channel.schema.as_ref() else {
            return encode_schemaless_json(&channel.message_encoding, data);
        };
        if schema.encoding.is_empty() {
            return encode_schemaless_json(&channel.message_encoding, data);
        }

        match schema.encoding.as_str() {
            "jsonschema" => Ok(Cow::Borrowed(data)),
            "protobuf" => {
                let descriptor = match self.protobuf_descriptors.get(&schema.id) {
                    Some(descriptor) => descriptor.clone(),
                    None => {
                        let pool = DescriptorPool::decode(schema.data.as_ref())
                            .context("failed to build file descriptor set")?;
                        let descriptor = pool.get_message_by_name(&schema.name).ok_or_else(|| {
                            anyhow::anyhow!("failed to find descriptor: {}", schema.name)
                        })?;
                        self.protobuf_descriptors
                            .insert(schema.id, descriptor.clone());
                        descriptor
                    }
                };
                let message = DynamicMessage::decode(descriptor, data)
                    .context("failed to parse message")?;
                let json = serde_json::to_vec(&message).context("failed to marshal message")?;
                Ok(Cow::Owned(json))
            }
            "ros1msg" => {
                let transcoder = match self.ros1_transcoders.get(&schema.id) {
                    Some(transcoder) => transcoder,
                    None => {
                        let transcoder = Ros1MessageDef::parse(&schema.name, schema.data.as_ref())
                            .with_context(|| {
                                format!("failed to build transcoder for {}", channel.topic)
                            })?;
                        self.ros1_transcoders.insert(schema.id, transcoder);
                        self.ros1_transcoders
                            .get(&schema.id)
                            .expect("transcoder was just inserted")
                    }
                };
                let json = transcoder
                    .transcode(data)
                    .with_context(|| format!("failed to transcode {} record on {}", schema.name, channel.topic))?;
                Ok(Cow::Owned(json))
            }
            encoding => bail!(
                "JSON output only supported for ros1msg, protobuf, and jsonschema schemas. Found: {encoding}"
            ),
        }
    }
}

fn encode_schemaless_json<'a>(message_encoding: &str, data: &'a [u8]) -> Result<Cow<'a, [u8]>> {
    match message_encoding {
        "json" => Ok(Cow::Borrowed(data)),
        encoding => bail!(
            "for schema-less channels, JSON output is only supported with 'json' message encoding. found: {encoding}"
        ),
    }
}

#[derive(Debug, Clone)]
struct Ros1MessageDef {
    root_type: String,
    definitions: HashMap<String, Ros1Definition>,
}

#[derive(Debug, Clone)]
struct Ros1Definition {
    package: String,
    fields: Vec<Ros1Field>,
}

#[derive(Debug, Clone)]
struct Ros1Field {
    field_type: Ros1FieldType,
    name: String,
}

#[derive(Debug, Clone)]
struct Ros1FieldType {
    base: String,
    array: Option<Option<usize>>,
}

impl Ros1MessageDef {
    fn parse(root_type: &str, data: &[u8]) -> Result<Self> {
        let schema = std::str::from_utf8(data).context("schema data is not utf8")?;
        let mut definitions = HashMap::<String, Ros1Definition>::new();
        let mut current_type = root_type.to_string();
        definitions.insert(current_type.clone(), Ros1Definition::new(&current_type));

        for line in schema.lines() {
            let line = line.trim();
            if line.starts_with("MSG:") {
                current_type =
                    normalize_ros1_type(line.trim_start_matches("MSG:").trim(), root_type);
                definitions
                    .entry(current_type.clone())
                    .or_insert_with(|| Ros1Definition::new(&current_type));
                continue;
            }
            if line.starts_with('=') {
                continue;
            }
            let Some(field) = parse_ros1_field(line) else {
                continue;
            };
            definitions
                .entry(current_type.clone())
                .or_insert_with(|| Ros1Definition::new(&current_type))
                .fields
                .push(field);
        }

        Ok(Self {
            root_type: root_type.to_string(),
            definitions,
        })
    }

    fn transcode(&self, data: &[u8]) -> Result<Vec<u8>> {
        let mut cursor = 0usize;
        let mut out = Vec::new();
        self.write_message(&mut out, &self.root_type, data, &mut cursor)?;
        Ok(out)
    }

    fn write_message(
        &self,
        out: &mut Vec<u8>,
        type_name: &str,
        data: &[u8],
        cursor: &mut usize,
    ) -> Result<()> {
        let definition = self
            .definitions
            .get(type_name)
            .ok_or_else(|| anyhow::anyhow!("unknown ROS1 message type {type_name}"))?;
        out.push(b'{');
        for (index, field) in definition.fields.iter().enumerate() {
            if index > 0 {
                out.push(b',');
            }
            serde_json::to_writer(&mut *out, &field.name)?;
            out.push(b':');
            self.write_value(out, &definition.package, &field.field_type, data, cursor)?;
        }
        out.push(b'}');
        Ok(())
    }

    fn write_value(
        &self,
        out: &mut Vec<u8>,
        package: &str,
        field_type: &Ros1FieldType,
        data: &[u8],
        cursor: &mut usize,
    ) -> Result<()> {
        if let Some(array_len) = field_type.array {
            let len = match array_len {
                Some(len) => len,
                None => read_u32(data, cursor)? as usize,
            };
            out.push(b'[');
            for index in 0..len {
                if index > 0 {
                    out.push(b',');
                }
                self.write_single_value(out, package, &field_type.base, data, cursor)?;
            }
            out.push(b']');
            return Ok(());
        }

        self.write_single_value(out, package, &field_type.base, data, cursor)
    }

    fn write_single_value(
        &self,
        out: &mut Vec<u8>,
        package: &str,
        base_type: &str,
        data: &[u8],
        cursor: &mut usize,
    ) -> Result<()> {
        match base_type {
            "bool" => out.extend_from_slice(if read_u8(data, cursor)? == 0 {
                b"false"
            } else {
                b"true"
            }),
            "int8" | "byte" => write!(out, "{}", read_i8(data, cursor)?)?,
            "uint8" | "char" => write!(out, "{}", read_u8(data, cursor)?)?,
            "int16" => write!(out, "{}", read_i16(data, cursor)?)?,
            "uint16" => write!(out, "{}", read_u16(data, cursor)?)?,
            "int32" => write!(out, "{}", read_i32(data, cursor)?)?,
            "uint32" => write!(out, "{}", read_u32(data, cursor)?)?,
            "int64" => write!(out, "{}", read_i64(data, cursor)?)?,
            "uint64" => write!(out, "{}", read_u64(data, cursor)?)?,
            "float32" => write_ros1_float(out, read_f32(data, cursor)? as f64)?,
            "float64" => write_ros1_float(out, read_f64(data, cursor)?)?,
            "string" => {
                let len = read_u32(data, cursor)? as usize;
                let bytes = read_exact(data, cursor, len)?;
                let value = String::from_utf8_lossy(bytes);
                serde_json::to_writer(&mut *out, value.as_ref())?;
            }
            "time" => {
                let sec = read_u32(data, cursor)? as u64;
                let nsec = read_u32(data, cursor)? as u64;
                write!(out, "{sec}.{nsec:09}")?;
            }
            "duration" => {
                let sec = read_i32(data, cursor)?;
                let nsec = read_i32(data, cursor)?;
                write_signed_decimal_time(out, sec, nsec)?;
            }
            nested_type => {
                let resolved = resolve_ros1_type(package, nested_type);
                self.write_message(out, &resolved, data, cursor)?;
            }
        }
        Ok(())
    }
}

impl Ros1Definition {
    fn new(type_name: &str) -> Self {
        let package = type_name
            .split_once('/')
            .map(|(package, _)| package.to_string())
            .unwrap_or_default();
        Self {
            package,
            fields: Vec::new(),
        }
    }
}

fn normalize_ros1_type(type_name: &str, root_type: &str) -> String {
    if type_name.contains('/') {
        type_name.to_string()
    } else {
        resolve_ros1_type(
            root_type
                .split_once('/')
                .map(|(package, _)| package)
                .unwrap_or(""),
            type_name,
        )
    }
}

fn resolve_ros1_type(package: &str, type_name: &str) -> String {
    if type_name.contains('/') {
        type_name.to_string()
    } else if type_name == "Header" {
        "std_msgs/Header".to_string()
    } else {
        format!("{package}/{type_name}")
    }
}

fn parse_ros1_field(line: &str) -> Option<Ros1Field> {
    let line = line
        .split_once('#')
        .map(|(prefix, _)| prefix)
        .unwrap_or(line)
        .trim();
    if line.is_empty() || line.contains('=') {
        return None;
    }
    let mut parts = line.split_whitespace();
    let type_token = parts.next()?;
    let name = parts.next()?.to_string();
    Some(Ros1Field {
        field_type: parse_ros1_field_type(type_token),
        name,
    })
}

fn parse_ros1_field_type(type_token: &str) -> Ros1FieldType {
    if let Some(array_start) = type_token.find('[') {
        let base = strip_bound(&type_token[..array_start]).to_string();
        let Some(array_end) = type_token[array_start + 1..]
            .find(']')
            .map(|relative| array_start + 1 + relative)
        else {
            return Ros1FieldType { base, array: None };
        };
        let array_suffix = &type_token[array_start + 1..array_end];
        let array = if array_suffix.is_empty() || array_suffix.starts_with("<=") {
            Some(None)
        } else {
            Some(array_suffix.parse::<usize>().ok())
        };
        Ros1FieldType { base, array }
    } else {
        Ros1FieldType {
            base: strip_bound(type_token).to_string(),
            array: None,
        }
    }
}

fn strip_bound(type_token: &str) -> &str {
    type_token
        .split_once("<=")
        .map(|(base, _)| base)
        .unwrap_or(type_token)
}

fn write_ros1_float(writer: &mut impl std::io::Write, value: f64) -> std::io::Result<()> {
    if value.is_nan() {
        writer.write_all(br#""NaN""#)
    } else if value == f64::INFINITY {
        writer.write_all(br#""Infinity""#)
    } else if value == f64::NEG_INFINITY {
        writer.write_all(br#""-Infinity""#)
    } else {
        serde_json::to_writer(writer, &value)
            .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?;
        Ok(())
    }
}

fn write_signed_decimal_time(
    writer: &mut impl std::io::Write,
    seconds: i32,
    nanos: i32,
) -> std::io::Result<()> {
    let total_nanos = seconds as i128 * 1_000_000_000i128 + nanos as i128;
    let sign = if total_nanos < 0 { "-" } else { "" };
    let abs = total_nanos.abs();
    write!(
        writer,
        "{sign}{}.{:09}",
        abs / 1_000_000_000,
        abs % 1_000_000_000
    )
}

fn read_exact<'a>(data: &'a [u8], cursor: &mut usize, len: usize) -> Result<&'a [u8]> {
    let end = cursor
        .checked_add(len)
        .ok_or_else(|| anyhow::anyhow!("ROS1 cursor overflow"))?;
    let slice = data
        .get(*cursor..end)
        .ok_or_else(|| anyhow::anyhow!("ROS1 message ended unexpectedly"))?;
    *cursor = end;
    Ok(slice)
}

fn read_u8(data: &[u8], cursor: &mut usize) -> Result<u8> {
    Ok(read_exact(data, cursor, 1)?[0])
}

fn read_i8(data: &[u8], cursor: &mut usize) -> Result<i8> {
    Ok(read_u8(data, cursor)? as i8)
}

fn read_u16(data: &[u8], cursor: &mut usize) -> Result<u16> {
    Ok(u16::from_le_bytes(read_exact(data, cursor, 2)?.try_into()?))
}

fn read_i16(data: &[u8], cursor: &mut usize) -> Result<i16> {
    Ok(i16::from_le_bytes(read_exact(data, cursor, 2)?.try_into()?))
}

fn read_u32(data: &[u8], cursor: &mut usize) -> Result<u32> {
    Ok(u32::from_le_bytes(read_exact(data, cursor, 4)?.try_into()?))
}

fn read_i32(data: &[u8], cursor: &mut usize) -> Result<i32> {
    Ok(i32::from_le_bytes(read_exact(data, cursor, 4)?.try_into()?))
}

fn read_u64(data: &[u8], cursor: &mut usize) -> Result<u64> {
    Ok(u64::from_le_bytes(read_exact(data, cursor, 8)?.try_into()?))
}

fn read_i64(data: &[u8], cursor: &mut usize) -> Result<i64> {
    Ok(i64::from_le_bytes(read_exact(data, cursor, 8)?.try_into()?))
}

fn read_f32(data: &[u8], cursor: &mut usize) -> Result<f32> {
    Ok(f32::from_le_bytes(read_exact(data, cursor, 4)?.try_into()?))
}

fn read_f64(data: &[u8], cursor: &mut usize) -> Result<f64> {
    Ok(f64::from_le_bytes(read_exact(data, cursor, 8)?.try_into()?))
}

fn write_payload_preview(
    writer: &mut impl std::io::Write,
    data: &[u8],
    max_bytes: usize,
) -> std::io::Result<()> {
    let preview = if data.len() > max_bytes {
        &data[..max_bytes]
    } else {
        data
    };

    write!(writer, "[")?;
    for (idx, byte) in preview.iter().enumerate() {
        if idx > 0 {
            write!(writer, " ")?;
        }
        write!(writer, "{byte}")?;
    }
    write!(writer, "]")?;

    if data.len() > max_bytes {
        write!(writer, "...")?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{borrow::Cow, collections::BTreeMap, io::Cursor, sync::Arc};

    use super::{
        cat_mcap, cat_streaming, parse_ros1_field_type, write_payload_preview, write_ros1_float,
        write_signed_decimal_time, CatOptions, JsonTranscoders, Ros1MessageDef,
    };

    fn sample_message(schema_name: Option<&str>, data: Vec<u8>) -> mcap::Message<'static> {
        let schema = schema_name.map(|name| {
            Arc::new(mcap::Schema {
                id: 1,
                name: name.to_string(),
                encoding: "jsonschema".to_string(),
                data: Cow::Owned(Vec::new()),
            })
        });
        mcap::Message {
            channel: Arc::new(mcap::Channel {
                id: 1,
                topic: "/demo".to_string(),
                schema,
                message_encoding: "json".to_string(),
                metadata: BTreeMap::new(),
            }),
            sequence: 1,
            log_time: 42,
            publish_time: 43,
            data: Cow::Owned(data),
        }
    }

    fn payload_preview_string(data: &[u8], max_bytes: usize) -> String {
        let mut out = Vec::new();
        write_payload_preview(&mut out, data, max_bytes).expect("payload preview should serialize");
        String::from_utf8(out).expect("payload preview should be utf8")
    }

    fn message_line_string(message: &mcap::Message<'_>, max_preview_bytes: usize) -> String {
        let mut out = Vec::new();
        let schema_name = message
            .channel
            .schema
            .as_ref()
            .map(|schema| schema.name.as_str())
            .unwrap_or("no schema");
        let broken_pipe = super::write_message_fields(
            &mut out,
            message.log_time,
            &message.channel.topic,
            schema_name,
            message.data.as_ref(),
            max_preview_bytes,
        )
        .expect("message line should write");
        assert!(!broken_pipe);
        String::from_utf8(out)
            .expect("message line should be utf8")
            .trim_end_matches('\n')
            .to_string()
    }

    fn build_out_of_order_chunked_mcap() -> Vec<u8> {
        let mut cursor = Cursor::new(Vec::new());
        {
            let mut writer = mcap::WriteOptions::new()
                .chunk_size(Some(1024))
                .create(&mut cursor)
                .expect("writer");

            let schema_id = writer
                .add_schema("Example", "jsonschema", br#"{"type":"object"}"#)
                .expect("schema");
            let channel_id = writer
                .add_channel(schema_id, "/demo", "json", &BTreeMap::new())
                .expect("channel");

            writer
                .write_to_known_channel(
                    &mcap::records::MessageHeader {
                        channel_id,
                        sequence: 1,
                        log_time: 30,
                        publish_time: 30,
                    },
                    &[1],
                )
                .expect("write message 1");
            writer.flush().expect("flush chunk 1");

            writer
                .write_to_known_channel(
                    &mcap::records::MessageHeader {
                        channel_id,
                        sequence: 2,
                        log_time: 10,
                        publish_time: 10,
                    },
                    &[2],
                )
                .expect("write message 2");
            writer.flush().expect("flush chunk 2");

            writer.finish().expect("finish");
        }
        cursor.into_inner()
    }

    fn build_out_of_order_linear_mcap_without_summary() -> Vec<u8> {
        let mut cursor = Cursor::new(Vec::new());
        {
            let mut writer = mcap::WriteOptions::new()
                .chunk_size(None)
                .emit_summary_records(false)
                .emit_summary_offsets(false)
                .create(&mut cursor)
                .expect("writer");

            let schema_id = writer
                .add_schema("Example", "jsonschema", br#"{"type":"object"}"#)
                .expect("schema");
            let channel_id = writer
                .add_channel(schema_id, "/demo", "json", &BTreeMap::new())
                .expect("channel");

            writer
                .write_to_known_channel(
                    &mcap::records::MessageHeader {
                        channel_id,
                        sequence: 1,
                        log_time: 30,
                        publish_time: 30,
                    },
                    &[1],
                )
                .expect("write message 1");

            writer
                .write_to_known_channel(
                    &mcap::records::MessageHeader {
                        channel_id,
                        sequence: 2,
                        log_time: 10,
                        publish_time: 10,
                    },
                    &[2],
                )
                .expect("write message 2");

            writer.finish().expect("finish");
        }
        cursor.into_inner()
    }

    fn build_multi_topic_mcap() -> Vec<u8> {
        let mut cursor = Cursor::new(Vec::new());
        {
            let mut writer = mcap::WriteOptions::new()
                .chunk_size(Some(1024))
                .create(&mut cursor)
                .expect("writer");
            let schema_id = writer
                .add_schema("Example", "jsonschema", br#"{"type":"object"}"#)
                .expect("schema");
            let camera_id = writer
                .add_channel(schema_id, "/camera", "json", &BTreeMap::new())
                .expect("camera channel");
            let radar_id = writer
                .add_channel(schema_id, "/radar", "json", &BTreeMap::new())
                .expect("radar channel");
            for (sequence, channel_id, log_time, data) in [
                (1, camera_id, 10, br#"{"camera":1}"#.as_slice()),
                (2, radar_id, 20, br#"{"radar":1}"#.as_slice()),
                (3, camera_id, 30, br#"{"camera":2}"#.as_slice()),
            ] {
                writer
                    .write_to_known_channel(
                        &mcap::records::MessageHeader {
                            channel_id,
                            sequence,
                            log_time,
                            publish_time: log_time,
                        },
                        data,
                    )
                    .expect("write message");
            }
            writer.finish().expect("finish");
        }
        cursor.into_inner()
    }

    #[test]
    fn payload_preview_includes_full_message_when_short() {
        assert_eq!(payload_preview_string(&[1, 2, 3], 10), "[1 2 3]");
    }

    #[test]
    fn payload_preview_truncates_with_ellipsis() {
        let data: Vec<u8> = (0..12).collect();
        assert_eq!(
            payload_preview_string(&data, 10),
            "[0 1 2 3 4 5 6 7 8 9]..."
        );
    }

    #[test]
    fn message_line_includes_schema_name_when_present() {
        let message = sample_message(Some("Example"), vec![1, 2, 3]);
        assert_eq!(
            message_line_string(&message, 10),
            "42 /demo [Example] [1 2 3]"
        );
    }

    #[test]
    fn message_line_uses_no_schema_for_schemaless_channel() {
        let message = sample_message(None, vec![1, 2, 3]);
        assert_eq!(
            message_line_string(&message, 10),
            "42 /demo [no schema] [1 2 3]"
        );
    }

    #[test]
    fn cat_prefers_log_time_order_when_index_available() {
        let mcap = build_out_of_order_chunked_mcap();
        let mut out = Vec::new();
        let broken_pipe =
            cat_mcap(&mut out, &mcap, &CatOptions::default()).expect("cat should succeed");
        assert!(!broken_pipe);

        let output = String::from_utf8(out).expect("valid utf8 output");
        let lines: Vec<&str> = output.lines().collect();
        assert_eq!(
            lines,
            vec!["10 /demo [Example] [2]", "30 /demo [Example] [1]"]
        );
    }

    #[test]
    fn cat_falls_back_to_linear_order_without_index() {
        let mcap = build_out_of_order_linear_mcap_without_summary();
        let mut out = Vec::new();
        let broken_pipe =
            cat_mcap(&mut out, &mcap, &CatOptions::default()).expect("cat should succeed");
        assert!(!broken_pipe);

        let output = String::from_utf8(out).expect("valid utf8 output");
        let lines: Vec<&str> = output.lines().collect();
        assert_eq!(
            lines,
            vec!["30 /demo [Example] [1]", "10 /demo [Example] [2]"]
        );
    }

    #[test]
    fn cat_applies_topic_and_time_filters() {
        let mcap = build_multi_topic_mcap();
        let opts = CatOptions {
            topics: vec!["/camera".to_string()],
            start: 20,
            end: None,
            json: false,
        };
        let mut out = Vec::new();
        let broken_pipe = cat_mcap(&mut out, &mcap, &opts).expect("cat should succeed");
        assert!(!broken_pipe);

        let output = String::from_utf8(out).expect("valid utf8 output");
        let lines: Vec<&str> = output.lines().collect();
        assert_eq!(
            lines,
            vec![r#"30 /camera [Example] [123 34 99 97 109 101 114 97 34 58]..."#]
        );
    }

    #[test]
    fn cat_streaming_reads_without_buffering_full_input() {
        let mcap = build_multi_topic_mcap();
        let opts = CatOptions {
            topics: vec!["/radar".to_string()],
            ..CatOptions::default()
        };
        let mut out = Vec::new();
        let broken_pipe = cat_streaming(&mut out, Cursor::new(mcap), &opts)
            .expect("streaming cat should succeed");
        assert!(!broken_pipe);

        let output = String::from_utf8(out).expect("valid utf8 output");
        let lines: Vec<&str> = output.lines().collect();
        assert_eq!(
            lines,
            vec![r#"20 /radar [Example] [123 34 114 97 100 97 114 34 58 49]..."#]
        );
    }

    #[test]
    fn cat_json_wraps_jsonschema_messages() {
        let message = sample_message(Some("Example"), br#"{"value":1}"#.to_vec());
        let mut out = Vec::new();
        let mut transcoders = JsonTranscoders::default();
        let opts = CatOptions {
            json: true,
            ..CatOptions::default()
        };
        let cat_message = super::CatMessage {
            channel: &message.channel,
            sequence: message.sequence,
            log_time: message.log_time,
            publish_time: message.publish_time,
            data: message.data.as_ref(),
        };
        let broken_pipe = super::write_message(&mut out, cat_message, &opts, &mut transcoders)
            .expect("json message should write");
        assert!(!broken_pipe);

        assert_eq!(
            String::from_utf8(out).expect("valid utf8 output"),
            r#"{"topic":"/demo","sequence":1,"log_time":0.000000042,"publish_time":0.000000043,"data":{"value":1}}"#
                .to_string()
                + "\n"
        );
    }

    #[test]
    fn protobuf_json_uses_lower_camel_case_and_omits_zero_values() {
        let descriptor = vec![
            10, 122, 10, 12, 115, 97, 109, 112, 108, 101, 46, 112, 114, 111, 116, 111, 18, 4, 116,
            101, 115, 116, 34, 92, 10, 6, 83, 97, 109, 112, 108, 101, 18, 29, 10, 10, 115, 110, 97,
            107, 101, 95, 99, 97, 115, 101, 24, 1, 32, 1, 40, 9, 82, 9, 115, 110, 97, 107, 101, 67,
            97, 115, 101, 18, 29, 10, 10, 122, 101, 114, 111, 95, 118, 97, 108, 117, 101, 24, 2,
            32, 1, 40, 13, 82, 9, 122, 101, 114, 111, 86, 97, 108, 117, 101, 18, 20, 10, 5, 99,
            111, 117, 110, 116, 24, 3, 32, 1, 40, 13, 82, 5, 99, 111, 117, 110, 116, 98, 6, 112,
            114, 111, 116, 111, 51,
        ];
        let schema = Arc::new(mcap::Schema {
            id: 1,
            name: "test.Sample".to_string(),
            encoding: "protobuf".to_string(),
            data: Cow::Owned(descriptor),
        });
        let channel = Arc::new(mcap::Channel {
            id: 1,
            topic: "proto".to_string(),
            schema: Some(schema),
            message_encoding: "protobuf".to_string(),
            metadata: BTreeMap::new(),
        });
        let mut transcoders = JsonTranscoders::default();
        let encoded = transcoders
            .encode(&channel, &[10, 5, b'h', b'e', b'l', b'l', b'o', 24, 7])
            .expect("protobuf should encode");
        assert_eq!(
            String::from_utf8(encoded.into_owned()).expect("valid utf8"),
            r#"{"snakeCase":"hello","count":7}"#
        );
    }

    #[test]
    fn ros1_transcoder_handles_nested_messages_and_arrays() {
        let schema = b"Header header\nint32[] values\nstring label\n================================================================================\nMSG: std_msgs/Header\nuint32 seq\ntime stamp\nstring frame_id\n";
        let transcoder =
            Ros1MessageDef::parse("demo/Example", schema).expect("schema should parse");
        let mut data = Vec::new();
        data.extend_from_slice(&7u32.to_le_bytes());
        data.extend_from_slice(&1u32.to_le_bytes());
        data.extend_from_slice(&2u32.to_le_bytes());
        data.extend_from_slice(&3u32.to_le_bytes());
        data.extend_from_slice(b"map");
        data.extend_from_slice(&2u32.to_le_bytes());
        data.extend_from_slice(&10i32.to_le_bytes());
        data.extend_from_slice(&20i32.to_le_bytes());
        data.extend_from_slice(&5u32.to_le_bytes());
        data.extend_from_slice(b"hello");

        let json = transcoder
            .transcode(&data)
            .expect("message should transcode");
        assert_eq!(
            String::from_utf8(json).expect("valid utf8"),
            r#"{"header":{"seq":7,"stamp":1.000000002,"frame_id":"map"},"values":[10,20],"label":"hello"}"#
        );
    }

    #[test]
    fn ros1_duration_formats_signed_total_nanoseconds() {
        let mut out = Vec::new();
        write_signed_decimal_time(&mut out, 5, -100).expect("duration should format");
        assert_eq!(String::from_utf8(out).expect("valid utf8"), "4.999999900");

        let mut out = Vec::new();
        write_signed_decimal_time(&mut out, -5, 100).expect("duration should format");
        assert_eq!(String::from_utf8(out).expect("valid utf8"), "-4.999999900");
    }

    #[test]
    fn malformed_ros1_array_type_does_not_panic() {
        let field_type = parse_ros1_field_type("int32[");
        assert_eq!(field_type.base, "int32");
        assert!(field_type.array.is_none());
    }

    #[test]
    fn bounded_ros1_array_type_is_variable_length_array() {
        let field_type = parse_ros1_field_type("int32[<=10]");
        assert_eq!(field_type.base, "int32");
        assert_eq!(field_type.array, Some(None));
    }

    #[test]
    fn bounded_ros1_scalar_type_strips_bound() {
        let field_type = parse_ros1_field_type("string<=10");
        assert_eq!(field_type.base, "string");
        assert!(field_type.array.is_none());
    }

    #[test]
    fn ros1_float_special_values_match_protojson_strings() {
        let mut out = Vec::new();
        write_ros1_float(&mut out, f64::NAN).expect("nan should write");
        assert_eq!(String::from_utf8(out).expect("valid utf8"), r#""NaN""#);

        let mut out = Vec::new();
        write_ros1_float(&mut out, f64::INFINITY).expect("infinity should write");
        assert_eq!(String::from_utf8(out).expect("valid utf8"), r#""Infinity""#);

        let mut out = Vec::new();
        write_ros1_float(&mut out, f64::NEG_INFINITY).expect("negative infinity should write");
        assert_eq!(
            String::from_utf8(out).expect("valid utf8"),
            r#""-Infinity""#
        );
    }
}
