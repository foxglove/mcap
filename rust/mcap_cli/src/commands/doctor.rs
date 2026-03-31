use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result};

use crate::cli::DoctorCommand;
use crate::commands::common;
use crate::context::CommandContext;

pub fn run(ctx: &CommandContext, args: DoctorCommand) -> Result<()> {
    let mcap = common::map_file(&args.file)?;
    if ctx.verbose() > 0 {
        println!("Examining {}", args.file.display());
    }

    let diagnosis = diagnose_mcap(&mcap, args.strict_message_order);
    for warning in &diagnosis.warnings {
        eprintln!("Warning: {warning}");
    }
    for error in &diagnosis.errors {
        eprintln!("Error: {error}");
    }

    if !diagnosis.errors.is_empty() {
        anyhow::bail!("encountered {} errors", diagnosis.errors.len());
    }

    Ok(())
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
struct Diagnosis {
    errors: Vec<String>,
    warnings: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ParsedSchema {
    header: mcap::records::SchemaHeader,
    data: Vec<u8>,
}

#[derive(Debug, Clone)]
struct Doctor {
    strict_message_order: bool,
    diagnosis: Diagnosis,
    schemas_in_data_section: BTreeMap<u16, ParsedSchema>,
    channels_in_data_section: BTreeMap<u16, mcap::records::Channel>,
    channels_referenced_in_chunks_by_offset: BTreeMap<u64, BTreeSet<u16>>,
    channel_ids_in_summary_section: BTreeSet<u16>,
    schema_ids_in_summary_section: BTreeSet<u16>,
    chunk_indexes: BTreeMap<u64, mcap::records::ChunkIndex>,
    in_summary_section: bool,
    saw_data_end: bool,
    saw_footer: bool,
    saw_top_level_message: bool,
    message_index_warning_emitted: bool,
    last_top_level_message_time: Option<u64>,
    global_max_log_time: Option<u64>,
    min_log_time: Option<u64>,
    max_log_time: Option<u64>,
    message_count: u64,
    statistics: Option<mcap::records::Statistics>,
}

fn diagnose_mcap(mcap: &[u8], strict_message_order: bool) -> Diagnosis {
    let mut doctor = Doctor::new(strict_message_order);
    doctor.scan_top_level(mcap);
    doctor.finalize_record_presence();
    doctor.validate_chunk_indexes(mcap);
    doctor.validate_statistics();
    doctor.diagnosis
}

impl Doctor {
    fn new(strict_message_order: bool) -> Self {
        Self {
            strict_message_order,
            diagnosis: Diagnosis::default(),
            schemas_in_data_section: BTreeMap::new(),
            channels_in_data_section: BTreeMap::new(),
            channels_referenced_in_chunks_by_offset: BTreeMap::new(),
            channel_ids_in_summary_section: BTreeSet::new(),
            schema_ids_in_summary_section: BTreeSet::new(),
            chunk_indexes: BTreeMap::new(),
            in_summary_section: false,
            saw_data_end: false,
            saw_footer: false,
            saw_top_level_message: false,
            message_index_warning_emitted: false,
            last_top_level_message_time: None,
            global_max_log_time: None,
            min_log_time: None,
            max_log_time: None,
            message_count: 0,
            statistics: None,
        }
    }

    fn warn(&mut self, message: impl Into<String>) {
        self.diagnosis.warnings.push(message.into());
    }

    fn error(&mut self, message: impl Into<String>) {
        self.diagnosis.errors.push(message.into());
    }

    fn scan_top_level(&mut self, mcap: &[u8]) {
        let mut reader = mcap::sans_io::LinearReader::new_with_options(
            mcap::sans_io::LinearReaderOptions::default()
                .with_emit_chunks(true)
                .with_validate_chunk_crcs(true)
                .with_validate_data_section_crc(true)
                .with_validate_summary_section_crc(true)
                .with_record_length_limit(mcap.len()),
        );
        let mut remaining = mcap;
        let mut next_record_offset = mcap::MAGIC.len() as u64;

        while let Some(event) = reader.next_event() {
            match event {
                Err(err) => {
                    self.error(format!("Failed to read next token: {err:#}"));
                    break;
                }
                Ok(mcap::sans_io::LinearReadEvent::ReadRequest(need)) => {
                    let read = need.min(remaining.len());
                    let dst = reader.insert(read);
                    dst.copy_from_slice(&remaining[..read]);
                    reader.notify_read(read);
                    remaining = &remaining[read..];
                }
                Ok(mcap::sans_io::LinearReadEvent::Record { opcode, data }) => {
                    let record_offset = next_record_offset;
                    next_record_offset += 9 + data.len() as u64;
                    let record = match mcap::parse_record(opcode, data) {
                        Ok(record) => record,
                        Err(err) => {
                            self.error(format!(
                                "Failed to parse top-level record at offset {}: {err:#}",
                                record_offset
                            ));
                            continue;
                        }
                    };
                    self.handle_top_level_record(record, record_offset);
                }
            }
        }
    }

    fn handle_top_level_record(&mut self, record: mcap::records::Record<'_>, offset: u64) {
        match record {
            mcap::records::Record::Header(header) => {
                if header.library.is_empty() {
                    self.warn(
                        "Set the Header.library field to a value that identifies the software that produced the file.",
                    );
                }
                if !header.profile.is_empty()
                    && header.profile != "ros1"
                    && header.profile != "ros2"
                {
                    self.warn(format!(
                        "Header.profile field {:?} is not a well-known profile.",
                        header.profile
                    ));
                }
            }
            mcap::records::Record::Footer(_) => {
                self.saw_footer = true;
            }
            mcap::records::Record::Schema { header, data } => {
                self.examine_schema(header, data.as_ref());
            }
            mcap::records::Record::Channel(channel) => {
                self.examine_channel(channel);
            }
            mcap::records::Record::Message { header, .. } => {
                self.saw_top_level_message = true;
                self.examine_top_level_message(&header);
            }
            mcap::records::Record::Chunk { header, data } => {
                self.examine_chunk(header, data.as_ref(), offset);
            }
            mcap::records::Record::MessageIndex(_) => {
                if self.saw_top_level_message {
                    self.warn_message_index_outside_chunks_once();
                }
            }
            mcap::records::Record::ChunkIndex(chunk_index) => {
                if self.saw_top_level_message {
                    self.warn_message_index_outside_chunks_once();
                }
                if self
                    .chunk_indexes
                    .insert(chunk_index.chunk_start_offset, chunk_index.clone())
                    .is_some()
                {
                    self.error(format!(
                        "Multiple chunk indexes found for chunk at offset {}",
                        chunk_index.chunk_start_offset
                    ));
                }
            }
            mcap::records::Record::Statistics(statistics) => {
                if self.statistics.is_some() {
                    self.error("File contains multiple Statistics records");
                }
                self.statistics = Some(statistics);
            }
            mcap::records::Record::DataEnd(_) => {
                self.saw_data_end = true;
                self.in_summary_section = true;
            }
            mcap::records::Record::AttachmentIndex(_) => {}
            mcap::records::Record::Attachment { .. } => {}
            mcap::records::Record::Metadata(_) => {}
            mcap::records::Record::MetadataIndex(_) => {}
            mcap::records::Record::SummaryOffset(_) => {}
            mcap::records::Record::Unknown { opcode, .. } => {
                self.warn(format!(
                    "Encountered unknown top-level record opcode 0x{opcode:02x}"
                ));
            }
        }
    }

    fn finalize_record_presence(&mut self) {
        if !self.saw_data_end {
            self.error("File does not contain a DataEnd record");
        }
        if !self.saw_footer {
            self.error("File does not contain a Footer record");
        }
    }

    fn warn_message_index_outside_chunks_once(&mut self) {
        if self.message_index_warning_emitted {
            return;
        }
        self.message_index_warning_emitted = true;
        self.warn(
            "Message index in file has message records outside chunks. Indexed readers will miss these messages.",
        );
    }

    fn examine_schema(&mut self, header: mcap::records::SchemaHeader, data: &[u8]) {
        if header.encoding.is_empty() {
            if data.is_empty() {
                self.warn(format!(
                    "Schema with ID: {}, Name: {:?} has empty Encoding and Data fields",
                    header.id, header.name
                ));
            } else {
                self.error(format!(
                    "Schema with ID: {} has empty Encoding but Data is non-empty",
                    header.id
                ));
            }
        }

        if header.id == 0 {
            self.error("Schema.ID 0 is reserved. Do not make Schema records with ID 0.");
        }

        let parsed = ParsedSchema {
            header: header.clone(),
            data: data.to_vec(),
        };
        if let Some(previous) = self.schemas_in_data_section.get(&header.id).cloned() {
            if previous.header.name != header.name {
                self.error(format!(
                    "Two schema records with same ID {} but different names ({:?} != {:?})",
                    header.id, header.name, previous.header.name
                ));
            }
            if previous.header.encoding != header.encoding {
                self.error(format!(
                    "Two schema records with same ID {} but different encodings ({:?} != {:?})",
                    header.id, header.encoding, previous.header.encoding
                ));
            }
            if previous.data != parsed.data {
                self.error(format!(
                    "Two schema records with different data present with same ID {}",
                    header.id
                ));
            }
        }

        if self.in_summary_section {
            if !self.schemas_in_data_section.contains_key(&header.id) {
                self.error(format!(
                    "Schema with id {} in summary section does not exist in data section",
                    header.id
                ));
            }
            self.schema_ids_in_summary_section.insert(header.id);
        } else {
            if self.schemas_in_data_section.contains_key(&header.id) {
                self.warn(format!(
                    "Duplicate schema records in data section with ID {}",
                    header.id
                ));
            }
            self.schemas_in_data_section.insert(header.id, parsed);
        }
    }

    fn examine_channel(&mut self, channel: mcap::records::Channel) {
        if let Some(previous) = self.channels_in_data_section.get(&channel.id).cloned() {
            if previous.schema_id != channel.schema_id {
                self.error(format!(
                    "Two channel records with same ID {} but different schema IDs ({} != {})",
                    channel.id, channel.schema_id, previous.schema_id
                ));
            }
            if previous.topic != channel.topic {
                self.error(format!(
                    "Two channel records with same ID {} but different topics ({:?} != {:?})",
                    channel.id, channel.topic, previous.topic
                ));
            }
            if previous.message_encoding != channel.message_encoding {
                self.error(format!(
                    "Two channel records with same ID {} but different message encodings ({:?} != {:?})",
                    channel.id, channel.message_encoding, previous.message_encoding
                ));
            }
            if previous.metadata != channel.metadata {
                self.error(format!(
                    "Two channel records with different metadata present with same ID {}",
                    channel.id
                ));
            }
        }

        if self.in_summary_section {
            if !self.channels_in_data_section.contains_key(&channel.id) {
                self.error(format!(
                    "Channel with ID {} in summary section does not exist in data section",
                    channel.id
                ));
            }
            self.channel_ids_in_summary_section.insert(channel.id);
        } else {
            if self.channels_in_data_section.contains_key(&channel.id) {
                self.warn(format!(
                    "Duplicate channel records in data section with ID {}",
                    channel.id
                ));
            }
            self.channels_in_data_section
                .insert(channel.id, channel.clone());
        }

        if channel.schema_id != 0
            && !self
                .schemas_in_data_section
                .contains_key(&channel.schema_id)
        {
            self.error(format!(
                "Encountered Channel ({}) with unknown Schema ({})",
                channel.id, channel.schema_id
            ));
        }
    }

    fn examine_top_level_message(&mut self, header: &mcap::records::MessageHeader) {
        let channel_topic = self
            .channels_in_data_section
            .get(&header.channel_id)
            .map(|channel| channel.topic.clone());
        if channel_topic.is_none() {
            self.error(format!(
                "Got a Message record for channel: {} before a channel record.",
                header.channel_id
            ));
        }

        if let Some(previous) = self.last_top_level_message_time {
            if header.log_time < previous {
                let topic = channel_topic.as_deref().unwrap_or("<unknown>");
                self.error(format!(
                    "Message.log_time {} on {:?} is less than the previous message record time {}",
                    header.log_time, topic, previous
                ));
            }
        }
        self.last_top_level_message_time = Some(header.log_time);
        self.observe_message_time(header.log_time);
    }

    fn examine_chunk(
        &mut self,
        header: mcap::records::ChunkHeader,
        data: &[u8],
        start_offset: u64,
    ) {
        let mut referenced_channels = BTreeSet::new();
        let mut min_log_time = None::<u64>;
        let mut max_log_time = None::<u64>;
        let mut chunk_message_count = 0u64;

        let chunk_reader = match mcap::read::ChunkReader::new(header.clone(), data) {
            Ok(reader) => reader,
            Err(err) => {
                self.error(format!(
                    "failed to read chunk at offset {start_offset}: {err:#}"
                ));
                return;
            }
        };

        for nested_record in chunk_reader {
            let nested_record = match nested_record {
                Ok(record) => record,
                Err(err) => {
                    self.error(format!(
                        "failed to parse nested chunk record at {start_offset}: {err:#}"
                    ));
                    continue;
                }
            };
            match nested_record {
                mcap::records::Record::Schema { header, data } => {
                    self.examine_schema(header, data.as_ref());
                }
                mcap::records::Record::Channel(channel) => {
                    self.examine_channel(channel);
                }
                mcap::records::Record::Message { header, .. } => {
                    referenced_channels.insert(header.channel_id);
                    let channel_topic = self
                        .channels_in_data_section
                        .get(&header.channel_id)
                        .map(|channel| channel.topic.clone());
                    if channel_topic.is_none() {
                        self.error(format!(
                            "Got a Message record for channel: {} before a channel record.",
                            header.channel_id
                        ));
                    }

                    if let Some(latest_log_time) = self.global_max_log_time {
                        if header.log_time < latest_log_time {
                            let topic = channel_topic.as_deref().unwrap_or("<unknown>");
                            let message = format!(
                                "Message.log_time {} on {:?} is less than the latest log time {}",
                                header.log_time, topic, latest_log_time
                            );
                            if self.strict_message_order {
                                self.error(message);
                            } else {
                                self.warn(message);
                            }
                        }
                    }

                    min_log_time =
                        Some(min_log_time.map_or(header.log_time, |min| min.min(header.log_time)));
                    max_log_time =
                        Some(max_log_time.map_or(header.log_time, |max| max.max(header.log_time)));
                    chunk_message_count += 1;
                    self.observe_message_time(header.log_time);
                }
                other => {
                    self.error(format!(
                        "Illegal record in chunk: {}",
                        opcode_name(other.opcode())
                    ));
                }
            }
        }

        if chunk_message_count > 0 {
            let observed_min = min_log_time.expect("min present for non-empty chunk");
            let observed_max = max_log_time.expect("max present for non-empty chunk");
            if observed_min != header.message_start_time {
                self.error(format!(
                    "Chunk.message_start_time {} does not match the earliest message log time {}",
                    header.message_start_time, observed_min
                ));
            }
            if observed_max != header.message_end_time {
                self.error(format!(
                    "Chunk.message_end_time {} does not match the latest message log time {}",
                    header.message_end_time, observed_max
                ));
            }
        }

        self.channels_referenced_in_chunks_by_offset
            .insert(start_offset, referenced_channels);
    }

    fn observe_message_time(&mut self, log_time: u64) {
        self.message_count += 1;
        self.global_max_log_time = Some(
            self.global_max_log_time
                .map_or(log_time, |max| max.max(log_time)),
        );
        self.min_log_time = Some(self.min_log_time.map_or(log_time, |min| min.min(log_time)));
        self.max_log_time = Some(self.max_log_time.map_or(log_time, |max| max.max(log_time)));
    }

    fn validate_chunk_indexes(&mut self, mcap: &[u8]) {
        let chunk_indexes: Vec<(u64, mcap::records::ChunkIndex)> = self
            .chunk_indexes
            .iter()
            .map(|(offset, index)| (*offset, index.clone()))
            .collect();
        for (chunk_offset, chunk_index) in chunk_indexes {
            if let Some(channels_referenced) = self
                .channels_referenced_in_chunks_by_offset
                .get(&chunk_offset)
            {
                let channel_ids: Vec<u16> = channels_referenced.iter().copied().collect();
                for channel_id in channel_ids {
                    if !self.channel_ids_in_summary_section.contains(&channel_id) {
                        self.error(format!(
                            "Indexed chunk at offset {} contains messages referencing channel ({}) not duplicated in summary section",
                            chunk_offset, channel_id
                        ));
                    }
                    let schema_id = self
                        .channels_in_data_section
                        .get(&channel_id)
                        .map(|channel| channel.schema_id);
                    if let Some(schema_id) = schema_id {
                        if schema_id != 0
                            && !self.schema_ids_in_summary_section.contains(&schema_id)
                        {
                            self.error(format!(
                                "Indexed chunk at offset {} contains messages referencing schema ({}) not duplicated in summary section",
                                chunk_offset, schema_id
                            ));
                        }
                    }
                }
            }

            match read_record_at_offset(mcap, chunk_offset) {
                Ok(raw_record) => {
                    if raw_record.opcode != mcap::records::op::CHUNK {
                        self.error(format!(
                            "Chunk index points to offset {} but the record at this offset is a {}",
                            chunk_offset,
                            opcode_name(raw_record.opcode)
                        ));
                        continue;
                    }
                    if chunk_index.chunk_length != 9 + raw_record.length {
                        self.error(format!(
                            "Chunk index {} length mismatch: {} vs {}.",
                            chunk_offset,
                            chunk_index.chunk_length,
                            9 + raw_record.length
                        ));
                        continue;
                    }

                    let parsed = mcap::parse_record(raw_record.opcode, raw_record.body);
                    let Ok(mcap::records::Record::Chunk { header, .. }) = parsed else {
                        self.error(format!(
                            "Chunk index points to offset {} but encountered error parsing the chunk at that offset",
                            chunk_offset
                        ));
                        continue;
                    };

                    if header.message_start_time != chunk_index.message_start_time {
                        self.error(format!(
                            "Chunk at offset {} has message start time {}, but its chunk index has message start time {}",
                            chunk_offset, header.message_start_time, chunk_index.message_start_time
                        ));
                    }
                    if header.message_end_time != chunk_index.message_end_time {
                        self.error(format!(
                            "Chunk at offset {} has message end time {}, but its chunk index has message end time {}",
                            chunk_offset, header.message_end_time, chunk_index.message_end_time
                        ));
                    }
                    if header.compression != chunk_index.compression {
                        self.error(format!(
                            "Chunk at offset {} has compression {:?}, but its chunk index has compression {:?}",
                            chunk_offset, header.compression, chunk_index.compression
                        ));
                    }
                    if header.compressed_size != chunk_index.compressed_size {
                        self.error(format!(
                            "Chunk at offset {} has data length {}, but its chunk index has compressed size {}",
                            chunk_offset, header.compressed_size, chunk_index.compressed_size
                        ));
                    }
                    if header.uncompressed_size != chunk_index.uncompressed_size {
                        self.error(format!(
                            "Chunk at offset {} has uncompressed size {}, but its chunk index has uncompressed size {}",
                            chunk_offset, header.uncompressed_size, chunk_index.uncompressed_size
                        ));
                    }
                }
                Err(err) => {
                    self.error(format!(
                        "Chunk index points to offset {} but encountered error reading at that offset: {err}",
                        chunk_offset
                    ));
                }
            }
        }
    }

    fn validate_statistics(&mut self) {
        let Some(statistics) = self.statistics.clone() else {
            return;
        };

        if self.message_count > 0 {
            if let Some(min_log_time) = self.min_log_time {
                if statistics.message_start_time != min_log_time {
                    self.error(format!(
                        "Statistics has message start time {}, but the minimum message start time is {}",
                        statistics.message_start_time, min_log_time
                    ));
                }
            }
            if let Some(max_log_time) = self.max_log_time {
                if statistics.message_end_time != max_log_time {
                    self.error(format!(
                        "Statistics has message end time {}, but the maximum message end time is {}",
                        statistics.message_end_time, max_log_time
                    ));
                }
            }
        }
        if statistics.message_count != self.message_count {
            self.error(format!(
                "Statistics has message count {}, but actual number of messages is {}",
                statistics.message_count, self.message_count
            ));
        }
    }
}

struct RawRecordRef<'a> {
    opcode: u8,
    length: u64,
    body: &'a [u8],
}

fn read_record_at_offset<'a>(mcap: &'a [u8], offset: u64) -> Result<RawRecordRef<'a>> {
    let offset = usize::try_from(offset).context("record offset out of range")?;
    if offset + 9 > mcap.len() {
        anyhow::bail!("record header extends beyond file");
    }
    let opcode = mcap[offset];
    let length = u64::from_le_bytes(
        mcap[offset + 1..offset + 9]
            .try_into()
            .expect("slice length for record len"),
    );
    let end = offset
        .checked_add(9)
        .and_then(|start| start.checked_add(usize::try_from(length).ok()?))
        .context("record length overflow")?;
    if end > mcap.len() {
        anyhow::bail!("record extends beyond file");
    }
    Ok(RawRecordRef {
        opcode,
        length,
        body: &mcap[offset + 9..end],
    })
}

fn opcode_name(opcode: u8) -> Cow<'static, str> {
    match opcode {
        mcap::records::op::HEADER => Cow::Borrowed("Header"),
        mcap::records::op::FOOTER => Cow::Borrowed("Footer"),
        mcap::records::op::SCHEMA => Cow::Borrowed("Schema"),
        mcap::records::op::CHANNEL => Cow::Borrowed("Channel"),
        mcap::records::op::MESSAGE => Cow::Borrowed("Message"),
        mcap::records::op::CHUNK => Cow::Borrowed("Chunk"),
        mcap::records::op::MESSAGE_INDEX => Cow::Borrowed("MessageIndex"),
        mcap::records::op::CHUNK_INDEX => Cow::Borrowed("ChunkIndex"),
        mcap::records::op::ATTACHMENT => Cow::Borrowed("Attachment"),
        mcap::records::op::ATTACHMENT_INDEX => Cow::Borrowed("AttachmentIndex"),
        mcap::records::op::STATISTICS => Cow::Borrowed("Statistics"),
        mcap::records::op::METADATA => Cow::Borrowed("Metadata"),
        mcap::records::op::METADATA_INDEX => Cow::Borrowed("MetadataIndex"),
        mcap::records::op::SUMMARY_OFFSET => Cow::Borrowed("SummaryOffset"),
        mcap::records::op::DATA_END => Cow::Borrowed("DataEnd"),
        _ => Cow::Owned(format!("opcode 0x{opcode:02x}")),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::diagnose_mcap;

    fn write_chunked_mcap(
        configure: impl FnOnce(mcap::WriteOptions) -> mcap::WriteOptions,
        schema_id: Option<u16>,
        message_times: &[u64],
    ) -> Vec<u8> {
        let mut buffer = Vec::new();
        {
            let options = configure(mcap::WriteOptions::new().chunk_size(Some(64)));
            let mut writer = options
                .create(std::io::Cursor::new(&mut buffer))
                .expect("create writer");

            let schema_id = schema_id.unwrap_or_else(|| {
                writer
                    .add_schema("test_schema", "raw", b"{}")
                    .expect("add schema")
            });
            let channel_id = writer
                .add_channel(schema_id, "/demo", "raw", &BTreeMap::new())
                .expect("add channel");

            for (sequence, log_time) in message_times.iter().copied().enumerate() {
                writer
                    .write_to_known_channel(
                        &mcap::records::MessageHeader {
                            channel_id,
                            sequence: sequence as u32,
                            log_time,
                            publish_time: log_time,
                        },
                        b"payload",
                    )
                    .expect("write message");
            }

            writer.finish().expect("finish writer");
        }
        buffer
    }

    #[test]
    fn no_error_on_messageless_chunks() {
        let mcap = write_chunked_mcap(|opts| opts, Some(0), &[]);
        let diagnosis = diagnose_mcap(&mcap, false);
        assert!(diagnosis.errors.is_empty(), "{:?}", diagnosis.errors);
    }

    #[test]
    fn no_error_on_schemaless_messages() {
        let mcap = write_chunked_mcap(|opts| opts, Some(0), &[10]);
        let diagnosis = diagnose_mcap(&mcap, false);
        assert!(diagnosis.errors.is_empty(), "{:?}", diagnosis.errors);
    }

    #[test]
    fn requires_duplicated_schemas_for_indexed_messages() {
        let mcap = write_chunked_mcap(
            |opts| opts.repeat_channels(false).repeat_schemas(false),
            None,
            &[10],
        );
        let diagnosis = diagnose_mcap(&mcap, false);
        assert!(
            diagnosis
                .errors
                .iter()
                .any(|msg| msg.contains("channel (1)")),
            "{:?}",
            diagnosis.errors
        );
        assert!(
            diagnosis
                .errors
                .iter()
                .any(|msg| msg.contains("schema (1)")),
            "{:?}",
            diagnosis.errors
        );
    }

    #[test]
    fn passes_indexed_messages_with_repeated_schemas() {
        let mcap = write_chunked_mcap(|opts| opts, None, &[10]);
        let diagnosis = diagnose_mcap(&mcap, false);
        assert!(diagnosis.errors.is_empty(), "{:?}", diagnosis.errors);
    }

    #[test]
    fn strict_message_order_toggles_warning_vs_error() {
        let mcap = write_chunked_mcap(|opts| opts, None, &[20, 10]);

        let non_strict = diagnose_mcap(&mcap, false);
        assert!(
            non_strict
                .warnings
                .iter()
                .any(|msg| msg.contains("less than the latest log time")),
            "{:?}",
            non_strict.warnings
        );
        assert!(non_strict.errors.is_empty(), "{:?}", non_strict.errors);

        let strict = diagnose_mcap(&mcap, true);
        assert!(
            strict
                .errors
                .iter()
                .any(|msg| msg.contains("less than the latest log time")),
            "{:?}",
            strict.errors
        );
    }
}
