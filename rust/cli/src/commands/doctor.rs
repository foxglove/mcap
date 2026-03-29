use std::collections::{HashMap, HashSet};

use anyhow::{Context, Result};
use mcap::{
    records,
    sans_io::{LinearReadEvent, LinearReader as SansIoReader, LinearReaderOptions},
};

use crate::{cli::DoctorArgs, cli_io};

#[derive(Default)]
struct Diagnosis {
    errors: Vec<String>,
    warnings: Vec<String>,
}

#[derive(Clone, Eq, PartialEq)]
struct StoredSchema {
    name: String,
    encoding: String,
    data: Vec<u8>,
}

#[derive(Clone)]
struct ObservedChunk {
    header: records::ChunkHeader,
    chunk_length: u64,
    referenced_channels: HashSet<u16>,
}

struct DoctorState {
    strict_message_order: bool,
    in_summary_section: bool,
    saw_data_end: bool,
    saw_footer: bool,
    saw_message_outside_chunk: bool,
    warned_outside_chunk_indexing: bool,
    schemas_in_data: HashMap<u16, StoredSchema>,
    channels_in_data: HashMap<u16, records::Channel>,
    schema_ids_in_summary: HashSet<u16>,
    channel_ids_in_summary: HashSet<u16>,
    chunk_indexes: HashMap<u64, records::ChunkIndex>,
    observed_chunks: HashMap<u64, ObservedChunk>,
    statistics: Option<records::Statistics>,
    message_count: u64,
    message_start_time: u64,
    message_end_time: u64,
    latest_message_time: Option<u64>,
    diagnosis: Diagnosis,
}

impl DoctorState {
    fn new(strict_message_order: bool) -> Self {
        Self {
            strict_message_order,
            in_summary_section: false,
            saw_data_end: false,
            saw_footer: false,
            saw_message_outside_chunk: false,
            warned_outside_chunk_indexing: false,
            schemas_in_data: HashMap::new(),
            channels_in_data: HashMap::new(),
            schema_ids_in_summary: HashSet::new(),
            channel_ids_in_summary: HashSet::new(),
            chunk_indexes: HashMap::new(),
            observed_chunks: HashMap::new(),
            statistics: None,
            message_count: 0,
            message_start_time: u64::MAX,
            message_end_time: 0,
            latest_message_time: None,
            diagnosis: Diagnosis::default(),
        }
    }

    fn error<S: Into<String>>(&mut self, message: S) {
        self.diagnosis.errors.push(message.into());
    }

    fn warn<S: Into<String>>(&mut self, message: S) {
        self.diagnosis.warnings.push(message.into());
    }

    fn note_message_time(&mut self, log_time: u64, topic: Option<&str>) {
        if let Some(previous) = self.latest_message_time {
            if log_time < previous {
                let topic_text = topic.unwrap_or("<unknown>");
                let detail = format!(
                    "message.log_time {log_time} on topic '{topic_text}' is less than previous message log time {previous}"
                );
                if self.strict_message_order {
                    self.error(detail);
                } else {
                    self.warn(detail);
                }
            }
        }
        self.latest_message_time = Some(log_time);
        self.message_count += 1;
        self.message_start_time = self.message_start_time.min(log_time);
        self.message_end_time = self.message_end_time.max(log_time);
    }

    fn examine_schema(&mut self, schema: records::SchemaHeader, data: Vec<u8>) {
        if schema.id == 0 {
            self.error("schema id 0 is reserved");
        }
        let current = StoredSchema {
            name: schema.name.clone(),
            encoding: schema.encoding.clone(),
            data,
        };
        if let Some(existing) = self.schemas_in_data.get(&schema.id) {
            if existing != &current {
                self.error(format!(
                    "schema {} conflicts with a previous schema definition",
                    schema.id
                ));
            } else if !self.in_summary_section {
                self.warn(format!(
                    "duplicate schema records in data section with id {}",
                    schema.id
                ));
            }
        }

        if self.in_summary_section {
            if !self.schemas_in_data.contains_key(&schema.id) {
                self.error(format!(
                    "schema {} in summary section does not exist in data section",
                    schema.id
                ));
            }
            self.schema_ids_in_summary.insert(schema.id);
        } else {
            self.schemas_in_data.entry(schema.id).or_insert(current);
        }
    }

    fn examine_channel(&mut self, channel: records::Channel) {
        if let Some(existing) = self.channels_in_data.get(&channel.id) {
            if existing != &channel {
                self.error(format!(
                    "channel {} conflicts with a previous channel definition",
                    channel.id
                ));
            } else if !self.in_summary_section {
                self.warn(format!(
                    "duplicate channel records in data section with id {}",
                    channel.id
                ));
            }
        }

        if channel.schema_id != 0 && !self.schemas_in_data.contains_key(&channel.schema_id) {
            self.error(format!(
                "channel {} references unknown schema {}",
                channel.id, channel.schema_id
            ));
        }

        if self.in_summary_section {
            if !self.channels_in_data.contains_key(&channel.id) {
                self.error(format!(
                    "channel {} in summary section does not exist in data section",
                    channel.id
                ));
            }
            self.channel_ids_in_summary.insert(channel.id);
        } else {
            self.channels_in_data.entry(channel.id).or_insert(channel);
        }
    }

    fn examine_chunk(
        &mut self,
        chunk_start_offset: u64,
        header: records::ChunkHeader,
        data: Vec<u8>,
        chunk_length: u64,
    ) {
        let mut referenced_channels = HashSet::new();
        let mut chunk_reader = match mcap::read::ChunkReader::new(header.clone(), &data) {
            Ok(reader) => reader,
            Err(err) => {
                self.error(format!(
                    "failed to open chunk at offset {chunk_start_offset}: {err}"
                ));
                return;
            }
        };

        let mut chunk_message_count = 0u64;
        let mut chunk_min_log_time = u64::MAX;
        let mut chunk_max_log_time = 0u64;

        while let Some(record) = chunk_reader.next() {
            let record = match record {
                Ok(record) => record,
                Err(err) => {
                    self.error(format!(
                        "failed reading record from chunk at offset {chunk_start_offset}: {err}"
                    ));
                    return;
                }
            };

            match record {
                records::Record::Schema { header, data } => {
                    self.examine_schema(header, data.into_owned());
                }
                records::Record::Channel(channel) => {
                    self.examine_channel(channel);
                }
                records::Record::Message { header, .. } => {
                    referenced_channels.insert(header.channel_id);
                    let topic = self
                        .channels_in_data
                        .get(&header.channel_id)
                        .map(|channel| channel.topic.clone());
                    if topic.is_none() {
                        self.error(format!(
                            "chunk message references unknown channel {}",
                            header.channel_id
                        ));
                    }
                    self.note_message_time(header.log_time, topic.as_deref());

                    chunk_message_count += 1;
                    chunk_min_log_time = chunk_min_log_time.min(header.log_time);
                    chunk_max_log_time = chunk_max_log_time.max(header.log_time);
                }
                other => {
                    self.error(format!(
                        "illegal record type 0x{:02x} found inside chunk at offset {}",
                        other.opcode(),
                        chunk_start_offset
                    ));
                }
            }
        }

        if chunk_message_count > 0 {
            if header.message_start_time != chunk_min_log_time {
                self.error(format!(
                    "chunk {} start time {} does not match earliest message {}",
                    chunk_start_offset, header.message_start_time, chunk_min_log_time
                ));
            }
            if header.message_end_time != chunk_max_log_time {
                self.error(format!(
                    "chunk {} end time {} does not match latest message {}",
                    chunk_start_offset, header.message_end_time, chunk_max_log_time
                ));
            }
        }

        self.observed_chunks.insert(
            chunk_start_offset,
            ObservedChunk {
                header,
                chunk_length,
                referenced_channels,
            },
        );
    }

    fn process_top_level_record(&mut self, offset: u64, opcode: u8, data: &[u8], length: u64) {
        let record = match mcap::parse_record(opcode, data) {
            Ok(record) => record,
            Err(err) => {
                self.error(format!(
                    "failed to parse top-level record 0x{opcode:02x} at offset {offset}: {err}"
                ));
                return;
            }
        };

        match record {
            records::Record::Header(header) => {
                if header.library.is_empty() {
                    self.warn(
                        "header.library is empty; consider setting it to identify the writing software",
                    );
                }
            }
            records::Record::Footer(_) => {
                self.saw_footer = true;
            }
            records::Record::Schema { header, data } => {
                self.examine_schema(header, data.into_owned());
            }
            records::Record::Channel(channel) => {
                self.examine_channel(channel);
            }
            records::Record::Message { header, .. } => {
                self.saw_message_outside_chunk = true;
                let topic = self
                    .channels_in_data
                    .get(&header.channel_id)
                    .map(|channel| channel.topic.clone());
                if topic.is_none() {
                    self.error(format!(
                        "top-level message references unknown channel {}",
                        header.channel_id
                    ));
                }
                self.note_message_time(header.log_time, topic.as_deref());
            }
            records::Record::Chunk { header, data } => {
                self.examine_chunk(offset, header, data.into_owned(), length);
            }
            records::Record::MessageIndex(_) => {
                if self.saw_message_outside_chunk && !self.warned_outside_chunk_indexing {
                    self.warn(
                        "message indexes are present while messages exist outside chunks; indexed readers may miss top-level messages",
                    );
                    self.warned_outside_chunk_indexing = true;
                }
            }
            records::Record::ChunkIndex(index) => {
                if self.saw_message_outside_chunk && !self.warned_outside_chunk_indexing {
                    self.warn(
                        "message indexes are present while messages exist outside chunks; indexed readers may miss top-level messages",
                    );
                    self.warned_outside_chunk_indexing = true;
                }
                if self
                    .chunk_indexes
                    .insert(index.chunk_start_offset, index.clone())
                    .is_some()
                {
                    self.error(format!(
                        "multiple chunk index records found for chunk offset {}",
                        index.chunk_start_offset
                    ));
                }
            }
            records::Record::Statistics(stats) => {
                if self.statistics.is_some() {
                    self.error("file contains multiple statistics records");
                }
                self.statistics = Some(stats);
            }
            records::Record::DataEnd(_) => {
                self.saw_data_end = true;
                self.in_summary_section = true;
            }
            _ => {}
        }
    }

    fn finalize(mut self) -> Result<()> {
        if !self.saw_data_end {
            self.error("file does not contain a data end record");
        }
        if !self.saw_footer {
            self.error("file does not contain a footer record");
        }

        let chunk_indexes = self
            .chunk_indexes
            .iter()
            .map(|(offset, index)| (*offset, index.clone()))
            .collect::<Vec<_>>();

        for (chunk_offset, chunk_index) in chunk_indexes {
            let Some(observed) = self.observed_chunks.get(&chunk_offset).cloned() else {
                self.error(format!(
                    "chunk index points to offset {} but no chunk was observed there",
                    chunk_offset
                ));
                continue;
            };

            if chunk_index.chunk_length != observed.chunk_length {
                self.error(format!(
                    "chunk index {} length mismatch: index={}, chunk={}",
                    chunk_offset, chunk_index.chunk_length, observed.chunk_length
                ));
            }
            if chunk_index.message_start_time != observed.header.message_start_time {
                self.error(format!(
                    "chunk index {} message_start_time mismatch: index={}, chunk={}",
                    chunk_offset,
                    chunk_index.message_start_time,
                    observed.header.message_start_time
                ));
            }
            if chunk_index.message_end_time != observed.header.message_end_time {
                self.error(format!(
                    "chunk index {} message_end_time mismatch: index={}, chunk={}",
                    chunk_offset, chunk_index.message_end_time, observed.header.message_end_time
                ));
            }
            if chunk_index.compression != observed.header.compression {
                self.error(format!(
                    "chunk index {} compression mismatch: index='{}', chunk='{}'",
                    chunk_offset, chunk_index.compression, observed.header.compression
                ));
            }
            if chunk_index.compressed_size != observed.header.compressed_size {
                self.error(format!(
                    "chunk index {} compressed_size mismatch: index={}, chunk={}",
                    chunk_offset, chunk_index.compressed_size, observed.header.compressed_size
                ));
            }
            if chunk_index.uncompressed_size != observed.header.uncompressed_size {
                self.error(format!(
                    "chunk index {} uncompressed_size mismatch: index={}, chunk={}",
                    chunk_offset, chunk_index.uncompressed_size, observed.header.uncompressed_size
                ));
            }

            for channel_id in observed.referenced_channels {
                if !self.channel_ids_in_summary.contains(&channel_id) {
                    self.error(format!(
                        "indexed chunk at offset {} references channel {} not duplicated in summary section",
                        chunk_offset, channel_id
                    ));
                }
                if let Some(channel) = self.channels_in_data.get(&channel_id) {
                    if channel.schema_id != 0
                        && !self.schema_ids_in_summary.contains(&channel.schema_id)
                    {
                        self.error(format!(
                            "indexed chunk at offset {} references schema {} not duplicated in summary section",
                            chunk_offset, channel.schema_id
                        ));
                    }
                }
            }
        }

        if let Some(stats) = self.statistics.clone() {
            if stats.message_count != self.message_count {
                self.error(format!(
                    "statistics message count {} does not match observed {}",
                    stats.message_count, self.message_count
                ));
            }
            if self.message_count > 0 {
                if stats.message_start_time != self.message_start_time {
                    self.error(format!(
                        "statistics message_start_time {} does not match observed {}",
                        stats.message_start_time, self.message_start_time
                    ));
                }
                if stats.message_end_time != self.message_end_time {
                    self.error(format!(
                        "statistics message_end_time {} does not match observed {}",
                        stats.message_end_time, self.message_end_time
                    ));
                }
            }
        }

        for warning in &self.diagnosis.warnings {
            eprintln!("Warning: {warning}");
        }
        for error in &self.diagnosis.errors {
            eprintln!("Error: {error}");
        }

        println!(
            "Doctor completed with {} error(s), {} warning(s).",
            self.diagnosis.errors.len(),
            self.diagnosis.warnings.len()
        );

        if !self.diagnosis.errors.is_empty() {
            anyhow::bail!("encountered {} errors", self.diagnosis.errors.len());
        }
        Ok(())
    }
}

pub fn run(args: DoctorArgs) -> Result<()> {
    let bytes = cli_io::open_local_mcap(&args.file)?;
    eprintln!("Examining {}", args.file.display());
    let default_record_limit = bytes.len().saturating_mul(4).max(1024 * 1024);

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let mut state = DoctorState::new(args.strict_message_order);
        let mut reader = SansIoReader::new_with_options(
            LinearReaderOptions::default()
                .with_emit_chunks(true)
                .with_record_length_limit(default_record_limit)
                .with_validate_chunk_crcs(true),
        );
        let mut consumed = 0usize;
        let mut stream_offset = mcap::MAGIC.len() as u64;

        while let Some(event) = reader.next_event() {
            match event.context("failed reading MCAP records")? {
                LinearReadEvent::ReadRequest(need) => {
                    let remaining = bytes.len().saturating_sub(consumed);
                    let len = remaining.min(need);
                    reader
                        .insert(len)
                        .copy_from_slice(&bytes[consumed..consumed + len]);
                    reader.notify_read(len);
                    consumed += len;
                }
                LinearReadEvent::Record { data, opcode } => {
                    let length = (1 + 8 + data.len()) as u64;
                    let offset = stream_offset;
                    stream_offset += length;
                    state.process_top_level_record(offset, opcode, data, length);
                }
            }
        }

        state.finalize()
    }));

    match result {
        Ok(inner) => inner,
        Err(_) => anyhow::bail!("doctor failed due to parser panic while scanning records"),
    }
}
