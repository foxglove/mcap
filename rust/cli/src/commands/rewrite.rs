use std::{
    borrow::Cow,
    collections::HashMap,
    io::BufWriter,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use mcap::records::{self, Record};

use crate::cli_io;

fn temp_output_path(path: &Path) -> Result<PathBuf> {
    let filename = path
        .file_name()
        .map(|v| v.to_string_lossy().into_owned())
        .ok_or_else(|| anyhow::anyhow!("path has no filename: {}", path.display()))?;
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system time is before UNIX_EPOCH")?
        .as_nanos();
    Ok(path.with_file_name(format!(
        "{filename}.mcap-cli-tmp-{}-{nanos}",
        std::process::id()
    )))
}

pub fn rewrite_mcap_with_appends(
    path: &Path,
    new_attachment: Option<mcap::Attachment<'static>>,
    new_metadata: Option<records::Metadata>,
) -> Result<()> {
    let input_bytes = cli_io::open_local_mcap(path)?;
    let mut records = mcap::read::ChunkFlattener::new(&input_bytes)
        .with_context(|| format!("failed to parse records in {}", path.display()))?;

    let header = match records.next() {
        Some(Ok(Record::Header(header))) => header,
        Some(Ok(_)) => anyhow::bail!(
            "file does not start with an MCAP header: {}",
            path.display()
        ),
        Some(Err(err)) => {
            return Err(err).with_context(|| format!("failed reading {}", path.display()))
        }
        None => anyhow::bail!("empty file: {}", path.display()),
    };

    let tmp_path = temp_output_path(path)?;
    let out_file = std::fs::File::create(&tmp_path)
        .with_context(|| format!("failed to create temporary file {}", tmp_path.display()))?;
    let out = BufWriter::new(out_file);
    let mut writer = mcap::WriteOptions::new()
        .profile(header.profile)
        .library(header.library)
        .create(out)
        .with_context(|| format!("failed to create MCAP writer for {}", tmp_path.display()))?;

    let mut schema_ids: HashMap<u16, u16> = HashMap::new();
    let mut channel_ids: HashMap<u16, u16> = HashMap::new();

    for record in records {
        match record
            .with_context(|| format!("failed while rewriting records from {}", path.display()))?
        {
            Record::Header(_) => {}
            Record::Schema { header, data } => {
                let new_schema_id = writer
                    .add_schema(&header.name, &header.encoding, &data)
                    .with_context(|| format!("failed writing schema {}", header.name))?;
                schema_ids.insert(header.id, new_schema_id);
            }
            Record::Channel(channel) => {
                let mapped_schema = if channel.schema_id == 0 {
                    0
                } else {
                    *schema_ids.get(&channel.schema_id).ok_or_else(|| {
                        anyhow::anyhow!(
                            "channel '{}' references unknown schema {}",
                            channel.topic,
                            channel.schema_id
                        )
                    })?
                };
                let new_channel_id = writer
                    .add_channel(
                        mapped_schema,
                        &channel.topic,
                        &channel.message_encoding,
                        &channel.metadata,
                    )
                    .with_context(|| format!("failed writing channel {}", channel.topic))?;
                channel_ids.insert(channel.id, new_channel_id);
            }
            Record::Message { header, data } => {
                let mapped_channel = *channel_ids.get(&header.channel_id).ok_or_else(|| {
                    anyhow::anyhow!("message references unknown channel {}", header.channel_id)
                })?;
                let message_header = records::MessageHeader {
                    channel_id: mapped_channel,
                    sequence: header.sequence,
                    log_time: header.log_time,
                    publish_time: header.publish_time,
                };
                writer
                    .write_to_known_channel(&message_header, &data)
                    .context("failed writing message")?;
            }
            Record::Attachment { header, data, .. } => {
                let attachment = mcap::Attachment {
                    log_time: header.log_time,
                    create_time: header.create_time,
                    name: header.name,
                    media_type: header.media_type,
                    data: Cow::Owned(data.into_owned()),
                };
                writer
                    .attach(&attachment)
                    .with_context(|| format!("failed writing attachment {}", attachment.name))?;
            }
            Record::Metadata(metadata) => {
                writer
                    .write_metadata(&metadata)
                    .with_context(|| format!("failed writing metadata {}", metadata.name))?;
            }
            Record::Unknown { opcode, data } => {
                if opcode >= 0x80 {
                    writer
                        .write_private_record(opcode, &data, Default::default())
                        .with_context(|| {
                            format!("failed writing private record opcode {opcode:#x}")
                        })?;
                }
            }
            Record::Footer(_)
            | Record::Chunk { .. }
            | Record::MessageIndex(_)
            | Record::ChunkIndex(_)
            | Record::AttachmentIndex(_)
            | Record::Statistics(_)
            | Record::MetadataIndex(_)
            | Record::SummaryOffset(_)
            | Record::DataEnd(_) => {}
        }
    }

    if let Some(metadata) = new_metadata {
        writer
            .write_metadata(&metadata)
            .with_context(|| format!("failed appending metadata {}", metadata.name))?;
    }

    if let Some(attachment) = new_attachment {
        writer
            .attach(&attachment)
            .with_context(|| format!("failed appending attachment {}", attachment.name))?;
    }

    writer
        .finish()
        .with_context(|| format!("failed finalizing rewritten file {}", tmp_path.display()))?;
    std::fs::rename(&tmp_path, path).with_context(|| {
        format!(
            "failed replacing {} with {}",
            path.display(),
            tmp_path.display()
        )
    })?;
    Ok(())
}
