//	info := &Info{
//		Statistics:        it.statistics,
//		Channels:          it.channels.ToMap(),
//		ChunkIndexes:      it.chunkIndexes,
//		AttachmentIndexes: it.attachmentIndexes,
//		MetadataIndexes:   it.metadataIndexes,
//		Schemas:           it.schemas.ToMap(),
//		Footer:            it.footer,
//		Header:            r.header,
//	}

use std::{io::SeekFrom, ops::Range, pin::Pin};

use mcap::{
    records::{
        AttachmentIndex, Channel, ChunkIndex, Footer, Header, MetadataIndex, Record, SchemaHeader,
        Statistics,
    },
    tokio::read::Options,
};

use crate::{
    error::{CliError, CliResult},
    traits::McapReader,
};

pub struct McapInfo {
    pub statistics: Option<Statistics>,
    pub channels: Vec<Channel>,
    pub chunk_indexes: Vec<ChunkIndex>,
    pub attachment_indexes: Vec<AttachmentIndex>,
    pub metadata_indexes: Vec<MetadataIndex>,
    pub schemas: Vec<SchemaHeader>,
    pub footer: Footer,
    pub header: Header,
}

type RecordReader = mcap::tokio::read::RecordReader<Pin<Box<dyn McapReader>>>;

const MIN_PREFETCH_SIZE: u64 = 4096;

fn create_prefetch_range(start: u64) -> Range<u64> {
    start..start + MIN_PREFETCH_SIZE
}

async fn read_summary_records(
    reader: &mut RecordReader,
    summary_offset_start: u64,
) -> CliResult<Vec<Record>> {
    if summary_offset_start == 0 {
        return Ok(Vec::with_capacity(0));
    }

    reader
        .as_base_reader_mut()?
        .prefetch(create_prefetch_range(summary_offset_start))
        .await;

    let mut offsets = vec![];

    reader.seek(SeekFrom::Start(summary_offset_start)).await?;

    while let Some(Record::SummaryOffset(offset)) = reader.read_record().await? {
        offsets.push(offset);
    }

    let min_offset_start_position = offsets.iter().map(|x| x.group_start).min();
    let max_offset_end_position = offsets.iter().map(|x| x.group_start + x.group_length).max();

    if let (Some(min), Some(max)) = (min_offset_start_position, max_offset_end_position) {
        reader.as_base_reader_mut()?.prefetch(min..max).await;
    }

    let mut records = vec![];

    for offset in offsets.iter() {
        reader.seek(SeekFrom::Start(offset.group_start)).await?;

        let end = offset.group_start + offset.group_length;

        while let Some(record) = reader.read_record().await? {
            if record.opcode() != offset.group_opcode {
                return Err(CliError::UnexpectedResponse(format!(
                    "summary group opcode was {:02x} but record opcode was {:02x}",
                    offset.group_opcode,
                    record.opcode()
                )));
            }

            records.push(record.into_owned());

            let current_position = reader.position().await?;

            if current_position >= end {
                break;
            }
        }
    }

    Ok(records)
}

pub async fn read_info(reader: Pin<Box<dyn McapReader>>) -> CliResult<McapInfo> {
    let options = Options {
        // skip the end magic so running over the
        // skip_end_magic: true,
        ..Default::default()
    };
    let mut reader = RecordReader::new_with_options(reader, &options);

    let mut statistics = None;
    let mut channels = vec![];
    let mut chunk_indexes = vec![];
    let mut attachment_indexes = vec![];
    let mut metadata_indexes = vec![];
    let mut schemas = vec![];

    let Some(Record::Header(header)) = reader.read_record().await? else {
        return Err(CliError::UnexpectedResponse(
            "Expected first record to be header record".into(),
        ));
    };

    let footer = reader.seek_and_read_footer().await?;
    let summary = read_summary_records(&mut reader, footer.summary_offset_start).await?;

    for record in summary.into_iter() {
        match record {
            Record::Statistics(stats) => {
                statistics = Some(stats);
            }

            Record::Channel(channel) => {
                channels.push(channel);
            }

            Record::ChunkIndex(index) => {
                chunk_indexes.push(index);
            }

            Record::AttachmentIndex(index) => {
                attachment_indexes.push(index);
            }

            Record::MetadataIndex(index) => {
                metadata_indexes.push(index);
            }

            Record::Schema { header, .. } => {
                schemas.push(header);
            }

            record => {
                CliError::UnexpectedResponse(format!(
                    "Received unexpected record in summary response. Record opcode: {:02x}",
                    record.opcode()
                ));
            }
        }
    }

    Ok(McapInfo {
        statistics,
        channels,
        chunk_indexes,
        attachment_indexes,
        metadata_indexes,
        schemas,
        footer,
        header,
    })
}
