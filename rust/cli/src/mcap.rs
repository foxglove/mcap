use std::{io::SeekFrom, ops::Range, pin::Pin};

use mcap::{
    records::{
        AttachmentIndex, Channel, ChunkIndex, Footer, Header, MetadataIndex, Record, SchemaHeader,
        Statistics,
    },
    tokio::read::RecordReaderOptions,
};
use tracing::{debug, instrument, warn};

use crate::{
    error::{CliError, CliResult},
    reader::SeekableMcapReader,
};

/// The information specified by the header, footer and summary sections of the MCAP file.
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

type RecordReader = mcap::tokio::read::RecordReader<Pin<Box<dyn SeekableMcapReader>>>;

/// The minimum amount that should be prefetched when doing a prefetch operation.
///
/// It's useful to buffer certain amounts of the file in memory as below a certain size latency
/// becomes the bottleneck for networks requests, not bandwidth.
const MIN_PREFETCH_SIZE: u64 = 8192;

/// Create a range for prefetching a small amount from a certain offset.
pub fn default_prefetch(start: u64) -> Range<u64> {
    start..start + MIN_PREFETCH_SIZE
}

async fn read_summary_records_slow(
    reader: &mut RecordReader,
    summary_start: u64,
) -> CliResult<Vec<Record>> {
    let mut records = vec![];

    reader.seek(SeekFrom::Start(summary_start)).await?;

    while let Some(
        record @ (Record::Statistics(_)
        | Record::Channel(_)
        | Record::ChunkIndex(_)
        | Record::AttachmentIndex(_)
        | Record::MetadataIndex(_)
        | Record::Schema { .. }),
    ) = reader.read_record().await?
    {
        records.push(record.into_owned());
    }

    Ok(records)
}

/// Using the provided [`RecordReader`] read all the summary information after the provided
/// summary offset start.
///
/// This operation will increment the readers internal position.
#[instrument(skip(reader))]
async fn read_summary_records_from_offset(
    reader: &mut RecordReader,
    summary_offset_start: u64,
) -> CliResult<Vec<Record>> {
    reader
        .as_base_reader_mut()?
        .prefetch(default_prefetch(summary_offset_start))
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

        loop {
            let current_position = reader.position().await?;

            // If the position we're at (the end of the previous record) is past the end of the
            // group then bail out. This makes sure we only read in the offset group.
            if current_position >= end {
                break;
            }

            let Some(record) = reader.read_record().await? else {
                break;
            };

            if record.opcode() != offset.group_opcode {
                return Err(CliError::UnexpectedResponse(format!(
                    "summary group opcode was 0x{:02x} but record opcode was 0x{:02x}",
                    offset.group_opcode,
                    record.opcode()
                )));
            }

            records.push(record.into_owned());
        }
    }

    Ok(records)
}

/// For a provided reader backed by an MCAP file, read all the information in the summary section.
///
/// This method takes the following approach:
///
/// 1. Read the start of the file and extract the header
/// 2. Seek to the end of the file and read the footer
/// 3. Using the offsets specified in the footer, jump to and read the summary offset section
/// 4. Using the summary offsets (if they are available), read the summary section
/// 5. Return all the summary information available
#[instrument(skip(reader))]
pub async fn read_info(reader: Pin<Box<dyn SeekableMcapReader>>) -> CliResult<McapInfo> {
    let options = RecordReaderOptions {
        // skip the end magic so overreading doesn't throw errors
        skip_end_magic: true,
        ..Default::default()
    };
    let mut reader = RecordReader::new_with_options(reader, &options);

    let mut statistics = None;
    let mut channels = vec![];
    let mut chunk_indexes = vec![];
    let mut attachment_indexes = vec![];
    let mut metadata_indexes = vec![];
    let mut schemas = vec![];

    // Since nothing has been read yet, calling read_record() will read the start of the MCAP file
    // and check for the magic, returning the header.
    let Some(Record::Header(header)) = reader.read_record().await? else {
        return Err(CliError::UnexpectedResponse(
            "Expected first record to be header record".into(),
        ));
    };

    let footer = reader.seek_and_read_footer().await?;

    // It's more efficient to get the summary information from the summary offset section as we're
    // able to preftch the entire summary section using the group lenghts provided. If there are
    // no summary offset records then fall back to reading the entire summary - which may be slow dependning on the size.
    let summary = if footer.summary_offset_start > 0 {
        read_summary_records_from_offset(&mut reader, footer.summary_offset_start).await?
    } else if footer.summary_start > 0 {
        debug!(
            target = "mcap::cli",
            "Summary offset was missing from footer. Reading full summary instead."
        );
        read_summary_records_slow(&mut reader, footer.summary_start).await?
    } else {
        debug!(
            target = "mcap::cli",
            "Summary section missing from file, ignoring."
        );
        Vec::with_capacity(0)
    };

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

            // The MCAP spec says that only the above records can be in the summary section.
            // However for backwards compatibility reasons don't throw an error here, just warn.
            record => {
                warn!(
                    target = "mcap::cli",
                    "Received unexpected record in summary response. Record opcode: {:02x}",
                    record.opcode()
                );
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
