use std::{
    borrow::Cow, collections::BTreeMap, fs::OpenOptions, io::BufWriter, sync::Arc, time::Duration,
};

use mcap::{
    parse_record,
    records::{MessageHeader, Record},
    tokio::RecordReader,
    write::{Metadata, Writer},
    Attachment, Channel, Schema, WriteOptions,
};
use regex::Regex;
use tracing::debug;

use crate::{
    cli::FilterArgs,
    error::{CliError, CliResult},
    reader::McapFd,
};

struct TopicMatcher {
    include_topics: Vec<Regex>,
    exclude_topics: Vec<Regex>,
}

impl TopicMatcher {
    fn new(
        include_topics: impl IntoIterator<Item = String>,
        exclude_topics: impl IntoIterator<Item = String>,
    ) -> Result<Self, regex::Error> {
        let include_topics = include_topics
            .into_iter()
            // Rust regex needs ^ and $ to match a string in entirety
            .map(|x| Regex::try_from(format!("^{x}$")))
            .collect::<Result<Vec<_>, _>>()?;

        let exclude_topics = exclude_topics
            .into_iter()
            // Rust regex needs ^ and $ to match a string in entirety
            .map(|x| Regex::try_from(format!("^{x}$")))
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Self {
            include_topics,
            exclude_topics,
        })
    }

    fn is_match(&self, topic: &str) -> bool {
        if self.exclude_topics.iter().any(|re| re.is_match(topic)) {
            return false;
        }

        // Since any() returns false when there is an empty array, actually check the length isn't
        // zero before seeing if any of the topics match.
        //
        // We want to include the topics by default if no specific includes are mentioned.
        if !self.include_topics.is_empty() {
            return self.include_topics.iter().any(|re| re.is_match(topic));
        }

        true
    }
}

fn get_start_and_end_time(input: &FilterArgs) -> CliResult<(u64, Option<u64>)> {
    let start_time = match (input.start_secs, input.start_nsecs) {
        (Some(secs), None) => Duration::from_secs(secs).as_nanos() as u64,
        (None, Some(nanos)) => nanos,
        // If neither were provided just return zero to read the whole file
        (None, None) => 0,
        (Some(_), Some(_)) => {
            return Err(CliError::UnexpectedResponse(
                "Both start seconds and start nanoseconds were provided but only one can be used"
                    .to_string(),
            ));
        }
    };

    let end_time = match (input.end_secs, input.end_nsecs) {
        (Some(secs), None) => Some(Duration::from_secs(secs).as_nanos() as u64),
        (None, Some(nanos)) => Some(nanos),
        // If neither were then return None to read the whole file
        (None, None) => None,
        (Some(_), Some(_)) => {
            return Err(CliError::UnexpectedResponse(
                "Both end seconds and end nanoseconds were provided but only one can be used"
                    .to_string(),
            ));
        }
    };

    Ok((start_time, end_time))
}

pub async fn filter_mcap(input: FilterArgs) -> CliResult<()> {
    let (start_time, end_time) = get_start_and_end_time(&input)?;

    let include_attachments = input.include_attachments;
    let include_metadata = input.include_metadata;

    let should_include_time = |time: u64| match (start_time, end_time) {
        // If there is an end make sure the time is between those bounds
        (start, Some(end)) => time >= start && time <= end,
        // If there is no end assume the whole file past the start
        (start, None) => time >= start,
    };

    let matcher = TopicMatcher::new(input.include_topic_regex, input.exclude_topic_regex)?;

    let input_file = McapFd::parse(input.path)?;

    // Currently only writing to files is supported as [`Writer`] needs a value that implements
    // [`Seek`].
    let mut output_writer = BufWriter::new(
        OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(input.output)?,
    );

    let mut record_reader = RecordReader::new(input_file.create_reader().await?);

    // Since records are read and then immediately written we don't need to own the [`Record`]
    // values. Create a temporary buffer that the [`RecordReader`] will read into before writing
    // the records.
    let mut buffer = vec![];

    // Both the channel ids and schemas will be rewritten for the filtered file.
    // Store all the schemas and a map of input channel id to output channel id to reconcile the
    // messages.
    let mut schemas = BTreeMap::new();
    let mut channel_map = BTreeMap::new();

    let Some(op) = record_reader.next_record(&mut buffer).await.transpose()? else {
        return Err(CliError::UnexpectedResponse(
            "Failed to read the beginning of the MCAP file".to_string(),
        ));
    };

    let Record::Header(header) = parse_record(op, &buffer)? else {
        return Err(CliError::UnexpectedResponse(format!(
            "First record of file should have been a header, but got record with opcode {:2x}",
            op
        )));
    };

    let mut record_writer = Writer::with_options(
        &mut output_writer,
        WriteOptions::default()
            .compression(input.output_compression.into())
            .chunk_size(Some(input.chunk_size))
            .profile(header.profile),
    )?;

    while let Some(op) = record_reader.next_record(&mut buffer).await.transpose()? {
        let record = parse_record(op, &buffer)?;

        match record {
            Record::Schema { header, data } => {
                // If we've already seen this schema just continue.
                // It's likely from the summary section.
                if schemas.contains_key(&header.id) {
                    continue;
                }

                schemas.insert(
                    header.id,
                    Arc::new(Schema {
                        name: header.name,
                        encoding: header.encoding,
                        data: Cow::Owned(data.into_owned()),
                    }),
                );
            }

            Record::Channel(channel) => {
                let schema = schemas.get(&channel.schema_id).cloned();

                if !matcher.is_match(&channel.topic) {
                    continue;
                }

                let new_channel = Channel {
                    topic: channel.topic,
                    schema,
                    metadata: channel.metadata,
                    message_encoding: channel.message_encoding,
                };

                let new_channel_id = record_writer.add_channel(&new_channel)?;

                channel_map.insert(channel.id, new_channel_id);
            }

            Record::Message { header, data } => {
                let Some(channel_id) = channel_map.get(&header.channel_id).cloned() else {
                    debug!(
                        "Recieved message for channel {} but it was missing",
                        header.channel_id
                    );
                    continue;
                };

                if !should_include_time(header.log_time) {
                    continue;
                }

                record_writer.write_to_known_channel(
                    &MessageHeader {
                        channel_id,
                        sequence: header.sequence,
                        log_time: header.log_time,
                        publish_time: header.publish_time,
                    },
                    &data,
                )?;
            }

            Record::Chunk { .. } => {
                return Err(CliError::UnexpectedResponse("internal error: recieved chunk record from reader despite emit_chunks being turned off".to_string()));
            }

            Record::Attachment { header, data } => {
                if !include_attachments {
                    continue;
                }

                if !should_include_time(header.log_time) {
                    continue;
                }

                record_writer.attach(&Attachment {
                    name: header.name,
                    media_type: header.media_type,
                    create_time: header.create_time,
                    log_time: header.log_time,
                    data
                })?;
            }

            Record::Metadata(metadata) => {
                if !include_metadata {
                    continue;
                }

                record_writer.write_metadata(&Metadata { name: metadata.name, metadata: metadata.metadata })?;
            }

            Record::DataEnd(_) => {
                // Since that's all of the data done we don't need to keep reading.
                // What follows is just the summary section.
                break;
            }

            Record::Header(_) => {
                return Err(CliError::UnexpectedResponse("Header record was repeated in MCAP file".to_string()));
            },

            // Ignore all the summary and index information as this will be added in
            | Record::MessageIndex(_)
            | Record::ChunkIndex(_)
            | Record::AttachmentIndex(_)
            | Record::MetadataIndex(_)
            | Record::SummaryOffset(_)
            | Record::Statistics(_)
            | Record::Footer(_)

            // Ignore unknown records
            | Record::Unknown { .. } => {
                // skip
            }
        };
    }

    record_writer.finish()?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::TopicMatcher;

    #[test]
    fn test_topic_matcher_empty() {
        let matcher = TopicMatcher::new([], []).expect("failed to create topic matcher");

        assert!(matcher.is_match("blah"));
        assert!(matcher.is_match(""));
        assert!(matcher.is_match("1234"));
    }

    #[test]
    fn test_topic_matcher_pass() {
        let matcher = TopicMatcher::new([r"[0-9A-z_\/]*".to_string()], [])
            .expect("failed to create topic matcher");

        assert!(matcher.is_match("blah"));
        assert!(matcher.is_match("1234"));
        assert!(matcher.is_match(""));
        assert!(matcher.is_match("blah/blah"));

        assert!(matcher.is_match("/diagnostics"));
        assert!(matcher.is_match("/image_color/compressed"));
        assert!(matcher.is_match("/tf"));
        assert!(matcher.is_match("/velodyne_points"));
        assert!(matcher.is_match("/radar/points"));
        assert!(matcher.is_match("/radar/range"));
        assert!(matcher.is_match("/radar/tracks"));
    }

    #[test]
    fn test_topic_matcher_exclude() {
        let matcher = TopicMatcher::new([], [r"/radar.*".to_string()])
            .expect("failed to create topic matcher");

        assert!(matcher.is_match("/diagnostics"));
        assert!(matcher.is_match("/image_color/compressed"));
        assert!(matcher.is_match("/tf"));
        assert!(matcher.is_match("/velodyne_points"));

        assert!(!matcher.is_match("/radar/points"));
        assert!(!matcher.is_match("/radar/range"));
        assert!(!matcher.is_match("/radar/tracks"));
    }

    #[test]
    fn test_topic_matcher_exclude_precedence() {
        let matcher = TopicMatcher::new(
            [
                // include everything
                ".*".to_string(),
            ],
            [
                // exclude radar
                r"/radar.*".to_string(),
            ],
        )
        .expect("failed to create topic matcher");

        assert!(matcher.is_match("/diagnostics"));
        assert!(matcher.is_match("/image_color/compressed"));

        assert!(!matcher.is_match("/radar/points"));
    }
}
