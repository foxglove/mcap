use tokio::io::{AsyncRead, AsyncReadExt};

pub use crate::sans_io::read::RecordReaderOptions;
use crate::sans_io::read::{ReadAction, RecordReader as SansIoReader};
use crate::McapResult;

/// Reads an MCAP file record-by-record, writing the raw record data into a caller-provided Vec.
/// ```no_run
/// use std::fs;
///
/// use tokio::fs::File;
///
/// async fn read_it() {
///     let file = File::open("in.mcap").await.expect("couldn't open file");
///     let mut record_buf: Vec<u8> = Vec::new();
///     let mut reader = mcap::tokio::RecordReader::new(file);
///     while let Some(result) = reader.next_record(&mut record_buf).await {
///         let opcode = result.expect("couldn't read next record");
///         let raw_record = mcap::parse_record(opcode, &record_buf[..]).expect("couldn't parse");
///         // do something with the record...
///     }
/// }
/// ```
pub struct RecordReader<R> {
    source: R,
    reader: SansIoReader,
}

impl<R> RecordReader<R>
where
    R: AsyncRead + std::marker::Unpin,
{
    pub fn new(reader: R) -> Self {
        Self::new_with_options(reader, &RecordReaderOptions::default())
    }

    pub fn new_with_options(source: R, options: &RecordReaderOptions) -> Self {
        Self {
            reader: SansIoReader::new_with_options(options.clone()),
            source,
        }
    }

    pub fn into_inner(self) -> McapResult<R> {
        Ok(self.source)
    }

    /// Reads the next record from the input stream and copies the raw content into `data`.
    /// Returns the record's opcode as a result.
    pub async fn next_record(&mut self, data: &mut Vec<u8>) -> Option<McapResult<u8>> {
        loop {
            match self.reader.next_action() {
                Ok(ReadAction::Fill(mut into_buf)) => {
                    let written = match self.source.read(into_buf.buf).await {
                        Ok(n) => n,
                        Err(err) => return Some(Err(err.into())),
                    };
                    into_buf.set_filled(written);
                }
                Ok(ReadAction::Finished) => {
                    return None;
                }
                Ok(ReadAction::GetRecord {
                    data: content,
                    opcode,
                }) => {
                    data.resize(content.len(), 0);
                    data.copy_from_slice(content);
                    return Some(Ok(opcode));
                }
                Err(err) => {
                    return Some(Err(err));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::read::parse_record;
    use crate::records;
    use std::collections::BTreeMap;

    use super::*;
    #[tokio::test]
    async fn test_record_reader() -> McapResult<()> {
        for compression in [
            None,
            #[cfg(feature = "zstd")]
            Some(crate::Compression::Zstd),
            #[cfg(feature = "lz4")]
            Some(crate::Compression::Lz4),
        ] {
            let mut buf = std::io::Cursor::new(Vec::new());
            {
                let mut writer = crate::WriteOptions::new()
                    .compression(compression)
                    .create(&mut buf)?;
                let channel = std::sync::Arc::new(crate::Channel {
                    topic: "chat".to_owned(),
                    schema: None,
                    message_encoding: "json".to_owned(),
                    metadata: BTreeMap::new(),
                });
                writer.add_channel(&channel)?;
                writer.write(&crate::Message {
                    channel,
                    sequence: 0,
                    log_time: 0,
                    publish_time: 0,
                    data: (&[0, 1, 2]).into(),
                })?;
                writer.finish()?;
            }
            let mut reader = RecordReader::new(std::io::Cursor::new(buf.into_inner()));
            let mut record = Vec::new();
            let mut opcodes: Vec<u8> = Vec::new();
            while let Some(opcode) = reader.next_record(&mut record).await {
                let opcode = opcode?;
                opcodes.push(opcode);
                parse_record(opcode, &record)?;
            }
            assert_eq!(
                opcodes.as_slice(),
                [
                    records::op::HEADER,
                    records::op::CHANNEL,
                    records::op::MESSAGE,
                    records::op::MESSAGE_INDEX,
                    records::op::DATA_END,
                    records::op::CHANNEL,
                    records::op::CHUNK_INDEX,
                    records::op::STATISTICS,
                    records::op::SUMMARY_OFFSET,
                    records::op::SUMMARY_OFFSET,
                    records::op::SUMMARY_OFFSET,
                    records::op::FOOTER,
                ],
                "reads opcodes from MCAP compressed with {:?}",
                compression
            );
        }
        Ok(())
    }
    #[cfg(feature = "lz4")]
    #[tokio::test]
    async fn test_lz4_decompression() -> McapResult<()> {
        let mut buf = std::io::Cursor::new(Vec::new());
        {
            let mut writer = crate::WriteOptions::new()
                .compression(Some(crate::Compression::Lz4))
                .create(&mut buf)?;
            let channel = std::sync::Arc::new(crate::Channel {
                topic: "chat".to_owned(),
                schema: None,
                message_encoding: "json".to_owned(),
                metadata: BTreeMap::new(),
            });
            let data: Vec<u8> = vec![0; 1024];
            writer.add_channel(&channel)?;
            for n in 0..10000 {
                {
                    writer.write(&crate::Message {
                        channel: channel.clone(),
                        log_time: n,
                        publish_time: n,
                        sequence: n as u32,
                        data: std::borrow::Cow::Owned(data.clone()),
                    })?;
                }
            }
            writer.finish()?;
        }
        let mut reader = RecordReader::new(std::io::Cursor::new(buf.into_inner()));
        let mut record = Vec::new();
        while let Some(opcode) = reader.next_record(&mut record).await {
            parse_record(opcode?, &record)?;
        }
        Ok(())
    }
}
