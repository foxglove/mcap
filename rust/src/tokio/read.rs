use tokio::io::{AsyncRead, AsyncReadExt};

pub use crate::sans_io::read::LinearReaderOptions;
use crate::sans_io::read::{LinearReader as SansIoReader, ReadAction};
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
        Self::new_with_options(reader, &LinearReaderOptions::default())
    }

    pub fn new_with_options(source: R, options: &LinearReaderOptions) -> Self {
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
        while let Some(action) = self.reader.next_action() {
            match action {
                Ok(ReadAction::NeedMore(n)) => {
                    let written = match self.source.read(self.reader.insert(n)).await {
                        Ok(n) => n,
                        Err(err) => return Some(Err(err.into())),
                    };
                    self.reader.set_written(written);
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
        None
    }
}
