//! Example helper: [`read_summary`] loads only the summary section from a [`Read`] + [`Seek`]
//! source. For messages, use [`mcap::io::MessageReader`].
#![allow(dead_code)]

use std::io::{Read, Seek};

use anyhow::Result;
use mcap::{
    sans_io::{SummaryReadEvent, SummaryReader},
    Summary,
};

/// Reads only the summary section (footer, indexes, statistics) from a seekable source.
///
/// Returns `Ok(None)` when the file has no summary section.
pub fn read_summary(source: &mut (impl Read + Seek)) -> Result<Option<Summary>> {
    let mut reader = SummaryReader::new();
    while let Some(event) = reader.next_event() {
        match event? {
            SummaryReadEvent::SeekRequest(pos) => {
                let at = source.seek(pos)?;
                reader.notify_seeked(at);
            }
            SummaryReadEvent::ReadRequest(need) => {
                let read = source.read(reader.insert(need))?;
                reader.notify_read(read);
            }
        }
    }
    Ok(reader.finish())
}
