mod common;

use common::*;

use std::{borrow::Cow, io::BufWriter};

use anyhow::Result;
use memmap::Mmap;
use tempfile::tempfile;

#[test]
fn smoke() -> Result<()> {
    let mapped = map_mcap("../tests/conformance/data/OneAttachment/OneAttachment.mcap")?;
    let attachments = mcap::read::LinearReader::new(&mapped)?
        .filter_map(|record| match record.unwrap() {
            mcap::records::Record::Attachment { header, data } => Some((header, data)),
            _ => None,
        })
        .collect::<Vec<_>>();

    assert_eq!(attachments.len(), 1);

    let expected_header = mcap::records::AttachmentHeader {
        log_time: 2,
        create_time: 1,
        name: String::from("myFile"),
        content_type: String::from("application/octet-stream"),
    };

    assert_eq!(attachments[0].0, expected_header);
    assert_eq!(attachments[0].1, &[1, 2, 3]);

    Ok(())
}

#[test]
fn round_trip() -> Result<()> {
    let mapped = map_mcap("../tests/conformance/data/OneAttachment/OneAttachment.mcap")?;
    let attachments =
        mcap::read::LinearReader::new(&mapped)?.filter_map(|record| match record.unwrap() {
            mcap::records::Record::Attachment { header, data } => Some((header, data)),
            _ => None,
        });

    let mut tmp = tempfile()?;
    let mut writer = mcap::Writer::new(BufWriter::new(&mut tmp))?;

    for (h, d) in attachments {
        let a = mcap::Attachment {
            log_time: h.log_time,
            create_time: h.create_time,
            content_type: h.content_type,
            name: h.name,
            data: Cow::Borrowed(d),
        };
        writer.attach(&a)?;
    }
    drop(writer);

    let ours = unsafe { Mmap::map(&tmp) }?;
    let summary = mcap::Summary::read(&ours)?;

    let expected_summary = Some(mcap::Summary {
        stats: Some(mcap::records::Statistics {
            attachment_count: 1,
            ..Default::default()
        }),
        attachment_indexes: vec![mcap::records::AttachmentIndex {
            offset: 38, // Finicky - depends on the length of the library version string
            length: 78,
            log_time: 2,
            create_time: 1,
            data_size: 3,
            name: String::from("myFile"),
            content_type: String::from("application/octet-stream"),
        }],
        ..Default::default()
    });
    assert_eq!(summary, expected_summary);

    let expected_attachment = mcap::Attachment {
        log_time: 2,
        create_time: 1,
        name: String::from("myFile"),
        content_type: String::from("application/octet-stream"),
        data: Cow::Borrowed(&[1, 2, 3]),
    };

    assert_eq!(
        mcap::read::attachment(&ours, &summary.unwrap().attachment_indexes[0])?,
        expected_attachment
    );

    Ok(())
}
