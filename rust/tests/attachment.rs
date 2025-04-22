mod common;

use common::*;
use mcap::records::AttachmentHeader;

use std::{borrow::Cow, io::BufWriter};

use anyhow::Result;
use memmap2::Mmap;
use tempfile::tempfile;

#[test]
fn smoke() -> Result<()> {
    let mapped = map_mcap("../tests/conformance/data/OneAttachment/OneAttachment.mcap")?;
    let attachments = mcap::read::LinearReader::new(&mapped)?
        .filter_map(|record| match record.unwrap() {
            mcap::records::Record::Attachment { header, data, crc } => Some((header, data, crc)),
            _ => None,
        })
        .collect::<Vec<_>>();

    assert_eq!(attachments.len(), 1);

    let expected_header = mcap::records::AttachmentHeader {
        log_time: 2,
        create_time: 1,
        name: String::from("myFile"),
        media_type: String::from("application/octet-stream"),
    };

    assert_eq!(attachments[0].0, expected_header);
    assert_eq!(&*attachments[0].1, &[1, 2, 3]);

    Ok(())
}

#[test]
fn test_attach_in_multiple_parts() -> Result<()> {
    let mut tmp = tempfile()?;
    let mut writer = mcap::Writer::new(BufWriter::new(&mut tmp))?;

    let data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
    let (left, right) = data.split_at(5);

    writer.start_attachment(
        10,
        AttachmentHeader {
            log_time: 100,
            create_time: 200,
            name: "great-attachment".into(),
            media_type: "application/octet-stream".into(),
        },
    )?;

    writer.put_attachment_bytes(left)?;
    writer.put_attachment_bytes(right)?;

    writer.finish_attachment()?;

    drop(writer);

    let ours = unsafe { Mmap::map(&tmp) }?;
    let summary = mcap::Summary::read(&ours)?;

    let expected_summary = Some(mcap::Summary {
        stats: Some(mcap::records::Statistics {
            attachment_count: 1,
            ..Default::default()
        }),
        attachment_indexes: vec![mcap::records::AttachmentIndex {
            // offset depends on the length of the embedded library string, which includes the crate version
            offset: 33 + (env!("CARGO_PKG_VERSION").len() as u64),
            length: 95,
            log_time: 100,
            create_time: 200,
            data_size: 10,
            name: "great-attachment".into(),
            media_type: "application/octet-stream".into(),
        }],
        ..Default::default()
    });
    assert_eq!(summary, expected_summary);

    let expected_attachment = mcap::Attachment {
        log_time: 100,
        create_time: 200,
        name: "great-attachment".into(),
        media_type: "application/octet-stream".into(),
        data: Cow::Borrowed(&[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]),
        crc: 2147504207,
    };

    assert_eq!(
        mcap::read::attachment(&ours, &summary.unwrap().attachment_indexes[0])?,
        expected_attachment
    );

    Ok(())
}

#[test]
fn round_trip() -> Result<()> {
    let mapped = map_mcap("../tests/conformance/data/OneAttachment/OneAttachment.mcap")?;
    let attachments =
        mcap::read::LinearReader::new(&mapped)?.filter_map(|record| match record.unwrap() {
            mcap::records::Record::Attachment { header, data, crc } => Some((header, data, crc)),
            _ => None,
        });

    let mut tmp = tempfile()?;
    let mut writer = mcap::Writer::new(BufWriter::new(&mut tmp))?;

    for (h, d, crc) in attachments {
        let a = mcap::Attachment {
            log_time: h.log_time,
            create_time: h.create_time,
            media_type: h.media_type,
            name: h.name,
            data: Cow::Borrowed(&d),
            crc,
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
            // offset depends on the length of the embedded library string, which includes the crate version
            offset: 33 + (env!("CARGO_PKG_VERSION").len() as u64),
            length: 78,
            log_time: 2,
            create_time: 1,
            data_size: 3,
            name: String::from("myFile"),
            media_type: String::from("application/octet-stream"),
        }],
        ..Default::default()
    });
    assert_eq!(summary, expected_summary);

    let expected_attachment = mcap::Attachment {
        log_time: 2,
        create_time: 1,
        name: String::from("myFile"),
        media_type: String::from("application/octet-stream"),
        data: Cow::Borrowed(&[1, 2, 3]),
        crc: 171394340,
    };

    assert_eq!(
        mcap::read::attachment(&ours, &summary.unwrap().attachment_indexes[0])?,
        expected_attachment
    );

    Ok(())
}
