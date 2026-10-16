mod common;

use common::*;
use mcap::records::AttachmentHeader;

use std::{borrow::Cow, io::BufWriter, io::Cursor};

use anyhow::Result;
use tempfile::tempfile;

const DEFAULT_LIBRARY_LENGTH: u64 = mcap::LIBRARY_IDENTIFIER.len() as u64;

fn attachment_body(data: &[u8]) -> Vec<u8> {
    let mut writer = mcap::Writer::new(Cursor::new(Vec::new())).expect("writer");
    writer
        .attach(&mcap::Attachment {
            log_time: 20,
            create_time: 10,
            name: "tiny".into(),
            media_type: "application/octet-stream".into(),
            data: Cow::Borrowed(data),
        })
        .expect("attachment");
    writer.finish().expect("finish");
    let input = writer.into_inner().into_inner();
    let index = mcap::Summary::read(&input)
        .expect("summary")
        .expect("summary present")
        .attachment_indexes
        .remove(0);
    input[index.offset as usize + 9..(index.offset + index.length) as usize].to_vec()
}

#[test]
fn parse_record_rejects_truncated_attachment() {
    let options = mcap::ParseOptions::default().with_validate_attachment_crcs(false);
    for data in [&[][..], &[1, 2, 3][..]] {
        let body = attachment_body(data);
        for length in 0..body.len() {
            assert!(mcap::parse_record(mcap::records::op::ATTACHMENT, &body[..length]).is_err());
            assert!(mcap::parse_record_with_options(
                mcap::records::op::ATTACHMENT,
                &body[..length],
                &options,
            )
            .is_err());
        }
    }
}

#[test]
fn attachment_crc_validation_options() {
    let mut body = attachment_body(&[1, 2, 3]);
    let end = body.len();
    body[end - 4] ^= 0xFF;
    let saved_crc = u32::from_le_bytes(body[end - 4..].try_into().expect("CRC"));
    for options in [
        mcap::ParseOptions::default(),
        mcap::ParseOptions::default().with_validate_attachment_crcs(true),
    ] {
        assert!(matches!(
            mcap::parse_record_with_options(mcap::records::op::ATTACHMENT, &body, &options),
            Err(mcap::McapError::BadAttachmentCrc { .. })
        ));
    }
    assert!(matches!(
        mcap::parse_record(mcap::records::op::ATTACHMENT, &body),
        Err(mcap::McapError::BadAttachmentCrc { .. })
    ));

    let options = mcap::ParseOptions::default().with_validate_attachment_crcs(false);
    match mcap::parse_record_with_options(mcap::records::op::ATTACHMENT, &body, &options)
        .expect("CRC validation disabled")
    {
        mcap::records::Record::Attachment { header, data, crc } => {
            assert_eq!(header.log_time, 20);
            assert_eq!(header.create_time, 10);
            assert_eq!(header.name, "tiny");
            assert_eq!(header.media_type, "application/octet-stream");
            assert_eq!(crc, saved_crc);
            assert!(matches!(data, Cow::Borrowed(_)));
            assert_eq!(data.as_ref(), [1, 2, 3]);
            assert_eq!(data.as_ptr(), body[end - 7..end - 4].as_ptr());
        }
        _ => panic!("expected attachment"),
    }
}

#[test]
fn attachment_crc_options_accept_valid_and_zero_crcs() {
    let mut body = attachment_body(&[1, 2, 3]);
    let end = body.len();
    for zero_crc in [false, true] {
        if zero_crc {
            body[end - 4..].fill(0);
        }
        assert!(mcap::parse_record(mcap::records::op::ATTACHMENT, &body).is_ok());
        for validate in [false, true] {
            let options = mcap::ParseOptions::default().with_validate_attachment_crcs(validate);
            assert!(mcap::parse_record_with_options(
                mcap::records::op::ATTACHMENT,
                &body,
                &options
            )
            .is_ok());
        }
    }
}

#[test]
fn attachment_crc_options_reject_invalid_data_length() {
    let mut body = attachment_body(&[1, 2, 3]);
    let data_length_offset = body.len() - 4 - 3 - 8;
    body[data_length_offset..data_length_offset + 8].copy_from_slice(&u64::MAX.to_le_bytes());
    for validate in [false, true] {
        let options = mcap::ParseOptions::default().with_validate_attachment_crcs(validate);
        assert!(matches!(
            mcap::parse_record_with_options(mcap::records::op::ATTACHMENT, &body, &options),
            Err(mcap::McapError::BadAttachmentLength { .. })
        ));
    }
}

#[test]
fn smoke() -> Result<()> {
    let mapped = read_mcap("../../tests/conformance/data/OneAttachment/OneAttachment.mcap")?;
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

    let (header, data, crc) = attachments[0].clone();

    assert_eq!(header, expected_header);
    assert_eq!(data, &[1u8, 2u8, 3u8] as &[u8]);
    assert_eq!(crc, 171394340);

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

    let ours = read_back(&mut tmp)?;
    let summary = mcap::Summary::read(&ours)?;

    let expected_summary = Some(mcap::Summary {
        stats: Some(mcap::records::Statistics {
            attachment_count: 1,
            ..Default::default()
        }),
        attachment_indexes: vec![mcap::records::AttachmentIndex {
            // offset depends on the length of the embedded library string, which includes the crate version
            offset: 25 + DEFAULT_LIBRARY_LENGTH,
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
    };

    assert_eq!(
        mcap::read::attachment(&ours, &summary.unwrap().attachment_indexes[0])?,
        expected_attachment
    );

    Ok(())
}

#[test]
fn round_trip() -> Result<()> {
    let mapped = read_mcap("../../tests/conformance/data/OneAttachment/OneAttachment.mcap")?;
    let attachments =
        mcap::read::LinearReader::new(&mapped)?.filter_map(|record| match record.unwrap() {
            mcap::records::Record::Attachment { header, data, .. } => Some((header, data)),
            _ => None,
        });

    let mut tmp = tempfile()?;
    let mut writer = mcap::Writer::new(BufWriter::new(&mut tmp))?;

    for (h, d) in attachments {
        let a = mcap::Attachment {
            log_time: h.log_time,
            create_time: h.create_time,
            media_type: h.media_type,
            name: h.name,
            data: Cow::Borrowed(&d),
        };
        writer.attach(&a)?;
    }
    drop(writer);

    let ours = read_back(&mut tmp)?;
    let summary = mcap::Summary::read(&ours)?;

    let expected_summary = Some(mcap::Summary {
        stats: Some(mcap::records::Statistics {
            attachment_count: 1,
            ..Default::default()
        }),
        attachment_indexes: vec![mcap::records::AttachmentIndex {
            // offset depends on the length of the embedded library string, which includes the crate version
            offset: 25 + DEFAULT_LIBRARY_LENGTH,
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
    };

    assert_eq!(
        mcap::read::attachment(&ours, &summary.unwrap().attachment_indexes[0])?,
        expected_attachment
    );

    Ok(())
}
