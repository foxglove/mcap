mod common;

use common::*;

use std::io::BufWriter;

use anyhow::Result;
use memmap::Mmap;
use tempfile::tempfile;

#[test]
fn smoke() -> Result<()> {
    let mapped = map_mcap("../tests/conformance/data/OneMetadata/OneMetadata.mcap")?;
    let metas = mcap::read::LinearReader::new(&mapped)?
        .filter_map(|record| match record.unwrap() {
            mcap::records::Record::Metadata(m) => Some(m),
            _ => None,
        })
        .collect::<Vec<_>>();

    assert_eq!(metas.len(), 1);

    let expected = mcap::records::Metadata {
        name: String::from("myMetadata"),
        metadata: [(String::from("foo"), String::from("bar"))].into(),
    };

    assert_eq!(metas[0], expected);

    Ok(())
}

#[test]
fn round_trip() -> Result<()> {
    let mapped = map_mcap("../tests/conformance/data/OneMetadata/OneMetadata.mcap")?;
    let metas =
        mcap::read::LinearReader::new(&mapped)?.filter_map(|record| match record.unwrap() {
            mcap::records::Record::Metadata(m) => Some(m),
            _ => None,
        });

    let mut tmp = tempfile()?;
    let mut writer = mcap::Writer::new(BufWriter::new(&mut tmp))?;

    for m in metas {
        writer.write_metadata(&m)?;
    }
    drop(writer);

    let ours = unsafe { Mmap::map(&tmp) }?;
    let summary = mcap::Summary::read(&ours)?;

    let expected_summary = Some(mcap::Summary {
        stats: Some(mcap::records::Statistics {
            metadata_count: 1,
            ..Default::default()
        }),
        metadata_indexes: vec![mcap::records::MetadataIndex {
            // offset depends on the length of the embedded library string, which includes the crate version
            offset: 33 + (env!("CARGO_PKG_VERSION").len() as u64),
            length: 41,
            name: String::from("myMetadata"),
        }],
        ..Default::default()
    });
    assert_eq!(summary, expected_summary);

    let expected = mcap::records::Metadata {
        name: String::from("myMetadata"),
        metadata: [(String::from("foo"), String::from("bar"))].into(),
    };

    assert_eq!(
        mcap::read::metadata(&ours, &summary.unwrap().metadata_indexes[0])?,
        expected
    );

    Ok(())
}
