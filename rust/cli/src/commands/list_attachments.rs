use anyhow::Result;

use crate::{cli::InputFile, cli_io, output};

pub fn run(args: InputFile) -> Result<()> {
    let mcap_bytes = cli_io::open_local_mcap(&args.file)?;
    let summary = mcap::Summary::read(&mcap_bytes)?.unwrap_or_default();

    let mut rows = vec![vec![
        "name".to_string(),
        "media type".to_string(),
        "log time".to_string(),
        "creation time".to_string(),
        "content length".to_string(),
        "offset".to_string(),
    ]];

    for idx in summary.attachment_indexes {
        rows.push(vec![
            idx.name,
            idx.media_type,
            idx.log_time.to_string(),
            idx.create_time.to_string(),
            idx.data_size.to_string(),
            idx.offset.to_string(),
        ]);
    }

    output::print_rows(&rows)
}
