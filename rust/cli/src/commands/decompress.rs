use crate::cli::{DecompressArgs, FilterArgs};

use super::filter;

pub fn run(args: DecompressArgs) -> anyhow::Result<()> {
    filter::run(FilterArgs {
        file: args.file,
        output: args.output,
        include_topic_regex: Vec::new(),
        exclude_topic_regex: Vec::new(),
        include_metadata: true,
        include_attachments: true,
        start: None,
        end: None,
        output_compression: "none".to_string(),
        chunk_size: args.chunk_size,
        unchunked: false,
    })
}
