//! The `merge` command: k-way merge several MCAP inputs into one output ordered by log time. This
//! is a thin adapter over the shared [`crate::rewrite`] merge pipeline, which owns the read/merge/
//! write machinery and the standardized record placement.
use anyhow::Result;
use log::warn;

use crate::cli::MergeCommand;
use crate::context::CommandContext;
use crate::rewrite::{self, MergeOptions};
use crate::source::SourceOptions;

pub fn run(ctx: &CommandContext, args: MergeCommand) -> Result<()> {
    // `merge` keeps its own args struct (its positional file list collides with the single-input
    // `CommonRewriteArgs`), so it warns about the deprecated `--output-file` alias itself.
    if args.output_file.is_some() {
        warn!("--output-file is deprecated; use --output instead");
    }
    rewrite::run_merge(
        build_merge_options(args),
        SourceOptions::new(ctx.allow_remote_scan()),
    )
}

/// Maps the parsed `merge` CLI arguments onto the engine-facing [`MergeOptions`]. The output path
/// prefers `--output`, falling back to the deprecated `--output-file` alias.
fn build_merge_options(args: MergeCommand) -> MergeOptions {
    MergeOptions {
        files: args.files,
        output: args.output.or(args.output_file),
        compression: args.compression.to_compression(),
        chunk_size: args.chunk_size,
        include_crc: !args.no_crc,
        chunked: !args.no_chunks,
        allow_duplicate_metadata: args.allow_duplicate_metadata,
        coalesce_channels: args.coalesce_channels,
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::build_merge_options;
    use crate::cli::{CoalesceChannels, CompressionFormat, MergeCommand};

    #[test]
    fn build_merge_options_maps_cli_fields() {
        let options = build_merge_options(MergeCommand {
            files: vec!["a.mcap".into(), "b.mcap".into()],
            output: Some("out.mcap".into()),
            output_file: None,
            compression: CompressionFormat::Lz4,
            chunk_size: 4096,
            no_crc: true,
            no_chunks: true,
            allow_duplicate_metadata: true,
            coalesce_channels: CoalesceChannels::Force,
        });

        assert_eq!(
            options.files,
            vec![PathBuf::from("a.mcap"), PathBuf::from("b.mcap")]
        );
        assert_eq!(options.output, Some(PathBuf::from("out.mcap")));
        assert!(matches!(options.compression, Some(mcap::Compression::Lz4)));
        assert_eq!(options.chunk_size, 4096);
        assert!(!options.include_crc);
        assert!(!options.chunked);
        assert!(options.allow_duplicate_metadata);
        assert_eq!(options.coalesce_channels, CoalesceChannels::Force);
    }

    #[test]
    fn build_merge_options_resolves_output_preferring_output_over_output_file() {
        let base = || MergeCommand {
            files: vec!["a.mcap".into()],
            output: None,
            output_file: None,
            compression: CompressionFormat::Zstd,
            chunk_size: mcap::WriteOptions::DEFAULT_CHUNK_SIZE,
            no_crc: false,
            no_chunks: false,
            allow_duplicate_metadata: false,
            coalesce_channels: CoalesceChannels::Auto,
        };

        // `--output` wins when both are supplied.
        let both = build_merge_options(MergeCommand {
            output: Some("out.mcap".into()),
            output_file: Some("legacy.mcap".into()),
            ..base()
        });
        assert_eq!(both.output, Some(PathBuf::from("out.mcap")));

        // The deprecated `--output-file` supplies the path when `--output` is absent.
        let fallback = build_merge_options(MergeCommand {
            output_file: Some("legacy.mcap".into()),
            ..base()
        });
        assert_eq!(fallback.output, Some(PathBuf::from("legacy.mcap")));
    }
}
