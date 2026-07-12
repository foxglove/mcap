//! The `merge` command: a thin adapter over the shared [`crate::rewrite`] engine. `merge` is just a
//! rewrite with multiple inputs and the merge-only knobs set (log-time order, metadata dedup, and
//! channel coalescing); the engine dispatches to its k-way merge phase when given more than one
//! input.
use anyhow::Result;
use log::warn;

use crate::cli::MergeCommand;
use crate::context::CommandContext;
use crate::rewrite::{self, RewriteOptions};
use crate::source::SourceOptions;

pub fn run(ctx: &CommandContext, args: MergeCommand) -> Result<()> {
    // `merge` keeps its own args struct (its positional file list collides with the single-input
    // `CommonRewriteArgs`), so it warns about the deprecated `--output-file` alias itself.
    if args.output_file.is_some() {
        warn!("--output-file is deprecated; use --output instead");
    }
    rewrite::run(
        RewriteOptions::from(&args),
        SourceOptions::new(ctx.allow_remote_scan()),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::{CoalesceChannels, CompressionFormat};

    fn merge_command(files: Vec<&str>) -> MergeCommand {
        MergeCommand {
            files: files.into_iter().map(Into::into).collect(),
            output: None,
            output_file: None,
            compression: CompressionFormat::Zstd,
            chunk_size: mcap::WriteOptions::DEFAULT_CHUNK_SIZE,
            no_crc: false,
            no_chunks: false,
            allow_duplicate_metadata: false,
            coalesce_channels: CoalesceChannels::Auto,
        }
    }

    #[test]
    fn merge_options_set_the_merge_only_knobs() {
        let options = RewriteOptions::from(&MergeCommand {
            compression: CompressionFormat::Lz4,
            chunk_size: 4096,
            no_crc: true,
            no_chunks: true,
            allow_duplicate_metadata: true,
            coalesce_channels: CoalesceChannels::Force,
            ..merge_command(vec!["a.mcap", "b.mcap"])
        });

        assert_eq!(
            options.files,
            vec![
                std::path::PathBuf::from("a.mcap"),
                std::path::PathBuf::from("b.mcap")
            ]
        );
        assert!(matches!(options.compression, Some(mcap::Compression::Lz4)));
        assert_eq!(options.chunk_size, 4096);
        assert!(!options.include_crc);
        assert!(!options.use_chunks);
        assert!(options.allow_duplicate_metadata);
        assert_eq!(options.coalesce_channels, CoalesceChannels::Force);
        // Merge always sorts by log time and deduplicates metadata.
        assert_eq!(options.order, crate::cli::MessageOrder::LogTime);
        assert!(options.dedup_metadata);
    }

    #[test]
    fn merge_resolves_output_preferring_output_over_output_file() {
        // `--output` wins when both are supplied.
        let both = RewriteOptions::from(&MergeCommand {
            output: Some("out.mcap".into()),
            output_file: Some("legacy.mcap".into()),
            ..merge_command(vec!["a.mcap"])
        });
        assert_eq!(both.output, Some("out.mcap".into()));

        // The deprecated `--output-file` supplies the path when `--output` is absent.
        let fallback = RewriteOptions::from(&MergeCommand {
            output_file: Some("legacy.mcap".into()),
            ..merge_command(vec!["a.mcap"])
        });
        assert_eq!(fallback.output, Some("legacy.mcap".into()));
    }
}
