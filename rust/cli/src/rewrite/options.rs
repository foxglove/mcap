//! Caller-facing [`RewriteOptions`], their resolution into the validated [`ResolvedOptions`] the
//! engine consumes, and the topic-selection predicate shared by the read paths.
use std::path::PathBuf;

use anyhow::{bail, Context, Result};
use regex::Regex;

use crate::cli::{
    parse_output_compression, parse_timestamp_or_nanos, CommonRewriteArgs, CompressionFormat,
    FilterCommand, MessageOrder, SortCommand,
};

#[derive(Debug, Clone)]
pub(crate) struct RewriteOptions {
    pub(crate) file: Option<PathBuf>,
    pub(crate) output: Option<PathBuf>,
    pub(crate) include_topic_regex: Vec<String>,
    pub(crate) exclude_topic_regex: Vec<String>,
    pub(crate) last_per_channel_topic_regex: Vec<String>,
    pub(crate) start: Option<String>,
    pub(crate) start_secs: u64,
    pub(crate) start_nsecs: u64,
    pub(crate) end: Option<String>,
    pub(crate) end_secs: u64,
    pub(crate) end_nsecs: u64,
    pub(crate) include_metadata: bool,
    pub(crate) include_attachments: bool,
    pub(crate) output_compression: String,
    pub(crate) chunk_size: u64,
    pub(crate) use_chunks: bool,
    pub(crate) include_crc: bool,
    pub(crate) order: MessageOrder,
}

/// Maps the arguments shared by every rewrite command onto their engine options: paths, chunk
/// size, `--no-crc`, and message order. Metadata and attachments are included by default (all
/// rewrite commands keep them; `filter` opts out via `--exclude-*`), and output is chunked with
/// zstd compression unless a command overrides those.
impl From<&CommonRewriteArgs> for RewriteOptions {
    fn from(args: &CommonRewriteArgs) -> Self {
        Self {
            file: args.file.clone(),
            output: args.output.clone(),
            include_topic_regex: Vec::new(),
            exclude_topic_regex: Vec::new(),
            last_per_channel_topic_regex: Vec::new(),
            start: None,
            start_secs: 0,
            start_nsecs: 0,
            end: None,
            end_secs: 0,
            end_nsecs: 0,
            include_metadata: true,
            include_attachments: true,
            output_compression: "zstd".to_string(),
            chunk_size: args.chunk_size,
            use_chunks: true,
            include_crc: !args.no_crc,
            order: args.order,
        }
    }
}

impl From<&FilterCommand> for RewriteOptions {
    fn from(args: &FilterCommand) -> Self {
        Self {
            include_topic_regex: args.include_topic_regex.clone(),
            exclude_topic_regex: args.exclude_topic_regex.clone(),
            last_per_channel_topic_regex: args.last_per_channel_topic_regex.clone(),
            start: args.start.clone(),
            start_secs: args.start_secs,
            start_nsecs: args.start_nsecs,
            end: args.end.clone(),
            end_secs: args.end_secs,
            end_nsecs: args.end_nsecs,
            include_metadata: !args.exclude_metadata,
            include_attachments: !args.exclude_attachments,
            output_compression: args
                .compression
                .or(args.output_compression)
                .unwrap_or(CompressionFormat::Zstd)
                .as_str()
                .to_string(),
            use_chunks: !args.no_chunks,
            ..RewriteOptions::from(&args.common)
        }
    }
}

/// `sort` is a `filter` preset that defaults `--order` to `log_time` instead of `preserve`,
/// keeping metadata and attachments. It uses its own flag surface (`<FILE>` + `-o/--output`, plus
/// chunking, compression, CRC, and order knobs) and does not expose topic/time selection. The
/// deprecated `--output-file` alias is honored when `--output` is absent (clap requires one).
impl From<&SortCommand> for RewriteOptions {
    fn from(args: &SortCommand) -> Self {
        Self {
            file: Some(args.file.clone()),
            output: args.output.clone().or_else(|| args.output_file.clone()),
            include_topic_regex: Vec::new(),
            exclude_topic_regex: Vec::new(),
            last_per_channel_topic_regex: Vec::new(),
            start: None,
            start_secs: 0,
            start_nsecs: 0,
            end: None,
            end_secs: 0,
            end_nsecs: 0,
            include_metadata: true,
            include_attachments: true,
            output_compression: args.compression.as_str().to_string(),
            chunk_size: args.chunk_size,
            use_chunks: !args.no_chunks,
            include_crc: !args.no_crc,
            order: args.order,
        }
    }
}

impl RewriteOptions {
    pub(crate) fn compression(mut self, value: impl Into<String>) -> Self {
        self.output_compression = value.into();
        self
    }
}

/// Validated, engine-ready form of [`RewriteOptions`]: regexes compiled, timestamps parsed, and the
/// output compression resolved. Produced by [`resolve_options`].
#[derive(Debug, Clone)]
pub(crate) struct ResolvedOptions {
    pub(crate) output: Option<PathBuf>,
    pub(crate) include_topics: Vec<Regex>,
    pub(crate) exclude_topics: Vec<Regex>,
    pub(crate) last_per_channel_topics: Vec<Regex>,
    pub(crate) start: u64,
    pub(crate) end: u64,
    pub(crate) include_metadata: bool,
    pub(crate) include_attachments: bool,
    pub(crate) compression: Option<mcap::Compression>,
    pub(crate) chunk_size: u64,
    pub(crate) use_chunks: bool,
    pub(crate) include_crc: bool,
    pub(crate) order: MessageOrder,
}

pub(crate) fn resolve_options(args: &RewriteOptions) -> Result<ResolvedOptions> {
    let start = parse_timestamp_args(args.start.as_deref(), args.start_nsecs, args.start_secs)
        .context("invalid start")?;
    let mut end = parse_timestamp_args(args.end.as_deref(), args.end_nsecs, args.end_secs)
        .context("invalid end")?;
    if end == 0 {
        end = u64::MAX;
    }
    if end < start {
        bail!("invalid time range query, end-time is before start-time");
    }

    if !args.include_topic_regex.is_empty() && !args.exclude_topic_regex.is_empty() {
        bail!("can only use one of --include-topic-regex and --exclude-topic-regex");
    }

    Ok(ResolvedOptions {
        output: args.output.clone(),
        include_topics: compile_matchers(&args.include_topic_regex)
            .context("invalid included topic regex")?,
        exclude_topics: compile_matchers(&args.exclude_topic_regex)
            .context("invalid excluded topic regex")?,
        last_per_channel_topics: compile_matchers(&args.last_per_channel_topic_regex)
            .context("invalid last-per-channel topic regex")?,
        start,
        end,
        include_metadata: args.include_metadata,
        include_attachments: args.include_attachments,
        compression: parse_output_compression(&args.output_compression)?,
        chunk_size: args.chunk_size,
        use_chunks: args.use_chunks,
        include_crc: args.include_crc,
        order: args.order,
    })
}

fn parse_timestamp_args(
    date_or_nanos: Option<&str>,
    nanoseconds: u64,
    seconds: u64,
) -> Result<u64> {
    // Preserve timestamp precedence:
    // --start/--end (string RFC3339 or nanos) > --*-nsecs > --*-secs.
    // --*-secs and --*-nsecs are mutually exclusive via clap's conflicts_with.
    // If both somehow arrive, this precedence order still applies as a fallback.
    if let Some(value) = date_or_nanos {
        return parse_timestamp_or_nanos(value);
    }
    if nanoseconds != 0 {
        return Ok(nanoseconds);
    }
    seconds
        .checked_mul(1_000_000_000)
        .context("seconds timestamp overflows nanoseconds")
}

fn compile_matchers(regex_strings: &[String]) -> Result<Vec<Regex>> {
    regex_strings
        .iter()
        .map(|pattern| {
            // Always wrap in a non-capturing group so alternation behaves as users expect.
            // This also fixes partially-anchored patterns like "^foo|bar$":
            // "^(?:^foo|bar$)$" preserves full-string matching for each branch.
            let anchored = format!("^(?:{pattern})$");
            Regex::new(&anchored).with_context(|| format!("{anchored} is not a valid regex"))
        })
        .collect()
}

pub(crate) fn include_topic(topic: &str, opts: &ResolvedOptions) -> bool {
    if !opts.include_topics.is_empty() {
        return opts
            .include_topics
            .iter()
            .any(|regex| regex.is_match(topic));
    }
    if !opts.exclude_topics.is_empty() {
        return !opts
            .exclude_topics
            .iter()
            .any(|regex| regex.is_match(topic));
    }
    true
}

#[cfg(test)]
fn build_filter_options(args: &FilterCommand) -> Result<ResolvedOptions> {
    resolve_options(&RewriteOptions::from(args))
}

#[cfg(test)]
mod tests {
    use regex::Regex;

    use super::{build_filter_options, include_topic, ResolvedOptions, RewriteOptions};
    use crate::cli::{
        CommonRewriteArgs, CompressionFormat, FilterCommand, MessageOrder, SortCommand,
    };

    fn default_filter_command() -> FilterCommand {
        FilterCommand {
            common: CommonRewriteArgs {
                file: None,
                output: None,
                chunk_size: mcap::WriteOptions::DEFAULT_CHUNK_SIZE,
                no_crc: false,
                order: MessageOrder::Preserve,
            },
            include_topic_regex: Vec::new(),
            exclude_topic_regex: Vec::new(),
            last_per_channel_topic_regex: Vec::new(),
            start: None,
            start_secs: 0,
            start_nsecs: 0,
            end: None,
            end_secs: 0,
            end_nsecs: 0,
            exclude_metadata: false,
            exclude_attachments: false,
            include_metadata: false,
            include_attachments: false,
            compression: None,
            output_compression: None,
            no_chunks: false,
        }
    }

    #[test]
    fn build_filter_options_rejects_include_exclude_conflict() {
        let mut args = default_filter_command();
        args.include_topic_regex.push("camera.*".to_string());
        args.exclude_topic_regex.push("radar.*".to_string());
        let err = build_filter_options(&args).expect_err("should fail");
        assert!(err
            .to_string()
            .contains("can only use one of --include-topic-regex and --exclude-topic-regex"));
    }

    #[test]
    fn build_filter_options_parses_timestamps_with_precedence() {
        let mut args = default_filter_command();
        args.start = Some("10".to_string());
        args.start_nsecs = 50;
        args.start_secs = 2;
        args.end_nsecs = 200;
        args.end_secs = 1;
        let opts = build_filter_options(&args).expect("options");
        assert_eq!(opts.start, 10);
        assert_eq!(opts.end, 200);
    }

    #[test]
    fn include_topic_honors_include_then_exclude() {
        let opts = ResolvedOptions {
            output: None,
            include_topics: vec![Regex::new("^camera_.*$").expect("regex")],
            exclude_topics: vec![Regex::new("^camera_a$").expect("regex")],
            last_per_channel_topics: Vec::new(),
            start: 0,
            end: u64::MAX,
            include_metadata: false,
            include_attachments: false,
            compression: Some(mcap::Compression::Zstd),
            chunk_size: mcap::WriteOptions::DEFAULT_CHUNK_SIZE,
            use_chunks: true,
            include_crc: true,
            order: MessageOrder::Preserve,
        };
        assert!(include_topic("camera_a", &opts));
        assert!(!include_topic("radar_a", &opts));
    }

    #[test]
    fn compile_matchers_wraps_alternation_with_grouping() {
        let matcher = super::compile_matchers(&["camera_a|camera_b".to_string()])
            .expect("regex")
            .pop()
            .expect("matcher");
        assert!(matcher.is_match("camera_a"));
        assert!(matcher.is_match("camera_b"));
        assert!(!matcher.is_match("camera_a_extra"));
        assert!(!matcher.is_match("extra_camera_b"));
    }

    #[test]
    fn compile_matchers_rewraps_partially_anchored_alternation() {
        let matcher = super::compile_matchers(&["^camera_a|camera_b$".to_string()])
            .expect("regex")
            .pop()
            .expect("matcher");
        assert!(matcher.is_match("camera_a"));
        assert!(matcher.is_match("camera_b"));
        assert!(!matcher.is_match("camera_a_extra"));
        assert!(!matcher.is_match("extra_camera_b"));
    }

    #[test]
    fn includes_metadata_and_attachments_by_default() {
        let opts = build_filter_options(&default_filter_command()).expect("options");
        assert!(opts.include_metadata);
        assert!(opts.include_attachments);
    }

    #[test]
    fn exclude_flags_drop_metadata_and_attachments() {
        let mut args = default_filter_command();
        args.exclude_metadata = true;
        args.exclude_attachments = true;
        let opts = build_filter_options(&args).expect("options");
        assert!(!opts.include_metadata);
        assert!(!opts.include_attachments);
    }

    #[test]
    fn defaults_enable_crc_and_chunks() {
        let opts = build_filter_options(&default_filter_command()).expect("options");
        assert!(opts.include_crc, "CRC should be on by default");
        assert!(opts.use_chunks, "chunking should be on by default");
    }

    #[test]
    fn no_crc_and_no_chunks_flags_map_to_engine_options() {
        let mut args = default_filter_command();
        args.common.no_crc = true;
        args.no_chunks = true;
        let opts = build_filter_options(&args).expect("options");
        assert!(!opts.include_crc, "--no-crc should disable CRC fields");
        assert!(
            !opts.use_chunks,
            "--no-chunks should write records outside of chunks"
        );
    }

    #[test]
    fn common_args_map_paths_order_and_defaults() {
        // `compress`/`decompress` build their options from the shared args; this locks in that
        // mapping (order + `--no-crc`) and the engine defaults (chunked, metadata/attachments kept).
        let common = CommonRewriteArgs {
            file: Some("in.mcap".into()),
            output: Some("out.mcap".into()),
            chunk_size: 4096,
            no_crc: true,
            order: MessageOrder::LogTime,
        };
        let opts = RewriteOptions::from(&common);
        assert_eq!(opts.file, Some("in.mcap".into()));
        assert_eq!(opts.output, Some("out.mcap".into()));
        assert_eq!(opts.chunk_size, 4096);
        assert_eq!(opts.order, MessageOrder::LogTime);
        assert!(!opts.include_crc, "--no-crc should disable CRC fields");
        assert!(opts.use_chunks, "output should be chunked by default");
        assert!(opts.include_metadata, "metadata should be kept by default");
        assert!(
            opts.include_attachments,
            "attachments should be kept by default"
        );
    }

    #[test]
    fn deprecated_include_flags_do_not_re_enable_excluded_records() {
        // The deprecated --include-* flags are no-ops; --exclude-* still wins.
        let mut args = default_filter_command();
        args.include_metadata = true;
        args.include_attachments = true;
        args.exclude_metadata = true;
        args.exclude_attachments = true;
        let opts = build_filter_options(&args).expect("options");
        assert!(!opts.include_metadata);
        assert!(!opts.include_attachments);
    }

    #[test]
    fn compression_defaults_to_zstd_when_unset() {
        let opts = build_filter_options(&default_filter_command()).expect("options");
        assert!(matches!(opts.compression, Some(mcap::Compression::Zstd)));
    }

    #[test]
    fn compression_flag_resolves_each_format() {
        // Guards the CompressionFormat -> str -> mcap::Compression bridge for every variant.
        let mut args = default_filter_command();

        args.compression = Some(CompressionFormat::Zstd);
        let opts = build_filter_options(&args).expect("options");
        assert!(matches!(opts.compression, Some(mcap::Compression::Zstd)));

        args.compression = Some(CompressionFormat::Lz4);
        let opts = build_filter_options(&args).expect("options");
        assert!(matches!(opts.compression, Some(mcap::Compression::Lz4)));

        args.compression = Some(CompressionFormat::None);
        let opts = build_filter_options(&args).expect("options");
        assert!(opts.compression.is_none());
    }

    #[test]
    fn deprecated_output_compression_applies_when_compression_unset() {
        let mut args = default_filter_command();
        args.compression = None;
        args.output_compression = Some(CompressionFormat::None);
        let opts = build_filter_options(&args).expect("options");
        assert!(opts.compression.is_none());
    }

    #[test]
    fn sort_command_maps_flags_onto_the_engine_preset() {
        // `sort` keeps metadata/attachments and translates its own flags (order, compression,
        // chunking, CRC) onto the engine options.
        let args = SortCommand {
            file: "in.mcap".into(),
            output: Some("out.mcap".into()),
            output_file: None,
            compression: CompressionFormat::Lz4,
            chunk_size: 4096,
            no_crc: true,
            no_chunks: true,
            order: MessageOrder::Preserve,
        };
        let opts = RewriteOptions::from(&args);
        assert_eq!(opts.file, Some("in.mcap".into()));
        assert_eq!(opts.output, Some("out.mcap".into()));
        assert_eq!(
            opts.order,
            MessageOrder::Preserve,
            "sort honors an explicit --order override"
        );
        assert_eq!(opts.output_compression, "lz4");
        assert_eq!(opts.chunk_size, 4096);
        assert!(!opts.use_chunks, "--no-chunks should write outside chunks");
        assert!(!opts.include_crc, "--no-crc should disable CRC fields");
        assert!(opts.include_metadata, "metadata is kept by default");
        assert!(opts.include_attachments, "attachments are kept by default");
    }

    #[test]
    fn sort_command_honors_deprecated_output_file_alias() {
        // When only the deprecated `--output-file` is set, it supplies the output path.
        let args = SortCommand {
            file: "in.mcap".into(),
            output: None,
            output_file: Some("out.mcap".into()),
            compression: CompressionFormat::Zstd,
            chunk_size: mcap::WriteOptions::DEFAULT_CHUNK_SIZE,
            no_crc: false,
            no_chunks: false,
            order: MessageOrder::LogTime,
        };
        assert_eq!(RewriteOptions::from(&args).output, Some("out.mcap".into()));
    }

    #[test]
    fn compression_takes_precedence_over_deprecated_output_compression() {
        let mut args = default_filter_command();
        args.compression = Some(CompressionFormat::Lz4);
        args.output_compression = Some(CompressionFormat::None);
        let opts = build_filter_options(&args).expect("options");
        assert!(matches!(opts.compression, Some(mcap::Compression::Lz4)));
    }
}
