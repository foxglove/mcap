//! Caller-facing [`RewriteOptions`], their resolution into the validated [`ResolvedOptions`] the
//! engine consumes, and the topic-selection predicate shared by the read paths.
use std::path::PathBuf;

use anyhow::{bail, Context, Result};
use regex::Regex;

use crate::cli::{parse_output_compression, parse_timestamp_or_nanos, FilterCommand, MessageOrder};

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
    /// Sort output messages by log time; when false the input's stored order is preserved.
    pub(crate) order_by_log_time: bool,
}

impl From<&FilterCommand> for RewriteOptions {
    fn from(args: &FilterCommand) -> Self {
        Self {
            file: args.file.clone(),
            output: args.output.clone(),
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
            output_compression: args.output_compression.clone(),
            chunk_size: args.chunk_size,
            use_chunks: true,
            order_by_log_time: matches!(args.order, MessageOrder::LogTime),
        }
    }
}

impl RewriteOptions {
    pub(crate) fn new(file: Option<PathBuf>, output: Option<PathBuf>, chunk_size: u64) -> Self {
        Self {
            file,
            output,
            include_topic_regex: Vec::new(),
            exclude_topic_regex: Vec::new(),
            last_per_channel_topic_regex: Vec::new(),
            start: None,
            start_secs: 0,
            start_nsecs: 0,
            end: None,
            end_secs: 0,
            end_nsecs: 0,
            include_metadata: false,
            include_attachments: false,
            output_compression: "zstd".to_string(),
            chunk_size,
            use_chunks: true,
            order_by_log_time: false,
        }
    }

    pub(crate) fn compression(mut self, value: impl Into<String>) -> Self {
        self.output_compression = value.into();
        self
    }

    pub(crate) fn use_chunks(mut self, value: bool) -> Self {
        self.use_chunks = value;
        self
    }

    pub(crate) fn include_metadata(mut self, value: bool) -> Self {
        self.include_metadata = value;
        self
    }

    pub(crate) fn include_attachments(mut self, value: bool) -> Self {
        self.include_attachments = value;
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
    /// Sort output messages by log time; when false the input's stored order is preserved.
    pub(crate) order_by_log_time: bool,
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
        order_by_log_time: args.order_by_log_time,
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
    use crate::cli::{FilterCommand, MessageOrder};

    fn default_filter_command() -> FilterCommand {
        FilterCommand {
            file: None,
            output: None,
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
            output_compression: "zstd".to_string(),
            chunk_size: mcap::WriteOptions::DEFAULT_CHUNK_SIZE,
            order: MessageOrder::Preserve,
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
            order_by_log_time: false,
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
    fn order_maps_to_order_by_log_time() {
        let mut args = default_filter_command();
        // preserve (the default) does not sort.
        assert!(!RewriteOptions::from(&args).order_by_log_time);
        args.order = MessageOrder::LogTime;
        assert!(RewriteOptions::from(&args).order_by_log_time);
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
}
