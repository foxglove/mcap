use std::cell::Cell;
use std::fmt::Write as _;

use crate::cli::TimeFormat;

/// Nanoseconds for 2000-01-01T00:00:00Z. Timestamps below this almost certainly are not real
/// wall-clock times (relative/monotonic recordings start at or near 0), so rendering them as an
/// absolute date would be misleading under `--time-format=auto`.
const WALL_CLOCK_CUTOFF_NANOS: u64 = 946_684_800_000_000_000;

/// Policy-driven timestamp renderer used by CLI command output.
///
/// For [`TimeFormat::Auto`], the RFC3339-vs-decimal choice is latched once per renderer (i.e. once
/// per command invocation). Mixed pre/post-cutoff files — e.g. a GPS clock that acquires a wall
/// time mid-recording — must render uniformly; switching format mid-stream would be confusing.
/// Prefer priming from a known summary `message_start_time` when available so the choice does not
/// depend on read/stream order; otherwise the first rendered timestamp decides.
#[derive(Debug, Clone)]
pub struct TimeRenderer {
    format: TimeFormat,
    /// Latched `auto` sub-format: `Some(true)` → RFC3339, `Some(false)` → decimal seconds.
    /// `Cell` so callers can share `&TimeRenderer` while streaming.
    auto_use_rfc3339: Cell<Option<bool>>,
}

impl Default for TimeRenderer {
    fn default() -> Self {
        Self::new(TimeFormat::Auto)
    }
}

// Compare policy only; latch state is intentionally ignored so CatOptions can derive Eq.
impl PartialEq for TimeRenderer {
    fn eq(&self, other: &Self) -> bool {
        self.format == other.format
    }
}

impl Eq for TimeRenderer {}

impl TimeRenderer {
    pub fn new(format: TimeFormat) -> Self {
        Self {
            format,
            auto_use_rfc3339: Cell::new(None),
        }
    }

    /// Latch `auto` from a known start timestamp before any output is rendered.
    ///
    /// No-op for non-`auto` formats, and no-op if `auto` is already latched.
    pub fn prime(&self, t: u64) {
        if self.format != TimeFormat::Auto {
            return;
        }
        if self.auto_use_rfc3339.get().is_some() {
            return;
        }
        self.auto_use_rfc3339
            .set(Some(t >= WALL_CLOCK_CUTOFF_NANOS));
    }

    pub fn format(&self, t: u64) -> String {
        match self.resolved_kind(t) {
            ResolvedTimeKind::Nanoseconds => t.to_string(),
            ResolvedTimeKind::Seconds => format_decimal_seconds(t),
            ResolvedTimeKind::Rfc3339 => format_rfc3339(t),
        }
    }

    /// Machine-facing timestamp string, matching [`Self::write_json`]'s content but without the
    /// surrounding JSON quotes. Used by tabular machine output (`cat --format=csv`), where the CSV
    /// writer supplies its own quoting. Like `write_json`, `auto` always resolves to RFC3339 (no
    /// y2k cutoff, no latch) so the column has a single predictable shape.
    pub fn format_machine(&self, t: u64) -> String {
        match self.resolved_json_kind() {
            ResolvedTimeKind::Nanoseconds => t.to_string(),
            ResolvedTimeKind::Seconds => format_decimal_seconds(t),
            ResolvedTimeKind::Rfc3339 => format_rfc3339(t),
        }
    }

    /// Write the timestamp directly into `writer` for human-facing (text/table) output.
    ///
    /// The numeric variants format straight into the writer to avoid a per-timestamp heap
    /// allocation on the hot `cat` streaming path; only RFC3339 needs an intermediate `String`
    /// (chrono builds one internally).
    pub fn write(&self, writer: &mut impl std::io::Write, t: u64) -> std::io::Result<()> {
        self.write_kind(writer, t, self.resolved_kind(t))
    }

    /// Write the timestamp as a quoted JSON string directly into `writer`, for machine-facing
    /// output (`cat --format=ndjson`).
    ///
    /// Under `auto`, machine output always uses RFC3339 (see [`Self::resolved_json_kind`]): the
    /// field shape must be predictable for a downstream parser, so it does not flip at the y2k
    /// cutoff the way human-facing `auto` does. Explicit `--time-format` values are honored as-is.
    ///
    /// Every `TimeFormat` renders to JSON-safe ASCII (digits plus `.`, `-`, `:`, `T`, `Z`), so no
    /// escaping is required and we can skip the `serde_json::to_string` intermediate. Timestamps are
    /// always strings (never bare numbers) to avoid float / `>2^53` integer precision loss in JSON.
    pub fn write_json(&self, writer: &mut impl std::io::Write, t: u64) -> std::io::Result<()> {
        writer.write_all(b"\"")?;
        self.write_kind(writer, t, self.resolved_json_kind())?;
        writer.write_all(b"\"")
    }

    fn write_kind(
        &self,
        writer: &mut impl std::io::Write,
        t: u64,
        kind: ResolvedTimeKind,
    ) -> std::io::Result<()> {
        match kind {
            ResolvedTimeKind::Nanoseconds => write!(writer, "{t}"),
            ResolvedTimeKind::Seconds => {
                write!(writer, "{}.{:09}", t / 1_000_000_000, t % 1_000_000_000)
            }
            ResolvedTimeKind::Rfc3339 => writer.write_all(format_rfc3339(t).as_bytes()),
        }
    }

    /// Resolve the format for human-facing (text/table) output. Under `auto`, the RFC3339-vs-decimal
    /// choice is latched once (see the type docs) using the y2k cutoff.
    fn resolved_kind(&self, t: u64) -> ResolvedTimeKind {
        match self.format {
            TimeFormat::Nanoseconds => ResolvedTimeKind::Nanoseconds,
            TimeFormat::Seconds => ResolvedTimeKind::Seconds,
            TimeFormat::Rfc3339 => ResolvedTimeKind::Rfc3339,
            TimeFormat::Auto => {
                let use_rfc3339 = match self.auto_use_rfc3339.get() {
                    Some(latched) => latched,
                    None => {
                        let use_rfc3339 = t >= WALL_CLOCK_CUTOFF_NANOS;
                        self.auto_use_rfc3339.set(Some(use_rfc3339));
                        use_rfc3339
                    }
                };
                if use_rfc3339 {
                    ResolvedTimeKind::Rfc3339
                } else {
                    ResolvedTimeKind::Seconds
                }
            }
        }
    }

    /// Resolve the format for machine-facing (JSON) output. Unlike [`Self::resolved_kind`],
    /// `auto` always maps to RFC3339 (no y2k cutoff, no latch) so the emitted field has a single,
    /// predictable shape regardless of the data; explicit formats map through unchanged.
    fn resolved_json_kind(&self) -> ResolvedTimeKind {
        match self.format {
            TimeFormat::Nanoseconds => ResolvedTimeKind::Nanoseconds,
            TimeFormat::Seconds => ResolvedTimeKind::Seconds,
            TimeFormat::Rfc3339 | TimeFormat::Auto => ResolvedTimeKind::Rfc3339,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ResolvedTimeKind {
    Nanoseconds,
    Seconds,
    Rfc3339,
}

fn format_decimal_seconds(t: u64) -> String {
    format!("{}.{:09}", t / 1_000_000_000, t % 1_000_000_000)
}

fn format_rfc3339(t: u64) -> String {
    let seconds = (t / 1_000_000_000) as i64;
    let nanos = (t % 1_000_000_000) as u32;
    match chrono::DateTime::from_timestamp(seconds, nanos) {
        // Always emit 9 fractional digits: a fixed width keeps lexicographic order equal to
        // chronological order (trimming trailing zeros would sort a whole second after a
        // sub-second value in the same second, since '.' < 'Z') and keeps table columns uniform.
        Some(dt) => dt.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true),
        // Every `u64` nanosecond timestamp fits chrono's range; keep a defensive fallback.
        None => format_decimal_seconds(t),
    }
}

pub fn human_bytes(num_bytes: u64) -> String {
    let prefixes = ["B", "kB", "MB", "GB", "TB", "PB"];
    for (index, prefix) in prefixes.iter().enumerate() {
        let displayed = num_bytes as f64 / 1000f64.powi(index as i32);
        let rounded = (displayed * 100.0).round() / 100.0;
        if rounded < 1000.0 {
            return format!("{rounded:.2} {prefix}");
        }
    }

    let last = prefixes.len() - 1;
    let displayed = num_bytes as f64 / 1000f64.powi(last as i32);
    format!("{displayed:.2} {}", prefixes[last])
}

pub fn format_table(rows: &[Vec<String>]) -> String {
    if rows.is_empty() {
        return String::new();
    }

    let mut widths = vec![0usize; rows[0].len()];
    for row in rows {
        for (idx, value) in row.iter().enumerate() {
            widths[idx] = widths[idx].max(value.chars().count());
        }
    }

    let mut out = String::new();
    let last_col_idx = rows[0].len().saturating_sub(1);
    for row in rows {
        let mut line = String::new();
        for (idx, value) in row.iter().enumerate() {
            if idx > 0 {
                line.push('\t');
            }
            if idx == last_col_idx {
                line.push_str(value);
            } else {
                let width = widths[idx];
                let _ = write!(&mut line, "{value:<width$}");
            }
        }
        let _ = writeln!(&mut out, "{line}");
    }
    out
}

pub fn print_table(rows: &[Vec<String>]) {
    let rendered = format_table(rows);
    if rendered.is_empty() {
        return;
    }
    print!("{rendered}");
}

#[cfg(test)]
mod tests {
    use super::{format_table, human_bytes, print_table, TimeRenderer, WALL_CLOCK_CUTOFF_NANOS};
    use crate::cli::TimeFormat;

    const DEMO_NS: u64 = 1_490_149_580_103_843_113;
    const DEMO_RFC3339: &str = "2017-03-22T02:26:20.103843113Z";
    const DEMO_SECONDS: &str = "1490149580.103843113";
    const DEMO_NANOS: &str = "1490149580103843113";
    const PRE_CUTOFF_NS: u64 = 1_000_000_000;

    #[test]
    fn table_printer_handles_empty_input() {
        print_table(&[]);
        assert!(format_table(&[]).is_empty());
    }

    #[test]
    fn table_formatter_aligns_columns() {
        let rows = vec![
            vec!["id".to_string(), "topic".to_string()],
            vec!["7".to_string(), "/foo".to_string()],
            vec!["12".to_string(), "/barbaz".to_string()],
        ];
        let rendered = format_table(&rows);
        let lines: Vec<&str> = rendered.lines().collect();
        assert_eq!(lines.len(), 3);
        assert!(lines[0].starts_with("id"));
        assert!(lines[1].contains('\t'));
        assert!(lines[2].contains("/barbaz"));
    }

    #[test]
    fn nanoseconds_format_is_integer_nanos() {
        let times = TimeRenderer::new(TimeFormat::Nanoseconds);
        assert_eq!(times.format(DEMO_NS), DEMO_NANOS);
        assert_eq!(times.format(PRE_CUTOFF_NS), "1000000000");
    }

    #[test]
    fn seconds_format_is_decimal_with_nine_fractional_digits() {
        let times = TimeRenderer::new(TimeFormat::Seconds);
        assert_eq!(times.format(DEMO_NS), DEMO_SECONDS);
        assert_eq!(times.format(PRE_CUTOFF_NS), "1.000000000");
        assert_eq!(times.format(1_234_567_890), "1.234567890");
    }

    #[test]
    fn rfc3339_format_is_always_wall_clock_including_pre_cutoff() {
        let times = TimeRenderer::new(TimeFormat::Rfc3339);
        assert_eq!(times.format(DEMO_NS), DEMO_RFC3339);
        // Pre-cutoff values are still rendered as the true UTC instant (no decimal fallback).
        assert_eq!(
            times.format(PRE_CUTOFF_NS),
            "1970-01-01T00:00:01.000000000Z"
        );
        assert_eq!(
            times.format(WALL_CLOCK_CUTOFF_NANOS),
            "2000-01-01T00:00:00.000000000Z"
        );
    }

    #[test]
    fn auto_uses_rfc3339_above_cutoff_and_decimal_below_without_parenthetical() {
        let above = TimeRenderer::new(TimeFormat::Auto);
        assert_eq!(above.format(DEMO_NS), DEMO_RFC3339);
        assert!(!above.format(DEMO_NS).contains('('));

        let below = TimeRenderer::new(TimeFormat::Auto);
        assert_eq!(below.format(PRE_CUTOFF_NS), "1.000000000");
    }

    #[test]
    fn auto_latches_from_prime_so_mixed_timestamps_render_uniformly() {
        // Start after the cutoff → every timestamp (including pre-cutoff) is RFC3339.
        let post = TimeRenderer::new(TimeFormat::Auto);
        post.prime(DEMO_NS);
        assert_eq!(post.format(DEMO_NS), DEMO_RFC3339);
        assert_eq!(post.format(PRE_CUTOFF_NS), "1970-01-01T00:00:01.000000000Z");

        // Start before the cutoff → every timestamp (including post-cutoff) is decimal seconds.
        let pre = TimeRenderer::new(TimeFormat::Auto);
        pre.prime(PRE_CUTOFF_NS);
        assert_eq!(pre.format(PRE_CUTOFF_NS), "1.000000000");
        assert_eq!(pre.format(DEMO_NS), DEMO_SECONDS);
    }

    #[test]
    fn auto_latches_on_first_rendered_timestamp_when_not_primed() {
        let times = TimeRenderer::new(TimeFormat::Auto);
        assert_eq!(times.format(PRE_CUTOFF_NS), "1.000000000");
        // Later post-cutoff values stay in the latched decimal mode.
        assert_eq!(times.format(DEMO_NS), DEMO_SECONDS);
    }

    #[test]
    fn write_emits_the_same_bytes_as_format() {
        let times = TimeRenderer::new(TimeFormat::Nanoseconds);
        let mut out = Vec::new();
        times
            .write(&mut out, 1_234_567_890)
            .expect("should write time");
        assert_eq!(
            String::from_utf8(out).expect("time output should be utf8"),
            "1234567890"
        );
    }

    fn json_string(times: &TimeRenderer, t: u64) -> String {
        let mut out = Vec::new();
        times
            .write_json(&mut out, t)
            .expect("should write json time");
        String::from_utf8(out).expect("json time output should be utf8")
    }

    #[test]
    fn json_auto_is_always_rfc3339_even_below_cutoff() {
        // Machine output must have a predictable shape, so `auto` does not apply the y2k cutoff the
        // way the text path does: a pre-cutoff value is RFC3339 in JSON but decimal seconds in text.
        let times = TimeRenderer::new(TimeFormat::Auto);
        assert_eq!(
            json_string(&times, PRE_CUTOFF_NS),
            "\"1970-01-01T00:00:01.000000000Z\""
        );
        assert_eq!(json_string(&times, DEMO_NS), format!("\"{DEMO_RFC3339}\""));
        // The text path for the same renderer/value still honors the cutoff.
        assert_eq!(times.format(PRE_CUTOFF_NS), "1.000000000");
    }

    #[test]
    fn json_explicit_formats_are_quoted_and_honored() {
        assert_eq!(
            json_string(&TimeRenderer::new(TimeFormat::Seconds), DEMO_NS),
            format!("\"{DEMO_SECONDS}\"")
        );
        assert_eq!(
            json_string(&TimeRenderer::new(TimeFormat::Nanoseconds), DEMO_NS),
            format!("\"{DEMO_NANOS}\"")
        );
        assert_eq!(
            json_string(&TimeRenderer::new(TimeFormat::Rfc3339), PRE_CUTOFF_NS),
            "\"1970-01-01T00:00:01.000000000Z\""
        );
    }

    #[test]
    fn format_machine_is_write_json_content_without_quotes() {
        // The tabular (`cat --format=csv`) path uses this: same resolution as `write_json`
        // (`auto` is always RFC3339, no cutoff), but unquoted for the CSV writer.
        assert_eq!(
            TimeRenderer::new(TimeFormat::Auto).format_machine(PRE_CUTOFF_NS),
            "1970-01-01T00:00:01.000000000Z"
        );
        assert_eq!(
            TimeRenderer::new(TimeFormat::Seconds).format_machine(DEMO_NS),
            DEMO_SECONDS
        );
        assert_eq!(
            TimeRenderer::new(TimeFormat::Nanoseconds).format_machine(DEMO_NS),
            DEMO_NANOS
        );
    }

    #[test]
    fn table_formatter_omits_trailing_whitespace() {
        let rows = vec![
            vec!["col1".to_string(), "col2".to_string()],
            vec!["a".to_string(), "b".to_string()],
        ];
        let rendered = format_table(&rows);
        for line in rendered.lines() {
            assert!(!line.ends_with(' '));
            assert!(!line.ends_with('\t'));
        }
    }

    #[test]
    fn human_bytes_scales_units() {
        assert_eq!(human_bytes(2), "2.00 B");
        assert_eq!(human_bytes(999), "999.00 B");
        assert_eq!(human_bytes(1000), "1.00 kB");
        assert_eq!(human_bytes(2 * 1000), "2.00 kB");
        assert_eq!(human_bytes(2 * 1000 * 1000), "2.00 MB");
        assert_eq!(human_bytes(999_996), "1.00 MB");
    }
}
