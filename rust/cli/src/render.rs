use std::fmt::Write as _;

use anyhow::{bail, Context, Result};

pub fn decimal_time(t: u64) -> String {
    format!("{}.{:09}", t / 1_000_000_000, t % 1_000_000_000)
}

pub fn raw_time(t: u64) -> String {
    t.to_string()
}

pub fn write_raw_time(writer: &mut impl std::io::Write, t: u64) -> std::io::Result<()> {
    write!(writer, "{t}")
}

pub fn formatted_time(t: u64) -> String {
    let seconds = (t / 1_000_000_000) as i64;
    let nanos = (t % 1_000_000_000) as u32;
    match chrono::DateTime::from_timestamp(seconds, nanos) {
        Some(dt) => format!("{} ({})", format_rfc3339_trimmed(dt), decimal_time(t)),
        None => decimal_time(t),
    }
}

fn format_rfc3339_trimmed(dt: chrono::DateTime<chrono::Utc>) -> String {
    let rendered = dt.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true);
    let Some(without_z) = rendered.strip_suffix('Z') else {
        return rendered;
    };

    let Some((prefix, fractional)) = without_z.split_once('.') else {
        return rendered;
    };

    let trimmed = fractional.trim_end_matches('0');
    if trimmed.is_empty() {
        format!("{prefix}Z")
    } else {
        format!("{prefix}.{trimmed}Z")
    }
}

pub fn human_bytes(num_bytes: u64) -> String {
    let prefixes = ["B", "KiB", "MiB", "GiB"];
    for (index, prefix) in prefixes.iter().enumerate() {
        let displayed = num_bytes as f64 / 1024f64.powi(index as i32);
        if displayed <= 1024.0 {
            return format!("{displayed:.2} {prefix}");
        }
    }

    let last = prefixes.len() - 1;
    let displayed = num_bytes as f64 / 1024f64.powi(last as i32);
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

pub fn parse_output_compression(value: &str) -> Result<Option<mcap::Compression>> {
    match value {
        "zstd" => Ok(Some(mcap::Compression::Zstd)),
        "lz4" => Ok(Some(mcap::Compression::Lz4)),
        "none" | "" => Ok(None),
        _ => bail!(
            "unrecognized compression format '{value}': valid options are 'lz4', 'zstd', or 'none'"
        ),
    }
}

/// Parse a CLI-supplied timestamp as either integer nanoseconds or an RFC3339 string.
///
/// Shared by commands that accept timestamps on the command line (for example
/// `add attachment` and `filter`).
pub(crate) fn parse_timestamp_or_nanos(value: &str) -> Result<u64> {
    if let Ok(nanos) = value.parse::<u64>() {
        return Ok(nanos);
    }

    let parsed = chrono::DateTime::parse_from_rfc3339(value)
        .with_context(|| format!("failed to parse timestamp '{value}'"))?;
    let seconds = parsed.timestamp();
    anyhow::ensure!(seconds >= 0, "timestamp is before unix epoch: '{value}'");
    let seconds = seconds as u64;
    let nanos = parsed.timestamp_subsec_nanos() as u64;
    seconds
        .checked_mul(1_000_000_000)
        .and_then(|v| v.checked_add(nanos))
        .with_context(|| format!("timestamp is out of range: '{value}'"))
}

#[cfg(test)]
mod tests {
    use super::{
        decimal_time, format_table, formatted_time, human_bytes, parse_timestamp_or_nanos,
        print_table, write_raw_time,
    };

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
    fn formatted_time_includes_rfc3339_and_decimal() {
        assert_eq!(
            formatted_time(1_000_000_000),
            "1970-01-01T00:00:01Z (1.000000000)"
        );
        assert_eq!(decimal_time(1_234_567_890), "1.234567890");
        assert_eq!(
            formatted_time(1_234_567_890),
            "1970-01-01T00:00:01.23456789Z (1.234567890)"
        );
    }

    #[test]
    fn raw_time_is_unformatted_nanoseconds() {
        assert_eq!(super::raw_time(1_234_567_890), "1234567890");
    }

    #[test]
    fn write_raw_time_writes_unformatted_nanoseconds() {
        let mut out = Vec::new();
        write_raw_time(&mut out, 1_234_567_890).expect("should write raw time");
        assert_eq!(
            String::from_utf8(out).expect("raw time output should be utf8"),
            "1234567890"
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
        assert_eq!(human_bytes(2 * 1024), "2.00 KiB");
    }

    #[test]
    fn parse_output_compression_supports_known_values() {
        assert!(matches!(
            super::parse_output_compression("zstd").expect("zstd should parse"),
            Some(mcap::Compression::Zstd)
        ));
        assert!(matches!(
            super::parse_output_compression("lz4").expect("lz4 should parse"),
            Some(mcap::Compression::Lz4)
        ));
        assert!(super::parse_output_compression("none")
            .expect("none should parse")
            .is_none());
        assert!(super::parse_output_compression("")
            .expect("empty should parse")
            .is_none());
    }

    #[test]
    fn parse_output_compression_rejects_unknown_values() {
        let err =
            super::parse_output_compression("snappy").expect_err("unknown compression should fail");
        assert!(err
            .to_string()
            .contains("unrecognized compression format 'snappy'"));
    }

    #[test]
    fn parses_nanos_or_rfc3339() {
        assert_eq!(parse_timestamp_or_nanos("123").expect("nanos"), 123);
        let ts = parse_timestamp_or_nanos("2023-07-25T15:27:30.132545471Z").expect("rfc3339");
        assert_eq!(ts, 1_690_298_850_132_545_471);
    }

    #[test]
    fn rejects_invalid_timestamp() {
        let err = parse_timestamp_or_nanos("not-a-time").expect_err("invalid time should fail");
        assert!(err.to_string().contains("failed to parse timestamp"));
    }
}
