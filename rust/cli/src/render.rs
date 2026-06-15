use std::fmt::Write as _;

pub fn decimal_time(t: u64) -> String {
    format!("{}.{:09}", t / 1_000_000_000, t % 1_000_000_000)
}

pub fn raw_time(t: u64) -> String {
    t.to_string()
}

pub fn write_raw_time(writer: &mut impl std::io::Write, t: u64) -> std::io::Result<()> {
    write!(writer, "{t}")
}

// Nanoseconds for 2000-01-01T00:00:00Z. Timestamps below this almost certainly are not real
// wall-clock times (relative/monotonic recordings start at or near 0), so rendering them as an
// absolute date would be misleading.
const WALL_CLOCK_CUTOFF_NANOS: u64 = 946_684_800_000_000_000;

pub fn formatted_time(t: u64) -> String {
    // Below the cutoff, render the raw decimal seconds (relative/monotonic recordings).
    // After, render an RFC3339 wall-clock string followed by the decimal seconds in parentheses.
    if t < WALL_CLOCK_CUTOFF_NANOS {
        return decimal_time(t);
    }
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
    let prefixes = ["B", "kB", "MB", "GB", "TB", "PB"];
    for (index, prefix) in prefixes.iter().enumerate() {
        let displayed = num_bytes as f64 / 1000f64.powi(index as i32);
        if displayed < 1000.0 {
            return format!("{displayed:.2} {prefix}");
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
    use super::{
        decimal_time, format_table, formatted_time, human_bytes, print_table, write_raw_time,
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
    fn formatted_time_is_decimal_below_cutoff_and_rfc3339_above() {
        // Below the 2000-01-01 cutoff: decimal seconds only (no misleading 1970 date).
        assert_eq!(formatted_time(1_000_000_000), "1.000000000");
        assert_eq!(decimal_time(1_234_567_890), "1.234567890");
        assert_eq!(formatted_time(1_234_567_890), "1.234567890");
        // At/after the cutoff: an RFC3339 wall-clock string with the decimal seconds in parens.
        assert_eq!(
            formatted_time(1_585_866_235_112_411_371),
            "2020-04-02T22:23:55.112411371Z (1585866235.112411371)"
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
        assert_eq!(human_bytes(999), "999.00 B");
        assert_eq!(human_bytes(1000), "1.00 kB");
        assert_eq!(human_bytes(2 * 1000), "2.00 kB");
        assert_eq!(human_bytes(2 * 1000 * 1000), "2.00 MB");
    }
}
