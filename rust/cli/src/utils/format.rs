use std::io::Write;

/// Format a table with headers and rows for consistent output
pub fn format_table<W: Write>(
    writer: &mut W,
    headers: Vec<&str>,
    rows: Vec<Vec<String>>,
) -> std::io::Result<()> {
    if rows.is_empty() {
        return Ok(());
    }

    // Simple ASCII table formatting since tabled is complex with dynamic columns
    // Calculate column widths
    let num_cols = headers.len();
    let mut col_widths = vec![0; num_cols];

    // Check header widths
    for (i, header) in headers.iter().enumerate() {
        col_widths[i] = col_widths[i].max(header.len());
    }

    // Check row widths
    for row in &rows {
        for (i, cell) in row.iter().enumerate().take(num_cols) {
            col_widths[i] = col_widths[i].max(cell.len());
        }
    }

    // Print headers
    write!(writer, "| ")?;
    for (i, header) in headers.iter().enumerate() {
        write!(writer, "{:<width$} | ", header, width = col_widths[i])?;
    }
    writeln!(writer)?;

    // Print separator
    write!(writer, "| ")?;
    for width in &col_widths {
        write!(writer, "{:-<width$} | ", "", width = width)?;
    }
    writeln!(writer)?;

    // Print rows
    for row in &rows {
        write!(writer, "| ")?;
        for (i, cell) in row.iter().enumerate().take(num_cols) {
            write!(writer, "{:<width$} | ", cell, width = col_widths[i])?;
        }
        writeln!(writer)?;
    }

    Ok(())
}

/// Format bytes in human-readable format (B, KiB, MiB, GiB)
pub fn format_bytes(bytes: u64) -> String {
    const UNITS: &[&str] = &["B", "KiB", "MiB", "GiB", "TiB"];
    const THRESHOLD: f64 = 1024.0;

    if bytes == 0 {
        return "0 B".to_string();
    }

    let bytes = bytes as f64;
    let unit_index = (bytes.log2() / THRESHOLD.log2()).floor() as usize;
    let unit_index = unit_index.min(UNITS.len() - 1);

    let value = bytes / THRESHOLD.powi(unit_index as i32);

    if unit_index == 0 {
        format!("{:.0} {}", value, UNITS[unit_index])
    } else {
        format!("{:.2} {}", value, UNITS[unit_index])
    }
}

/// Format duration in human-readable format
pub fn format_duration(nanos: u64) -> String {
    if nanos == 0 {
        return "0s".to_string();
    }

    let seconds = nanos as f64 / 1_000_000_000.0;

    if seconds < 60.0 {
        format!("{:.3}s", seconds)
    } else if seconds < 3600.0 {
        let minutes = seconds / 60.0;
        format!("{:.2}m", minutes)
    } else {
        let hours = seconds / 3600.0;
        format!("{:.2}h", hours)
    }
}

/// Format timestamp as decimal seconds
pub fn format_decimal_time(timestamp: u64) -> String {
    let seconds = timestamp / 1_000_000_000;
    let nanoseconds = timestamp % 1_000_000_000;
    format!("{}.{:09}", seconds, nanoseconds)
}

/// Format timestamp as human-readable date/time
pub fn format_human_time(timestamp: u64) -> String {
    use chrono::{TimeZone, Utc};

    let seconds = (timestamp / 1_000_000_000) as i64;
    let nanoseconds = (timestamp % 1_000_000_000) as u32;

    match Utc.timestamp_opt(seconds, nanoseconds) {
        chrono::LocalResult::Single(dt) => dt.format("%Y-%m-%d %H:%M:%S%.3f UTC").to_string(),
        _ => format_decimal_time(timestamp),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_bytes() {
        assert_eq!(format_bytes(0), "0 B");
        assert_eq!(format_bytes(512), "512 B");
        assert_eq!(format_bytes(1024), "1.00 KiB");
        assert_eq!(format_bytes(1536), "1.50 KiB");
        assert_eq!(format_bytes(1024 * 1024), "1.00 MiB");
        assert_eq!(format_bytes(1024 * 1024 * 1024), "1.00 GiB");
    }

    #[test]
    fn test_format_duration() {
        assert_eq!(format_duration(0), "0s");
        assert_eq!(format_duration(1_000_000_000), "1.000s");
        assert_eq!(format_duration(2_500_000_000), "2.500s");
        assert_eq!(format_duration(60_000_000_000), "1.00m");
        assert_eq!(format_duration(3600_000_000_000), "1.00h");
    }

    #[test]
    fn test_format_decimal_time() {
        assert_eq!(
            format_decimal_time(1234567890123456789),
            "1234567890.123456789"
        );
        assert_eq!(format_decimal_time(1000000000), "1.000000000");
    }
}
