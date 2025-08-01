/// Format bytes in human-readable format (KiB, MiB, GiB)
pub fn human_bytes(num_bytes: u64) -> String {
    let prefixes = ["B", "KiB", "MiB", "GiB"];

    for (index, &prefix) in prefixes.iter().enumerate() {
        let divisor = 1024_f64.powi(index as i32);
        let displayed_value = num_bytes as f64 / divisor;

        if displayed_value <= 1024.0 || index == prefixes.len() - 1 {
            return format!("{:.2} {}", displayed_value, prefix);
        }
    }

    // Fallback (should never reach here)
    let last_index = prefixes.len() - 1;
    let divisor = 1024_f64.powi(last_index as i32);
    let displayed_value = num_bytes as f64 / divisor;
    format!("{:.2} {}", displayed_value, prefixes[last_index])
}

/// Count the number of digits in a number
pub fn digits(num: u64) -> usize {
    if num == 0 {
        1
    } else {
        (num as f64).log10().floor() as usize + 1
    }
}

/// Format a duration in nanoseconds as a human-readable string
pub fn format_duration_ns(duration_ns: u64) -> String {
    let duration = std::time::Duration::from_nanos(duration_ns);
    format_duration(duration)
}

/// Format a duration as a human-readable string
pub fn format_duration(duration: std::time::Duration) -> String {
    let total_seconds = duration.as_secs();
    let nanos = duration.subsec_nanos();

    if total_seconds >= 3600 {
        let hours = total_seconds / 3600;
        let minutes = (total_seconds % 3600) / 60;
        let seconds = total_seconds % 60;
        format!("{}h{}m{}s", hours, minutes, seconds)
    } else if total_seconds >= 60 {
        let minutes = total_seconds / 60;
        let seconds = total_seconds % 60;
        format!("{}m{}s", minutes, seconds)
    } else if total_seconds > 0 {
        format!("{}.{:03}s", total_seconds, nanos / 1_000_000)
    } else if nanos >= 1_000_000 {
        format!("{:.3}ms", nanos as f64 / 1_000_000.0)
    } else if nanos >= 1_000 {
        format!("{:.3}µs", nanos as f64 / 1_000.0)
    } else {
        format!("{}ns", nanos)
    }
}

// Future: add serde traits when needed

#[derive(Debug, Clone)]
pub enum OutputFormat {
    Human,
    Json,
    Table,
}

#[derive(Debug, Clone)]
pub struct FormatOptions {
    pub format: OutputFormat,
    pub colored: bool,
    pub compact: bool,
}

impl Default for FormatOptions {
    fn default() -> Self {
        Self {
            format: OutputFormat::Human,
            colored: true,
            compact: false,
        }
    }
}
