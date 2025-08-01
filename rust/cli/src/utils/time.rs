use chrono::DateTime;

pub fn decimal_time(timestamp_ns: u64) -> String {
    let seconds = timestamp_ns / 1_000_000_000;
    let nanoseconds = timestamp_ns % 1_000_000_000;
    format!("{}.{:09}", seconds, nanoseconds)
}

pub fn format_duration(duration_ns: u64) -> String {
    let duration = std::time::Duration::from_nanos(duration_ns);
    format!("{:.3}s", duration.as_secs_f64())
}

pub fn parse_time(input: &str) -> anyhow::Result<u64> {
    // Try parsing as RFC3339 first
    if let Ok(dt) = DateTime::parse_from_rfc3339(input) {
        return Ok((dt.timestamp_nanos_opt().unwrap_or(0)) as u64);
    }

    // Try parsing as integer nanoseconds
    if let Ok(ns) = input.parse::<u64>() {
        return Ok(ns);
    }

    anyhow::bail!("Could not parse time: {}", input);
}
