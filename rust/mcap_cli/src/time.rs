use anyhow::{Context, Result};

/// Parses either integer nanoseconds or RFC3339 timestamp into nanoseconds.
#[allow(dead_code)]
pub fn parse_date_or_nanos(input: &str) -> Result<u64> {
    if let Ok(value) = input.parse::<u64>() {
        return Ok(value);
    }

    let datetime = chrono::DateTime::parse_from_rfc3339(input).with_context(|| {
        format!("failed to parse timestamp '{input}' as RFC3339 or nanoseconds")
    })?;
    let nanos = datetime
        .timestamp_nanos_opt()
        .context("timestamp out of range for nanosecond precision")?;
    u64::try_from(nanos).context("timestamp must not be negative")
}

#[cfg(test)]
mod tests {
    use super::parse_date_or_nanos;

    #[test]
    fn parses_nanoseconds() {
        assert_eq!(
            parse_date_or_nanos("123456789").expect("numeric nanos should parse"),
            123_456_789
        );
    }

    #[test]
    fn parses_rfc3339() {
        assert_eq!(
            parse_date_or_nanos("1970-01-01T00:00:01Z").expect("RFC3339 should parse"),
            1_000_000_000
        );
    }

    #[test]
    fn rejects_invalid_input() {
        parse_date_or_nanos("not-a-date").expect_err("invalid timestamp should fail");
    }
}
