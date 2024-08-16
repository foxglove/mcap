use chrono::Duration;

/// Format bytes as a human readable string
///
/// I.e. 8192 bytes -> "8.00 KiB"
pub fn format_human_bytes(num_bytes: u64) -> String {
    let prefixes = ["B", "KiB", "MiB", "GiB"];

    for (index, p) in prefixes.iter().enumerate() {
        let displayed_value = (num_bytes as f64) / (1024_f64.powi(index as _));

        if displayed_value <= 1024. {
            return format!("{displayed_value:.2} {p}");
        }
    }

    let last_index = prefixes.len() - 1;
    let p = prefixes[last_index];
    let displayed_value = (num_bytes as f64) / (1024_f64.powi(last_index as _));

    format!("{displayed_value:.2} {p}")
}

fn format_nanos_component(nanoseconds: i32) -> String {
    if nanoseconds == 0 {
        return "0".to_string();
    }

    let nanos = format!("{nanoseconds:0>9}");
    nanos.trim_end_matches('0').to_string()
}

/// Format nanoseconds as a human readable frational seconds or nanoseconds string
pub fn format_human_nanos(nanoseconds: u64) -> String {
    let duration = Duration::nanoseconds(nanoseconds as i64);

    let seconds = duration.num_seconds();
    let nanos = duration.subsec_nanos();

    if seconds > 0 || nanos > 50_000 {
        let nanos = format_nanos_component(nanos);
        format!("{seconds}.{nanos}s")
    } else {
        format!("{nanos}ns")
    }
}

/// Format nanoseconds as a fractional seconds string
pub fn format_decimal_nanos(nanoseconds: u64) -> String {
    let duration = Duration::nanoseconds(nanoseconds as i64);

    let seconds = duration.num_seconds();
    let nanos = duration.subsec_nanos();

    format!("{seconds}.{nanos:0>9}")
}

#[cfg(test)]
mod tests {
    use super::*;

    macro_rules! test_decimal {
        ($a:expr => $b:expr) => {
            assert_eq!(format_decimal_nanos(($a * 1e9 as f64) as _), $b);
        };
    }

    macro_rules! test_human {
        ($a:expr => $b:expr) => {
            assert_eq!(format_human_nanos(($a * 1e9 as f64) as _), $b);
        };
    }

    #[test]
    fn test_decimal_format() {
        test_decimal!(14.5 =>       "14.500000000");
        test_decimal!(14.005 =>     "14.005000000");
        test_decimal!(0.000000001 => "0.000000001");
        test_decimal!(0.1 =>         "0.100000000");
        test_decimal!(0.001 =>       "0.001000000");
        test_decimal!(1000. =>    "1000.000000000");

        test_human!(14.5 => "14.5s");
        test_human!(14.005 => "14.005s");
        test_human!(0.000000001 => "1ns");
        test_human!(0.1 => "0.1s");
        test_human!(0.001 => "0.001s");
        test_human!(1000. => "1000.0s");
    }

    #[test]
    fn test_human_bytes() {
        assert_eq!(format_human_bytes(8), "8.00 B");
        assert_eq!(format_human_bytes(8 * 1024), "8.00 KiB");
        assert_eq!(format_human_bytes(8 * 1024 * 1024), "8.00 MiB");
        assert_eq!(format_human_bytes(8 * 1024 * 1024 * 1024), "8.00 GiB");
        assert_eq!(
            format_human_bytes(8 * 1024 * 1024 * 1024 * 1024),
            "8192.00 GiB"
        );
    }
}
