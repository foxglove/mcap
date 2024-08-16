pub fn human_bytes(num_bytes: u64) -> String {
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
