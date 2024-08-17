use std::time::Duration;

use crate::{cli::FilterArgs, error::CliResult};

pub async fn filter_mcap(input: FilterArgs) -> CliResult<()> {
    let start_time = input.start_secs
        .map(Duration::from_secs)
        .or_else(|| input.start_nsecs.map(Duration::from_nanos))
        .unwrap_or_default();

    let end_time = input.end_secs
        .map(Duration::from_secs)
        .or_else(|| input.end_nsecs.map(Duration::from_nanos));


    Ok(())
}
