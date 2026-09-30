#[path = "common/logsetup.rs"]
mod logsetup;
#[path = "common/summary.rs"]
mod summary;

use std::{fs, io::BufReader, process};

use anyhow::{Context, Result};
use camino::Utf8PathBuf;
use clap::Parser;
use log::*;

#[derive(Parser, Debug)]
struct Args {
    /// Verbosity (-v, -vv, -vvv, etc.)
    #[clap(short, long, action = clap::ArgAction::Count)]
    verbose: u8,

    #[clap(short, long, value_enum, default_value = "auto")]
    color: logsetup::Color,

    mcap: Utf8PathBuf,
}

fn run() -> Result<()> {
    let args = Args::parse();
    logsetup::init_logger(args.verbose, args.color);

    // Stream records one at a time; memory scales with the largest record, not the file.
    let mut file = BufReader::new(fs::File::open(&args.mcap).context("Couldn't open MCAP file")?);

    for message in mcap::io::MessageReader::new(&mut file) {
        let message = message?;
        let ts = message.publish_time;
        println!(
            "{} {} [{}] [{}]...",
            ts,
            message.channel.topic,
            message
                .channel
                .schema
                .as_ref()
                .map(|s| s.name.as_str())
                .unwrap_or_default(),
            message
                .data
                .iter()
                .take(10)
                .map(|b| b.to_string())
                .collect::<Vec<_>>()
                .join(" ")
        );
    }

    // The summary lives at the end of the file; read just that section.
    info!("{:#?}", summary::read_summary(&mut file)?);
    Ok(())
}

fn main() {
    run().unwrap_or_else(|e| {
        error!("{e:?}");
        process::exit(1);
    });
}
