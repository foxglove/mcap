#[path = "common/logsetup.rs"]
mod logsetup;

use std::{fs, process};

use anyhow::{Context, Result};
use camino::{Utf8Path, Utf8PathBuf};
use clap::Parser;
use log::*;
use memmap2::Mmap;

#[derive(Parser, Debug)]
struct Args {
    /// Verbosity (-v, -vv, -vvv, etc.)
    #[clap(short, long, parse(from_occurrences))]
    verbose: u8,

    #[clap(short, long, arg_enum, default_value = "auto")]
    color: logsetup::Color,

    mcap: Utf8PathBuf,
}

fn map_mcap(p: &Utf8Path) -> Result<Mmap> {
    let fd = fs::File::open(p).context("Couldn't open MCAP file")?;
    unsafe { Mmap::map(&fd) }.context("Couldn't map MCAP file")
}

fn run() -> Result<()> {
    let args = Args::parse();
    logsetup::init_logger(args.verbose, args.color);

    let mapped = map_mcap(&args.mcap)?;

    for message in mcap::MessageStream::new(&mapped)? {
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

    info!("{:#?}", mcap::Summary::read(&mapped)?);
    Ok(())
}

fn main() {
    run().unwrap_or_else(|e| {
        error!("{:?}", e);
        process::exit(1);
    });
}
