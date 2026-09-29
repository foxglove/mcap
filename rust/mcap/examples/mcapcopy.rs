#[path = "common/logsetup.rs"]
mod logsetup;
#[path = "common/messages.rs"]
mod messages;

use std::{
    fs,
    io::{BufReader, BufWriter},
};

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

    let input = BufReader::new(fs::File::open(&args.mcap).context("Couldn't open MCAP file")?);

    let mut out = mcap::Writer::new(BufWriter::new(fs::File::create("out.mcap")?))?;

    // Stream records one at a time; memory scales with the largest record, not the file.
    for message in messages::MessageReader::new(input) {
        let message = message?;
        let ts = message.publish_time;
        info!(
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

        // We can easily take each Message and write it as a quick and dirty example,
        // but in real code, we'd be much better off adding each channel to the writer,
        // then calling `write_to_known_channel()`.
        // This avoids having to rehash the channel (and its schema) on each `write()`
        // to figure out what its ID is.
        out.write(&message)?;
    }
    Ok(())
}

fn main() {
    run().unwrap_or_else(|e| {
        error!("{e:?}");
        std::process::exit(1);
    });
}
