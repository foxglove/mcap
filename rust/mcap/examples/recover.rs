#[path = "common/logsetup.rs"]
mod logsetup;
#[path = "common/messages.rs"]
mod messages;

use std::{
    fs,
    io::{BufReader, BufWriter},
};

use anyhow::{ensure, Context, Result};
use camino::Utf8PathBuf;
use clap::Parser;
use log::*;
use mcap::sans_io::LinearReaderOptions;

#[derive(Parser, Debug)]
struct Args {
    /// Verbosity (-v, -vv, -vvv, etc.)
    #[clap(short, long, action = clap::ArgAction::Count)]
    verbose: u8,

    #[clap(short, long, value_enum, default_value = "auto")]
    color: logsetup::Color,

    #[clap(help = "input MCAP file")]
    input: Utf8PathBuf,

    #[clap(
        short,
        long,
        help = "output MCAP file, defaults to <input-file>.recovered.mcap"
    )]
    output: Option<Utf8PathBuf>,
}

fn make_output_path(input: Utf8PathBuf) -> Result<Utf8PathBuf> {
    use std::str::FromStr;
    let file_stem = input.file_stem().context("no file stem for input path")?;
    let output_path = Utf8PathBuf::from_str(file_stem)?.with_extension("recovered.mcap");
    Ok(output_path)
}

fn run() -> Result<()> {
    let args = Args::parse();
    logsetup::init_logger(args.verbose, args.color);
    debug!("{args:?}");

    let input = BufReader::new(fs::File::open(&args.input).context("Couldn't open MCAP file")?);
    let output_path = args.output.unwrap_or(make_output_path(args.input)?);
    ensure!(
        !output_path.exists(),
        "output path {output_path} already exists"
    );

    let mut out = mcap::Writer::new(BufWriter::new(fs::File::create(output_path)?))?;

    info!("recovering as many messages as possible...");
    let mut recovered_count = 0;
    // A truncated file has no trailing magic, so do not require one; the reader stops with an
    // error at the first record it cannot complete, and everything before it is kept.
    for maybe_message in messages::MessageReader::with_options(
        input,
        LinearReaderOptions::default().with_skip_end_magic(true),
    ) {
        match maybe_message {
            Ok(message) => {
                out.write(&message)?;
                recovered_count += 1;
            }
            Err(err) => {
                error!("{err} -- stopping");
                break;
            }
        }
    }
    info!("recovered {recovered_count} messages");
    Ok(())
}

fn main() {
    run().unwrap_or_else(|e| {
        error!("{e:?}");
        std::process::exit(1);
    });
}
