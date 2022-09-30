#[path = "common/logsetup.rs"]
mod logsetup;

use std::{fs, io::BufWriter};

use anyhow::{ensure, Context, Result};
use camino::{Utf8Path, Utf8PathBuf};
use clap::Parser;
use enumset::enum_set;
use log::*;
use memmap::Mmap;

#[derive(Parser, Debug)]
struct Args {
    /// Verbosity (-v, -vv, -vvv, etc.)
    #[clap(short, long, parse(from_occurrences))]
    verbose: u8,

    #[clap(short, long, arg_enum, default_value = "auto")]
    color: logsetup::Color,

    #[clap(help = "input mcap file")]
    input: Utf8PathBuf,

    #[clap(
        short,
        long,
        help = "output mcap file, defaults to <input-file>.recovered.mcap"
    )]
    output: Option<Utf8PathBuf>,
}

fn map_mcap(p: &Utf8Path) -> Result<Mmap> {
    let fd = fs::File::open(p).context("Couldn't open MCAP file")?;
    unsafe { Mmap::map(&fd) }.context("Couldn't map MCAP file")
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
    debug!("{:?}", args);

    let mapped = map_mcap(&args.input)?;
    let output_path = args.output.unwrap_or(make_output_path(args.input)?);
    ensure!(
        !output_path.exists(),
        "output path {output_path} already exists"
    );

    let mut out = mcap::Writer::new(BufWriter::new(fs::File::create(output_path)?))?;

    info!("recovering as many messages as possible...");
    let mut recovered_count = 0;
    for maybe_message in mcap::MessageStream::new_with_options(
        &mapped,
        enum_set!(mcap::read::Options::IgnoreEndMagic),
    )? {
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
    info!("recovered {} messages", recovered_count);
    Ok(())
}

fn main() {
    run().unwrap_or_else(|e| {
        error!("{:?}", e);
        std::process::exit(1);
    });
}
