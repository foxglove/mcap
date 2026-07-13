//! Shared engine for the file-rewriting commands (`filter`, `compress`, `decompress`, `sort`) and
//! the multi-input `merge` command.
//!
//! [`run`] is the single entrypoint. A command supplies a [`RewriteOptions`] describing the inputs,
//! record selection, output encoding, and (for `merge`) the merge-only knobs; `run` loads the
//! inputs and dispatches by count: one input is a single-input rewrite ([`single`]), more than one
//! is a k-way log-time merge ([`merge`]). An empty input list reads a single input from stdin.
//!
//! Selection is opt-out: by default everything is kept, and flags narrow the output. Each dimension
//! is independent — narrowing one never drops another. Topic and time-range selection apply to
//! messages; metadata and attachments are kept unless explicitly excluded.
//!
//! Both phases place records in a fixed layout: metadata immediately after the header, then
//! messages, then attachments immediately before the data end record, preserving order within each
//! group. Indexed readers seek via the summary index (metadata and attachments are not duplicated
//! into it), so the layout serves linear readers and keeps message chunks unfragmented.
//!
//! The module is split into [`options`] (the caller-facing [`RewriteOptions`], its resolution into
//! the validated [`options::ResolvedOptions`], and record selection), [`single`] (the single-input
//! read / select / place pipeline), [`merge`] (the multi-input k-way merge phase), and [`common`]
//! (the low-level helpers both phases share). The [`run`] dispatcher lives here at the root.
use std::io::{IsTerminal as _, Seek, Write};

use anyhow::{bail, Context, Result};

use crate::source::{self, InputData};
use common::InputRef;
use options::{resolve_options, ResolvedOptions};

mod common;
mod merge;
mod options;
mod single;

pub(crate) use options::RewriteOptions;

/// The entrypoint for every rewrite command (`filter`/`sort`/`compress`/`decompress`) and `merge`.
/// Loads the inputs (an empty file list means a single input from stdin), then dispatches on count:
/// more than one input is a k-way merge, otherwise a single-input rewrite that preserves channel
/// IDs. The merge-only knobs on [`RewriteOptions`] are inert for a single input.
pub(crate) fn run(args: RewriteOptions, source_options: source::SourceOptions) -> Result<()> {
    let opts = resolve_options(&args)?;

    if let Some(output) = opts.output.as_deref() {
        for file in &args.files {
            source::ensure_distinct_local_input_output(file, output)?;
        }
    }

    // An empty file list means a single input read from stdin; otherwise load each path.
    let mut mapped_inputs: Vec<InputData> = Vec::new();
    let mut names: Vec<String> = Vec::new();
    if args.files.is_empty() {
        mapped_inputs.push(source::load_input(None, source_options)?);
        names.push("<stdin>".to_string());
    } else {
        for file in &args.files {
            mapped_inputs.push(source::load_path(file, source_options)?);
            names.push(source::redacted_display(file));
        }
    }
    let inputs: Vec<InputRef<'_>> = mapped_inputs
        .iter()
        .zip(names.iter())
        .map(|(mapped, name)| InputRef {
            name: name.as_str(),
            data: mapped.as_slice(),
        })
        .collect();

    if let Some(output) = &opts.output {
        let sink = std::fs::File::create(output)
            .with_context(|| format!("failed to open '{}' for writing", output.display()))?;
        run_with_writer(sink, false, &inputs, &args, &opts)
    } else {
        if std::io::stdout().is_terminal() {
            bail!("{}", source::PLEASE_REDIRECT);
        }
        let stdout = std::io::stdout();
        let sink = mcap::write::NoSeek::new(stdout.lock());
        run_with_writer(sink, true, &inputs, &args, &opts)
    }
}

/// Builds the output writer and dispatches the message phase by input count: multiple inputs are
/// merged (with cross-input channel-ID remapping and coalescing); a single input is rewritten with
/// its channel IDs preserved.
fn run_with_writer<W: Write + Seek>(
    sink: W,
    disable_seeking: bool,
    inputs: &[InputRef<'_>],
    args: &RewriteOptions,
    opts: &ResolvedOptions,
) -> Result<()> {
    let mut writer = common::create_writer(
        sink,
        &common::WriterConfig {
            profile: common::common_profile(inputs)?,
            use_chunks: opts.use_chunks,
            chunk_size: opts.chunk_size,
            compression: opts.compression,
            include_crc: opts.include_crc,
        },
        disable_seeking,
    )?;

    if inputs.len() > 1 {
        merge::write_merged(
            &mut writer,
            inputs,
            args.coalesce_channels,
            args.dedup_metadata,
            args.allow_duplicate_metadata,
        )?;
    } else {
        let input = inputs.first().expect("at least one input is always loaded");
        single::write_single(
            &mut writer,
            input.data,
            opts,
            args.dedup_metadata,
            args.allow_duplicate_metadata,
        )?;
    }

    writer.finish().context("failed to finish mcap writer")?;
    Ok(())
}
