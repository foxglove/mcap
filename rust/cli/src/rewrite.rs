//! Shared engine for the file-rewriting commands (`filter`, `compress`, `decompress`, `sort`) and
//! the multi-input `merge` command.
//!
//! The single-input commands supply a [`RewriteOptions`] describing the input/output, record
//! selection, and output encoding; [`run`] reads the one input and writes a new MCAP. `merge`
//! supplies a set of inputs to [`run_merge`], which k-way merges their messages by log time.
//!
//! Selection is opt-out: by default everything is kept, and flags narrow the output. Each dimension
//! is independent — narrowing one never drops another. Topic and time-range selection apply to
//! messages; metadata and attachments are kept unless explicitly excluded.
//!
//! Both pipelines place records in a fixed layout: metadata immediately after the header, then
//! messages, then attachments immediately before the data end record, preserving order within each
//! group. Indexed readers seek via the summary index (metadata and attachments are not duplicated
//! into it), so the layout serves linear readers and keeps message chunks unfragmented.
//!
//! The module is split into [`options`] (the caller-facing [`RewriteOptions`], its resolution into
//! the validated [`options::ResolvedOptions`], and record selection), [`engine`] (the single-input
//! read / select / place pipeline), [`merge`] (the multi-input k-way merge pipeline), and
//! [`common`] (the low-level helpers both pipelines share).
mod common;
mod engine;
mod merge;
mod options;

pub(crate) use engine::run;
pub(crate) use merge::{run_merge, MergeOptions};
pub(crate) use options::RewriteOptions;
