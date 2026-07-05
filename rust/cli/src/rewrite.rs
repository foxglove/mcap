//! Shared engine for the file-rewriting commands (`filter`, `compress`, `decompress`).
//!
//! A command supplies a [`RewriteOptions`] describing the input/output, record selection, and
//! output encoding; [`run`] reads the input and writes a new MCAP.
//!
//! Selection is opt-out: by default everything is kept, and flags narrow the output. Each dimension
//! is independent — narrowing one never drops another. Topic and time-range selection apply to
//! messages; metadata and attachments are kept unless explicitly excluded.
//!
//! Records are placed in a fixed layout: metadata immediately after the header, then messages, then
//! attachments immediately before the data end record, preserving order within each group. Indexed
//! readers seek via the summary index (metadata and attachments are not duplicated into it), so the
//! layout serves linear readers and keeps message chunks unfragmented.
//!
//! The module is split into [`options`] (the caller-facing [`RewriteOptions`], its resolution into
//! the validated [`options::ResolvedOptions`], and record selection) and [`engine`] (the read /
//! select / place pipeline that drives the writer).
mod engine;
mod options;

pub(crate) use engine::{incomplete_indexed_summary_error, run, summary_supports_indexed_read};
pub(crate) use options::RewriteOptions;
