//! Header `library` field policy shared by every command that authors an output MCAP.
//!
//! The MCAP spec defines `library` as a free-form string identifying the *writer* of the file. The
//! CLI is the writer for any command that authors an output file (compress, decompress, filter,
//! sort, recover, merge, convert), so the output always names the CLI. To keep the provenance of
//! the original recording, the source writer is preserved as a trailing, `; `-separated origin
//! segment, similar to an HTTP `User-Agent`:
//!
//! ```text
//! mcap-cli/<cli-version> mcap-rust/<lib-version>[; <original-source-library>]
//! ```
//!
//! The `mcap-rust/<lib-version>` segment is the underlying `mcap` crate's own identifier
//! (`mcap::LIBRARY_IDENTIFIER`); the leading `mcap-cli/<cli-version>` segment adds the CLI's
//! identity in the same `User-Agent`-style `name/version` form.
//!
//! The origin is bounded to a single segment and is never the CLI itself, so repeatedly processing
//! a file is idempotent: the leading writer token refreshes to the current version while the
//! original source is retained unchanged. Commands that author a fresh file from a non-MCAP or
//! multi-file source (`convert`, `merge`) stamp the writer with no origin.
//!
//! `add` (attachment/metadata) is intentionally exempt: it splices records into the existing file
//! without rewriting it, so it preserves the original header — including its `library` — untouched.

/// Prefix identifying a `library` string previously written by this CLI (any version).
const CLI_PREFIX: &str = "mcap-cli/";

/// The writer identity for the current build: the CLI version paired with the underlying `mcap`
/// crate's own identifier (`mcap::LIBRARY_IDENTIFIER`, e.g. `mcap-rust/<version>`).
pub(crate) fn writer_library() -> String {
    format!(
        "{CLI_PREFIX}{} {}",
        env!("CARGO_PKG_VERSION"),
        mcap::LIBRARY_IDENTIFIER
    )
}

/// Builds the output `library` value from the input file's `library`, applying the policy described
/// in the module docs.
///
/// - No source (or an empty/whitespace source) yields the bare writer identity.
/// - A source previously written by this CLI contributes only its preserved origin segment, so the
///   CLI never stacks its own identity across repeated runs.
/// - Any other (foreign) source becomes the origin verbatim.
pub(crate) fn stamp_library(source: Option<&str>) -> String {
    let writer = writer_library();
    let Some(source) = source.map(str::trim).filter(|source| !source.is_empty()) else {
        return writer;
    };

    let origin = if source.starts_with(CLI_PREFIX) {
        // Already a CLI-authored string: keep only the original source it was carrying (everything
        // after the first separator), if any.
        source.split_once("; ").map(|(_, origin)| origin)
    } else {
        Some(source)
    };

    match origin.map(str::trim).filter(|origin| !origin.is_empty()) {
        Some(origin) => format!("{writer}; {origin}"),
        None => writer,
    }
}

#[cfg(test)]
mod tests {
    use super::{stamp_library, writer_library};

    #[test]
    fn empty_or_missing_source_uses_writer_only() {
        let writer = writer_library();
        assert_eq!(stamp_library(None), writer);
        assert_eq!(stamp_library(Some("")), writer);
        assert_eq!(stamp_library(Some("   ")), writer);
    }

    #[test]
    fn foreign_source_becomes_origin() {
        let writer = writer_library();
        assert_eq!(
            stamp_library(Some("foxglove-studio/2.0")),
            format!("{writer}; foxglove-studio/2.0")
        );
    }

    #[test]
    fn foreign_source_with_separator_is_preserved_whole() {
        let writer = writer_library();
        assert_eq!(
            stamp_library(Some("mcap go v1.8.0; recorder/3")),
            format!("{writer}; mcap go v1.8.0; recorder/3")
        );
    }

    #[test]
    fn reprocessing_is_idempotent_and_keeps_original_origin() {
        let once = stamp_library(Some("foxglove-studio/2.0"));
        let twice = stamp_library(Some(&once));
        assert_eq!(once, twice);
        assert_eq!(twice, format!("{}; foxglove-studio/2.0", writer_library()));
    }

    #[test]
    fn prior_cli_output_without_origin_collapses_to_writer() {
        let writer = writer_library();
        assert_eq!(stamp_library(Some(&writer)), writer);
        // An older CLI version (different version token) is still recognized as the CLI and dropped.
        assert_eq!(
            stamp_library(Some("mcap-cli/0.0.1 mcap-rust/0.1.0")),
            writer
        );
    }
}
