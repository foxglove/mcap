use std::process::Command;

/// Leading hex characters of the commit hash to embed.
/// Use a fixed length to ensure GitHub tarballs are deterministic.
const SHORT_SHA_LEN: usize = 9;

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=../../.git/logs/HEAD");

    let revision = git_short_sha().unwrap_or_else(|| "unknown".to_owned());
    println!("cargo:rustc-env=GIT_SHORT_SHA={revision}");
}

/// This build's short sha, extracted from the current tarball or local git checkout.
fn git_short_sha() -> Option<String> {
    archive_sha()
        .or_else(git_rev_parse)
        .map(|sha| sha.chars().take(SHORT_SHA_LEN).collect())
}

fn archive_sha() -> Option<String> {
    // In a `git archive` or GitHub tarball, this string is automatically
    // replaced by the commit hash using `.gitattributes` export-subst.
    let sha = "$Format:%H$";
    // If string was not replaced, return None.
    (!sha.starts_with('$')).then_some(sha.to_owned())
}

fn git_rev_parse() -> Option<String> {
    // Fetch the commit hash from local git checkout.
    let output = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .ok()
        .filter(|output| output.status.success())?;
    let sha = String::from_utf8_lossy(&output.stdout).trim().to_owned();
    (!sha.is_empty()).then_some(sha)
}
