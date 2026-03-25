use anyhow::{bail, Result};

pub fn run_info() -> Result<()> {
    bail!("'info' is not implemented yet")
}

pub fn run_version() -> Result<()> {
    bail!("'version' is not implemented yet")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn info_returns_not_implemented() {
        let err = run_info().expect_err("info should be a stub");
        assert_eq!(err.to_string(), "'info' is not implemented yet");
    }

    #[test]
    fn version_returns_not_implemented() {
        let err = run_version().expect_err("version should be a stub");
        assert_eq!(err.to_string(), "'version' is not implemented yet");
    }
}
