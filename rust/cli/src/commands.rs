use anyhow::{bail, Result};

pub fn run_info() -> Result<()> {
    bail!("'info' is not implemented yet")
}

pub fn run_version() -> Result<()> {
    println!("{}", env!("CARGO_PKG_VERSION"));
    Ok(())
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
    fn version_prints_successfully() {
        run_version().expect("version should print successfully");
    }
}
