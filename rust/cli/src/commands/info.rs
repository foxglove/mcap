use anyhow::{bail, Result};

pub fn run() -> Result<()> {
    bail!("'info' is not implemented yet")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn info_returns_not_implemented() {
        let err = run().expect_err("info should be a stub");
        assert_eq!(err.to_string(), "'info' is not implemented yet");
    }
}
