use std::io::{self, ErrorKind, Write};

use anyhow::{Context, Result};
use clap::CommandFactory;
use clap_complete::generate;

use crate::cli::{Args, CompletionCommand};

/// Generate a shell completion script for the requested shell and write it to stdout.
pub fn run(args: CompletionCommand) -> Result<()> {
    let mut buffer = Vec::new();
    let mut cmd = Args::command();
    let bin_name = cmd.get_name().to_string();
    generate(args.shell, &mut cmd, bin_name, &mut buffer);

    write_completion(&mut io::stdout(), &buffer)
}

fn write_completion(writer: &mut impl Write, buffer: &[u8]) -> Result<()> {
    match writer.write_all(buffer) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == ErrorKind::BrokenPipe => Ok(()),
        Err(err) => Err(err).context("failed to write completion script"),
    }
}

#[cfg(test)]
mod tests {
    use std::io::{self, ErrorKind, Write};

    use super::write_completion;

    struct FailingWriter {
        kind: ErrorKind,
    }

    impl Write for FailingWriter {
        fn write(&mut self, _buf: &[u8]) -> io::Result<usize> {
            Err(io::Error::new(self.kind, "failed"))
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn write_completion_ignores_broken_pipe() {
        let mut writer = FailingWriter {
            kind: ErrorKind::BrokenPipe,
        };

        write_completion(&mut writer, b"completion").expect("broken pipe should be ignored");
    }

    #[test]
    fn write_completion_reports_other_errors() {
        let mut writer = FailingWriter {
            kind: ErrorKind::PermissionDenied,
        };

        let err =
            write_completion(&mut writer, b"completion").expect_err("other errors should fail");
        assert!(err
            .to_string()
            .contains("failed to write completion script"));
    }
}
