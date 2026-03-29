mod cli;
mod cli_io;
mod commands;
mod logsetup;
mod output;
mod time;

use std::process;

use anyhow::Result;
use clap::Parser;

fn run() -> Result<()> {
    let args = cli::Args::parse();
    logsetup::init_logger(args.verbose, args.color);

    commands::dispatch(args.command)
}

fn main() {
    run().unwrap_or_else(|e| {
        eprintln!("Error: {e:#}");
        process::exit(1);
    });
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use clap::Parser;

    use crate::cli::{
        AddCommand, AddMetadataArgs, AddSubcommand, Args, CatArgs, Command, DoctorArgs, DuArgs,
        FilterArgs, GetAttachmentArgs, GetCommand, GetSubcommand, ListCommand, ListSubcommand,
        MergeArgs, RecoverArgs, SortArgs, VersionCommand,
    };

    #[test]
    fn parses_info_subcommand() {
        let args = Args::try_parse_from(["mcap", "info", "demo.mcap"]).expect("info should parse");
        assert_eq!(
            args.command,
            Command::Info(crate::cli::InputFile {
                file: PathBuf::from("demo.mcap"),
            })
        );
    }

    #[test]
    fn parses_version_subcommand() {
        let args = Args::try_parse_from(["mcap", "version"]).expect("version should parse");
        assert_eq!(
            args.command,
            Command::Version(VersionCommand { library: false })
        );
    }

    #[test]
    fn requires_subcommand() {
        let parse_err = Args::try_parse_from(["mcap"]).expect_err("subcommand is required");
        assert_eq!(
            parse_err.kind(),
            clap::error::ErrorKind::DisplayHelpOnMissingArgumentOrSubcommand
        );
    }

    #[test]
    fn parses_global_verbosity_flag() {
        let args = Args::try_parse_from(["mcap", "-vv", "info", "demo.mcap"])
            .expect("verbosity should parse");
        assert_eq!(args.verbose, 2);
        assert_eq!(
            args.command,
            Command::Info(crate::cli::InputFile {
                file: PathBuf::from("demo.mcap"),
            })
        );
    }

    #[test]
    fn parses_nested_list_subcommands() {
        let args = Args::try_parse_from(["mcap", "list", "channels", "demo.mcap"])
            .expect("list channels should parse");
        assert_eq!(
            args.command,
            Command::List(ListCommand {
                command: ListSubcommand::Channels(crate::cli::InputFile {
                    file: PathBuf::from("demo.mcap"),
                }),
            })
        );
    }

    #[test]
    fn parses_nested_get_subcommands() {
        let args =
            Args::try_parse_from(["mcap", "get", "attachment", "demo.mcap", "--name", "foo"])
                .expect("get attachment should parse");
        assert_eq!(
            args.command,
            Command::Get(GetCommand {
                command: GetSubcommand::Attachment(GetAttachmentArgs {
                    file: PathBuf::from("demo.mcap"),
                    name: "foo".to_string(),
                    offset: None,
                    output: None,
                }),
            })
        );
    }

    #[test]
    fn parses_nested_add_subcommands() {
        let args = Args::try_parse_from([
            "mcap",
            "add",
            "metadata",
            "demo.mcap",
            "--name",
            "meta",
            "--key",
            "foo=bar",
        ])
        .expect("add metadata should parse");
        assert_eq!(
            args.command,
            Command::Add(AddCommand {
                command: AddSubcommand::Metadata(AddMetadataArgs {
                    file: PathBuf::from("demo.mcap"),
                    name: "meta".to_string(),
                    key_values: vec!["foo=bar".to_string()],
                }),
            })
        );
    }

    #[test]
    fn parses_cat_with_options() {
        let args = Args::try_parse_from([
            "mcap",
            "cat",
            "a.mcap",
            "b.mcap",
            "--topics",
            "/tf,/diag",
            "--start",
            "1970-01-01T00:00:01Z",
            "--end",
            "1970-01-01T00:00:02Z",
            "--json",
        ])
        .expect("cat options should parse");
        assert_eq!(
            args.command,
            Command::Cat(CatArgs {
                files: vec![PathBuf::from("a.mcap"), PathBuf::from("b.mcap")],
                topics: Some("/tf,/diag".to_string()),
                start: Some("1970-01-01T00:00:01Z".to_string()),
                end: Some("1970-01-01T00:00:02Z".to_string()),
                json: true,
            })
        );
    }

    #[test]
    fn parses_filter_with_options() {
        let args = Args::try_parse_from([
            "mcap",
            "filter",
            "demo.mcap",
            "-o",
            "out.mcap",
            "-y",
            "^/tf$",
            "-n",
            "^/debug$",
            "--include-metadata",
            "--include-attachments",
            "--start",
            "1000",
            "--end",
            "2000",
            "--output-compression",
            "lz4",
            "--chunk-size",
            "12345",
            "--unchunked",
        ])
        .expect("filter options should parse");
        assert_eq!(
            args.command,
            Command::Filter(FilterArgs {
                file: Some(PathBuf::from("demo.mcap")),
                output: Some(PathBuf::from("out.mcap")),
                include_topic_regex: vec!["^/tf$".to_string()],
                exclude_topic_regex: vec!["^/debug$".to_string()],
                include_metadata: true,
                include_attachments: true,
                start: Some("1000".to_string()),
                end: Some("2000".to_string()),
                output_compression: "lz4".to_string(),
                chunk_size: 12345,
                unchunked: true,
            })
        );
    }

    #[test]
    fn parses_sort_with_options() {
        let args = Args::try_parse_from([
            "mcap",
            "sort",
            "input.mcap",
            "-o",
            "sorted.mcap",
            "--chunk-size",
            "8192",
            "--compression",
            "none",
            "--include-crc",
            "false",
            "--chunked",
            "false",
        ])
        .expect("sort options should parse");
        assert_eq!(
            args.command,
            Command::Sort(SortArgs {
                file: PathBuf::from("input.mcap"),
                output_file: PathBuf::from("sorted.mcap"),
                chunk_size: 8192,
                compression: "none".to_string(),
                include_crc: false,
                chunked: false,
            })
        );
    }

    #[test]
    fn parses_merge_with_options() {
        let args = Args::try_parse_from([
            "mcap",
            "merge",
            "a.mcap",
            "b.mcap",
            "-o",
            "merged.mcap",
            "--chunk-size",
            "16384",
            "--compression",
            "zstd",
        ])
        .expect("merge options should parse");
        assert_eq!(
            args.command,
            Command::Merge(MergeArgs {
                files: vec![PathBuf::from("a.mcap"), PathBuf::from("b.mcap")],
                output_file: PathBuf::from("merged.mcap"),
                chunk_size: 16384,
                compression: "zstd".to_string(),
                include_crc: true,
                chunked: true,
            })
        );
    }

    #[test]
    fn parses_recover_with_options() {
        let args = Args::try_parse_from([
            "mcap",
            "recover",
            "in.mcap",
            "-o",
            "out.mcap",
            "--chunk-size",
            "2048",
            "--compression",
            "none",
        ])
        .expect("recover options should parse");
        assert_eq!(
            args.command,
            Command::Recover(RecoverArgs {
                file: PathBuf::from("in.mcap"),
                output: PathBuf::from("out.mcap"),
                chunk_size: 2048,
                compression: "none".to_string(),
            })
        );
    }

    #[test]
    fn parses_doctor_with_options() {
        let args = Args::try_parse_from(["mcap", "doctor", "in.mcap", "--strict-message-order"])
            .expect("doctor options should parse");
        assert_eq!(
            args.command,
            Command::Doctor(DoctorArgs {
                file: PathBuf::from("in.mcap"),
                strict_message_order: true,
            })
        );
        assert_eq!(args.verbose, 0);
    }

    #[test]
    fn parses_du_with_options() {
        let args = Args::try_parse_from(["mcap", "du", "in.mcap", "--approximate"])
            .expect("du options should parse");
        assert_eq!(
            args.command,
            Command::Du(DuArgs {
                file: PathBuf::from("in.mcap"),
                approximate: true,
            })
        );
    }
}
