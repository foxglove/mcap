use std::borrow::Cow;
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::time::{Duration, Instant};

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

const DEFAULT_TOTAL_MIB: u64 = 16;
const DEFAULT_MERGE_INPUTS: usize = 4;
const DEFAULT_CHUNK_SIZE: u64 = 4 * 1024 * 1024;
const DEFAULT_SAMPLE_SIZE: usize = 10;
const DEFAULT_WARMUP_MS: u64 = 250;
const DEFAULT_MEASUREMENT_SECS: u64 = 2;
const PAYLOAD_SIZES: &[usize] = &[100, 1024, 10 * 1024, 100 * 1024, 1024 * 1024];

#[derive(Debug, Clone)]
struct BenchConfig {
    root: PathBuf,
    mcap_bin: PathBuf,
    total_mib: u64,
    merge_inputs: usize,
    chunk_size: u64,
}

#[derive(Debug, Clone)]
struct InputCase {
    path: PathBuf,
    payload_size: usize,
    message_count: usize,
    selected_count: usize,
}

#[derive(Debug, Clone)]
struct MergeCase {
    paths: Vec<PathBuf>,
    payload_size: usize,
    expected_count: usize,
}

#[derive(Debug, Clone, Copy)]
enum InputOrder {
    Ordered,
    Reversed,
    Interleaved {
        input_idx: usize,
        input_count: usize,
    },
}

fn criterion_config() -> Criterion {
    Criterion::default()
        .sample_size(env_usize("MCAP_CLI_BENCH_SAMPLE_SIZE", DEFAULT_SAMPLE_SIZE).max(10))
        .warm_up_time(Duration::from_millis(env_u64(
            "MCAP_CLI_BENCH_WARMUP_MS",
            DEFAULT_WARMUP_MS,
        )))
        .measurement_time(Duration::from_secs(env_u64(
            "MCAP_CLI_BENCH_MEASUREMENT_SECS",
            DEFAULT_MEASUREMENT_SECS,
        )))
}

fn bench_commands(c: &mut Criterion) {
    let config = BenchConfig::from_env();
    let suites = SuiteSelection::from_args();
    std::fs::create_dir_all(config.inputs_dir()).expect("create benchmark input dir");
    std::fs::create_dir_all(config.outputs_dir()).expect("create benchmark output dir");
    assert!(
        config.mcap_bin.exists(),
        "mcap binary '{}' does not exist; set MCAP_CLI_BENCH_BIN to a built mcap binary",
        config.mcap_bin.display()
    );

    if suites.merge {
        let merge_cases = PAYLOAD_SIZES
            .iter()
            .copied()
            .map(|payload_size| config.merge_case(payload_size))
            .collect::<Vec<_>>();
        bench_merge(c, &config, &merge_cases);
    }

    if suites.filter || suites.decompress {
        let input_cases = PAYLOAD_SIZES
            .iter()
            .copied()
            .map(|payload_size| config.input_case(payload_size, InputOrder::Ordered, Some("zstd")))
            .collect::<Vec<_>>();
        if suites.filter {
            bench_filter(c, &config, &input_cases);
        }
        if suites.decompress {
            bench_decompress(c, &config, &input_cases);
        }
    }

    if suites.sort {
        let sort_cases = PAYLOAD_SIZES
            .iter()
            .copied()
            .map(|payload_size| config.input_case(payload_size, InputOrder::Reversed, Some("zstd")))
            .collect::<Vec<_>>();
        bench_sort(c, &config, &sort_cases);
    }

    if suites.compress {
        let uncompressed_cases = PAYLOAD_SIZES
            .iter()
            .copied()
            .map(|payload_size| config.input_case(payload_size, InputOrder::Ordered, None))
            .collect::<Vec<_>>();
        bench_compress(c, &config, &uncompressed_cases);
    }
}

#[derive(Debug, Clone, Copy)]
struct SuiteSelection {
    merge: bool,
    filter: bool,
    sort: bool,
    compress: bool,
    decompress: bool,
}

impl SuiteSelection {
    fn from_args() -> Self {
        // Mirror the documented Criterion filters (`-- merge`, `-- filter`) so filtered runs only
        // generate inputs for selected suites.
        let filters = std::env::args()
            .skip(1)
            .filter(|arg| !arg.starts_with('-'))
            .collect::<Vec<_>>();
        let selected = |name: &str| {
            filters
                .iter()
                .any(|filter| filter.split(['/', ':']).any(|component| component == name))
        };
        let any_suite = ["merge", "filter", "sort", "compress", "decompress"]
            .iter()
            .any(|name| selected(name));

        if !any_suite {
            return Self {
                merge: true,
                filter: true,
                sort: true,
                compress: true,
                decompress: true,
            };
        }

        Self {
            merge: selected("merge"),
            filter: selected("filter"),
            sort: selected("sort"),
            compress: selected("compress"),
            decompress: selected("decompress"),
        }
    }
}

fn bench_merge(c: &mut Criterion, config: &BenchConfig, cases: &[MergeCase]) {
    let mut group = c.benchmark_group("cli/merge");
    for case in cases {
        group.throughput(Throughput::Bytes(config.total_bytes()));
        group.bench_with_input(
            BenchmarkId::from_parameter(size_label(case.payload_size)),
            case,
            |bench, case| {
                bench.iter_custom(|iters| {
                    run_measured(iters, |iteration| {
                        let output = config.output_path("merge", case.payload_size, iteration);
                        let mut args = vec![OsString::from("merge")];
                        args.extend(case.paths.iter().map(|path| path.as_os_str().to_owned()));
                        args.extend([
                            OsString::from("--compression"),
                            OsString::from("zstd"),
                            OsString::from("--chunk-size"),
                            OsString::from(config.chunk_size.to_string()),
                            OsString::from("--output-file"),
                            output.as_os_str().to_owned(),
                        ]);
                        let duration = run_mcap(&config.mcap_bin, args);
                        if iteration == 0 {
                            validate_output(&output, case.expected_count, true);
                        }
                        remove_file(&output);
                        duration
                    })
                });
            },
        );
    }
    group.finish();
}

fn bench_filter(c: &mut Criterion, config: &BenchConfig, cases: &[InputCase]) {
    let mut group = c.benchmark_group("cli/filter");
    for case in cases {
        group.throughput(Throughput::Bytes(config.total_bytes()));
        group.bench_with_input(
            BenchmarkId::from_parameter(size_label(case.payload_size)),
            case,
            |bench, case| {
                bench.iter_custom(|iters| {
                    run_measured(iters, |iteration| {
                        let output = config.output_path("filter", case.payload_size, iteration);
                        let args = vec![
                            OsString::from("filter"),
                            case.path.as_os_str().to_owned(),
                            OsString::from("--include-topic-regex"),
                            OsString::from("/bench/selected"),
                            OsString::from("--output"),
                            output.as_os_str().to_owned(),
                            OsString::from("--output-compression"),
                            OsString::from("zstd"),
                            OsString::from("--chunk-size"),
                            OsString::from(config.chunk_size.to_string()),
                        ];
                        let duration = run_mcap(&config.mcap_bin, args);
                        if iteration == 0 {
                            validate_output(&output, case.selected_count, true);
                        }
                        remove_file(&output);
                        duration
                    })
                });
            },
        );
    }
    group.finish();
}

fn bench_sort(c: &mut Criterion, config: &BenchConfig, cases: &[InputCase]) {
    let mut group = c.benchmark_group("cli/sort");
    for case in cases {
        group.throughput(Throughput::Bytes(config.total_bytes()));
        group.bench_with_input(
            BenchmarkId::from_parameter(size_label(case.payload_size)),
            case,
            |bench, case| {
                bench.iter_custom(|iters| {
                    run_measured(iters, |iteration| {
                        let output = config.output_path("sort", case.payload_size, iteration);
                        let args = vec![
                            OsString::from("sort"),
                            case.path.as_os_str().to_owned(),
                            OsString::from("--compression"),
                            OsString::from("zstd"),
                            OsString::from("--chunk-size"),
                            OsString::from(config.chunk_size.to_string()),
                            OsString::from("--output-file"),
                            output.as_os_str().to_owned(),
                        ];
                        let duration = run_mcap(&config.mcap_bin, args);
                        if iteration == 0 {
                            validate_output(&output, case.message_count, true);
                        }
                        remove_file(&output);
                        duration
                    })
                });
            },
        );
    }
    group.finish();
}

fn bench_compress(c: &mut Criterion, config: &BenchConfig, cases: &[InputCase]) {
    let mut group = c.benchmark_group("cli/compress");
    for case in cases {
        group.throughput(Throughput::Bytes(config.total_bytes()));
        group.bench_with_input(
            BenchmarkId::from_parameter(size_label(case.payload_size)),
            case,
            |bench, case| {
                bench.iter_custom(|iters| {
                    run_measured(iters, |iteration| {
                        let output = config.output_path("compress", case.payload_size, iteration);
                        let args = vec![
                            OsString::from("compress"),
                            case.path.as_os_str().to_owned(),
                            OsString::from("--output"),
                            output.as_os_str().to_owned(),
                            OsString::from("--compression"),
                            OsString::from("zstd"),
                            OsString::from("--chunk-size"),
                            OsString::from(config.chunk_size.to_string()),
                        ];
                        let duration = run_mcap(&config.mcap_bin, args);
                        if iteration == 0 {
                            validate_output(&output, case.message_count, true);
                        }
                        remove_file(&output);
                        duration
                    })
                });
            },
        );
    }
    group.finish();
}

fn bench_decompress(c: &mut Criterion, config: &BenchConfig, cases: &[InputCase]) {
    let mut group = c.benchmark_group("cli/decompress");
    for case in cases {
        group.throughput(Throughput::Bytes(config.total_bytes()));
        group.bench_with_input(
            BenchmarkId::from_parameter(size_label(case.payload_size)),
            case,
            |bench, case| {
                bench.iter_custom(|iters| {
                    run_measured(iters, |iteration| {
                        let output = config.output_path("decompress", case.payload_size, iteration);
                        let args = vec![
                            OsString::from("decompress"),
                            case.path.as_os_str().to_owned(),
                            OsString::from("--output"),
                            output.as_os_str().to_owned(),
                            OsString::from("--chunk-size"),
                            OsString::from(config.chunk_size.to_string()),
                        ];
                        let duration = run_mcap(&config.mcap_bin, args);
                        if iteration == 0 {
                            validate_output(&output, case.message_count, true);
                        }
                        remove_file(&output);
                        duration
                    })
                });
            },
        );
    }
    group.finish();
}

impl BenchConfig {
    fn from_env() -> Self {
        Self {
            root: env_path("MCAP_CLI_BENCH_DIR").unwrap_or_else(default_bench_dir),
            mcap_bin: env_path("MCAP_CLI_BENCH_BIN").unwrap_or_else(default_mcap_bin),
            total_mib: env_u64("MCAP_CLI_BENCH_TOTAL_MIB", DEFAULT_TOTAL_MIB),
            merge_inputs: env_usize("MCAP_CLI_BENCH_INPUTS", DEFAULT_MERGE_INPUTS).max(1),
            chunk_size: env_u64("MCAP_CLI_BENCH_CHUNK_SIZE", DEFAULT_CHUNK_SIZE),
        }
    }

    fn total_bytes(&self) -> u64 {
        self.total_mib * 1024 * 1024
    }

    fn inputs_dir(&self) -> PathBuf {
        self.root.join("inputs")
    }

    fn outputs_dir(&self) -> PathBuf {
        self.root.join("outputs")
    }

    fn input_case(
        &self,
        payload_size: usize,
        order: InputOrder,
        compression: Option<&'static str>,
    ) -> InputCase {
        let message_count = message_count(self.total_bytes(), payload_size, 1);
        let selected_count = message_count.div_ceil(2);
        let path = self.input_path(payload_size, message_count, order, compression, 0);
        if !path.exists() {
            write_input(
                &path,
                payload_size,
                message_count,
                order,
                compression,
                self.chunk_size,
            );
        }
        InputCase {
            path,
            payload_size,
            message_count,
            selected_count,
        }
    }

    fn merge_case(&self, payload_size: usize) -> MergeCase {
        let messages_per_input = message_count(self.total_bytes(), payload_size, self.merge_inputs);
        let paths = (0..self.merge_inputs)
            .map(|input_idx| {
                let order = InputOrder::Interleaved {
                    input_idx,
                    input_count: self.merge_inputs,
                };
                let path = self.input_path(
                    payload_size,
                    messages_per_input,
                    order,
                    Some("zstd"),
                    input_idx,
                );
                if !path.exists() {
                    write_input(
                        &path,
                        payload_size,
                        messages_per_input,
                        order,
                        Some("zstd"),
                        self.chunk_size,
                    );
                }
                path
            })
            .collect::<Vec<_>>();
        MergeCase {
            paths,
            payload_size,
            expected_count: messages_per_input * self.merge_inputs,
        }
    }

    fn input_path(
        &self,
        payload_size: usize,
        message_count: usize,
        order: InputOrder,
        compression: Option<&str>,
        input_idx: usize,
    ) -> PathBuf {
        let order_label = match order {
            InputOrder::Ordered => "ordered".to_string(),
            InputOrder::Reversed => "reversed".to_string(),
            InputOrder::Interleaved { input_count, .. } => format!("interleaved{input_count}"),
        };
        let compression_label = compression.unwrap_or("none");
        self.inputs_dir().join(format!(
            "payload{}_messages{}_chunk{}_{}_{}_part{}.mcap",
            payload_size, message_count, self.chunk_size, order_label, compression_label, input_idx
        ))
    }

    fn output_path(&self, command: &str, payload_size: usize, iteration: u64) -> PathBuf {
        self.outputs_dir().join(format!(
            "{}_payload{}_pid{}_iter{}.mcap",
            command,
            payload_size,
            std::process::id(),
            iteration
        ))
    }
}

fn write_input(
    path: &Path,
    payload_size: usize,
    message_count: usize,
    order: InputOrder,
    compression: Option<&str>,
    chunk_size: u64,
) {
    let compression = match compression {
        Some("zstd") => Some(mcap::Compression::Zstd),
        Some("lz4") => Some(mcap::Compression::Lz4),
        Some(value) => panic!("unsupported compression {value}"),
        None => None,
    };

    let file = std::fs::File::create(path).expect("create generated MCAP");
    let mut writer = mcap::WriteOptions::new()
        .profile("bench")
        .library("mcap-cli-bench")
        .compression(compression)
        .chunk_size(Some(chunk_size))
        .create(std::io::BufWriter::new(file))
        .expect("create MCAP writer");

    let schema = Arc::new(mcap::Schema {
        id: 1,
        name: "Bench".to_string(),
        encoding: "raw".to_string(),
        data: Cow::Borrowed(b"{}"),
    });
    let selected = Arc::new(mcap::Channel {
        id: 1,
        topic: "/bench/selected".to_string(),
        schema: Some(schema.clone()),
        message_encoding: "raw".to_string(),
        metadata: Default::default(),
    });
    let other = Arc::new(mcap::Channel {
        id: 2,
        topic: "/bench/other".to_string(),
        schema: Some(schema),
        message_encoding: "raw".to_string(),
        metadata: Default::default(),
    });

    let mut payload = vec![0u8; payload_size];
    for input_order in 0..message_count {
        fill_payload(&mut payload, input_order as u64);
        let log_time = match order {
            InputOrder::Ordered => input_order as u64,
            InputOrder::Reversed => (message_count - input_order) as u64,
            InputOrder::Interleaved {
                input_idx,
                input_count,
            } => (input_order * input_count + input_idx) as u64,
        };
        let channel = if input_order % 2 == 0 {
            selected.clone()
        } else {
            other.clone()
        };
        writer
            .write(&mcap::Message {
                channel,
                sequence: input_order as u32,
                log_time,
                publish_time: log_time,
                data: Cow::Borrowed(payload.as_slice()),
            })
            .expect("write generated message");
    }
    writer.finish().expect("finish generated MCAP");
}

fn run_measured<F>(iters: u64, mut run_once: F) -> Duration
where
    F: FnMut(u64) -> Duration,
{
    let mut total = Duration::ZERO;
    for iteration in 0..iters {
        total += run_once(iteration);
    }
    total
}

fn run_mcap(bin: &Path, args: Vec<OsString>) -> Duration {
    let start = Instant::now();
    let output = Command::new(bin)
        .args(args)
        .output()
        .expect("run mcap command");
    let duration = start.elapsed();
    if !output.status.success() {
        panic!(
            "mcap command failed with status {:?}\nstdout:\n{}\nstderr:\n{}",
            output.status.code(),
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
    duration
}

fn validate_output(path: &Path, expected_count: usize, require_ordered: bool) {
    let bytes = std::fs::read(path).expect("read benchmark output");
    assert!(
        bytes.starts_with(mcap::MAGIC) && bytes.ends_with(mcap::MAGIC),
        "output is not an MCAP file: {}",
        path.display()
    );
    assert!(
        mcap::Summary::read(&bytes)
            .expect("read output summary")
            .is_some(),
        "output should include a summary: {}",
        path.display()
    );

    let mut count = 0usize;
    let mut previous_log_time = None;
    for message in mcap::MessageStream::new(&bytes).expect("open output message stream") {
        let message = message.expect("read output message");
        if require_ordered {
            if let Some(previous) = previous_log_time {
                assert!(
                    previous <= message.log_time,
                    "messages are not log-time ordered in {}",
                    path.display()
                );
            }
            previous_log_time = Some(message.log_time);
        }
        count += 1;
    }
    assert_eq!(
        count,
        expected_count,
        "unexpected output message count for {}",
        path.display()
    );
}

fn remove_file(path: &Path) {
    match std::fs::remove_file(path) {
        Ok(()) => {}
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
        Err(err) => panic!("failed to remove {}: {err}", path.display()),
    }
}

fn message_count(total_bytes: u64, payload_size: usize, inputs: usize) -> usize {
    ((total_bytes / payload_size as u64) / inputs as u64).max(1) as usize
}

fn fill_payload(payload: &mut [u8], message_idx: u64) {
    let mut state = splitmix64(message_idx ^ payload.len() as u64);
    for chunk in payload.chunks_mut(8) {
        state = splitmix64(state);
        let bytes = state.to_le_bytes();
        chunk.copy_from_slice(&bytes[..chunk.len()]);
    }
}

fn splitmix64(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9e37_79b9_7f4a_7c15);
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^ (x >> 31)
}

fn default_bench_dir() -> PathBuf {
    workspace_root().join("target/mcap-cli-bench")
}

fn default_mcap_bin() -> PathBuf {
    if let Some(path) = option_env!("CARGO_BIN_EXE_mcap") {
        return PathBuf::from(path);
    }
    if let Ok(target_dir) = std::env::var("CARGO_TARGET_DIR") {
        return PathBuf::from(target_dir).join("release/mcap");
    }
    workspace_root().join("target/release/mcap")
}

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("rust/cli has parent rust")
        .parent()
        .expect("rust has parent workspace")
        .to_path_buf()
}

fn env_path(name: &str) -> Option<PathBuf> {
    std::env::var_os(name).map(PathBuf::from)
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn size_label(bytes: usize) -> String {
    match bytes {
        100 => "100B".to_string(),
        1024 => "1KiB".to_string(),
        10240 => "10KiB".to_string(),
        102400 => "100KiB".to_string(),
        1048576 => "1MiB".to_string(),
        _ => format!("{bytes}B"),
    }
}

criterion_group! {
    name = benches;
    config = criterion_config();
    targets = bench_commands
}
criterion_main!(benches);
