import { program } from "commander";
import { spawn } from "node:child_process";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { performance } from "node:perf_hooks";

import { applyFixtureActions, createCleanDirectory, resolveArgs } from "./fixtures.ts";
import { performanceCases } from "./performanceCases.ts";
import type { CliImplementation, PathContext } from "./types.ts";

type ProgramOptions = {
  dataDir: string;
  goBin: string;
  rustBin: string;
  workDir?: string;
  caseRegex?: RegExp;
  iterations: number;
  warmups: number;
  timeoutMs: number;
  failOnRegression: boolean;
};

type Measurement = {
  implementation: CliImplementation;
  durationsMs: number[];
  medianMs: number;
};

function repoRoot(): string {
  return path.resolve(import.meta.dirname, "../../..");
}

async function main(options: ProgramOptions): Promise<void> {
  const root = repoRoot();
  const workDir = path.resolve(
    options.workDir ?? (await fs.mkdtemp(path.join(os.tmpdir(), "mcap-cli-perf-"))),
  );
  await createCleanDirectory(workDir);

  const selectedCases = performanceCases.filter(
    (testCase) => options.caseRegex == undefined || options.caseRegex.test(testCase.id),
  );
  if (selectedCases.length === 0) {
    throw new Error("No CLI performance cases selected");
  }

  let failed = false;
  for (const testCase of selectedCases) {
    const [go, rust] = await Promise.all([
      measureImplementation("go", path.resolve(options.goBin), testCase, options, root, workDir),
      measureImplementation(
        "rust",
        path.resolve(options.rustBin),
        testCase,
        options,
        root,
        workDir,
      ),
    ]);
    const ratio = rust.medianMs / go.medianMs;
    const margin = testCase.margin ?? 0.2;
    const passes = ratio <= 1 + margin;
    failed ||= !passes;
    const status = passes ? "pass" : options.failOnRegression ? "fail" : "report";
    console.log(
      `${status} ${testCase.id}: go=${go.medianMs.toFixed(2)}ms rust=${rust.medianMs.toFixed(
        2,
      )}ms ratio=${ratio.toFixed(2)} margin=${margin.toFixed(2)} (${testCase.description})`,
    );
  }

  await fs.rm(workDir, { recursive: true, force: true });
  if (failed && options.failOnRegression) {
    process.exit(1);
  }
}

async function measureImplementation(
  implementation: CliImplementation,
  binary: string,
  testCase: (typeof performanceCases)[number],
  options: ProgramOptions,
  root: string,
  workDir: string,
): Promise<Measurement> {
  const durationsMs: number[] = [];
  const totalRuns = options.warmups + options.iterations;
  for (let runIndex = 0; runIndex < totalRuns; runIndex++) {
    const caseWorkDir = path.join(workDir, `${testCase.id}-${implementation}-${runIndex}`);
    await createCleanDirectory(caseWorkDir);
    const context: PathContext = {
      repoRoot: root,
      dataDir: path.resolve(options.dataDir),
      workDir,
      caseWorkDir,
    };
    await applyFixtureActions(testCase.setup, context);
    const durationMs = await runOnce(
      binary,
      resolveArgs(testCase.args, context),
      caseWorkDir,
      options.timeoutMs,
    );
    if (runIndex >= options.warmups) {
      durationsMs.push(durationMs);
    }
  }

  return {
    implementation,
    durationsMs,
    medianMs: median(durationsMs),
  };
}

async function runOnce(
  binary: string,
  args: string[],
  cwd: string,
  timeoutMs: number,
): Promise<number> {
  const started = performance.now();
  await new Promise<void>((resolve, reject) => {
    const child = spawn(binary, args, { cwd, stdio: ["ignore", "ignore", "ignore"] });
    const timer = setTimeout(() => {
      child.kill("SIGTERM");
      reject(new Error(`timed out after ${timeoutMs}ms: ${binary} ${args.join(" ")}`));
    }, timeoutMs);
    child.on("error", (error) => {
      clearTimeout(timer);
      reject(error);
    });
    child.on("close", (code, signal) => {
      clearTimeout(timer);
      if (code === 0) {
        resolve();
      } else {
        reject(new Error(`${binary} ${args.join(" ")} exited with ${code ?? signal ?? "unknown"}`));
      }
    });
  });
  return performance.now() - started;
}

function median(values: number[]): number {
  if (values.length === 0) {
    return Number.NaN;
  }
  const sorted = [...values].sort((left, right) => left - right);
  const middle = Math.floor(sorted.length / 2);
  if (sorted.length % 2 === 1) {
    return sorted[middle]!;
  }
  return (sorted[middle - 1]! + sorted[middle]!) / 2;
}

program
  .requiredOption("--data-dir <dataDir>", "directory containing generated MCAP conformance data")
  .option("--go-bin <path>", "path to legacy Go mcap binary", "go/cli/mcap/bin/mcap")
  .option("--rust-bin <path>", "path to Rust mcap binary", "rust/target/release/mcap")
  .option("--work-dir <path>", "directory for temporary per-case workspaces")
  .option("--case-regex <pattern>", "only run matching case ids", (value) => new RegExp(value, "u"))
  .option("--iterations <number>", "measured iterations per case", (value) => Number(value), 5)
  .option("--warmups <number>", "warmup iterations per case", (value) => Number(value), 1)
  .option(
    "--timeout-ms <number>",
    "per-process timeout in milliseconds",
    (value) => Number(value),
    60_000,
  )
  .option("--fail-on-regression", "exit nonzero when Rust exceeds the configured margin", false)
  .action((options: ProgramOptions) => {
    main(options).catch((error: unknown) => {
      console.error(error instanceof Error ? error.message : error);
      process.exit(1);
    });
  })
  .parse();
