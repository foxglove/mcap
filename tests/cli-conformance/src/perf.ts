import { program } from "commander";
import { spawn } from "node:child_process";
import path from "node:path";
import { performance } from "node:perf_hooks";

import {
  applyFixtureActions,
  cleanupManagedWorkDirectory,
  createCleanDirectory,
  createManagedWorkDirectory,
  resolveArgs,
} from "./fixtures.ts";
import { performanceCases } from "./performanceCases.ts";
import type { CliImplementation, PathContext } from "./types.ts";

const KILL_TIMEOUT_MS = 5_000;

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
  validateOptions(options);

  const root = repoRoot();
  const selectedCases = performanceCases.filter(
    (testCase) => options.caseRegex == undefined || options.caseRegex.test(testCase.id),
  );
  if (selectedCases.length === 0) {
    throw new Error("No CLI performance cases selected");
  }

  const workDirectory = await createManagedWorkDirectory(options.workDir, "mcap-cli-perf-");

  let failed = false;
  try {
    for (const testCase of selectedCases) {
      try {
        const [go, rust] = await Promise.all([
          measureImplementation(
            "go",
            path.resolve(options.goBin),
            testCase,
            options,
            root,
            workDirectory.path,
          ),
          measureImplementation(
            "rust",
            path.resolve(options.rustBin),
            testCase,
            options,
            root,
            workDirectory.path,
          ),
        ]);
        const ratio = rust.medianMs / go.medianMs;
        const margin = testCase.margin ?? 0.2;
        const passes = ratio <= 1 + margin;
        failed ||= !passes;
        const status = passes ? "pass" : options.failOnRegression ? "fail" : "report";
        console.log(
          `${status} ${testCase.id}: go=${formatMeasurement(go)} rust=${formatMeasurement(
            rust,
          )} ratio=${ratio.toFixed(2)} margin=${margin.toFixed(2)} (${testCase.description})`,
        );
      } catch (error) {
        failed = true;
        const status = options.failOnRegression ? "fail" : "report";
        console.log(
          `${status} ${testCase.id}: ${error instanceof Error ? error.message : String(error)}`,
        );
      }
    }
  } finally {
    await cleanupManagedWorkDirectory(workDirectory, { cleanup: "remove" });
  }

  if (failed && options.failOnRegression) {
    process.exit(1);
  }
}

function validateOptions(options: ProgramOptions): void {
  if (!Number.isInteger(options.iterations) || options.iterations <= 0) {
    throw new Error("--iterations must be a positive integer");
  }
  if (!Number.isInteger(options.warmups) || options.warmups < 0) {
    throw new Error("--warmups must be a non-negative integer");
  }
  if (!Number.isFinite(options.timeoutMs) || options.timeoutMs <= 0) {
    throw new Error("--timeout-ms must be a positive number");
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
    let timedOut = false;
    let killTimer: NodeJS.Timeout | undefined;
    const timer = setTimeout(() => {
      timedOut = true;
      child.kill("SIGTERM");
      killTimer = setTimeout(() => {
        child.kill("SIGKILL");
      }, KILL_TIMEOUT_MS);
    }, timeoutMs);
    child.on("error", (error) => {
      clearTimeout(timer);
      if (killTimer != undefined) {
        clearTimeout(killTimer);
      }
      reject(error);
    });
    child.on("close", (code, signal) => {
      clearTimeout(timer);
      if (killTimer != undefined) {
        clearTimeout(killTimer);
      }
      if (timedOut) {
        reject(new Error(`timed out after ${timeoutMs}ms: ${binary} ${args.join(" ")}`));
        return;
      }
      if (code === 0) {
        resolve();
      } else {
        reject(new Error(`${binary} ${args.join(" ")} exited with ${code ?? signal ?? "unknown"}`));
      }
    });
  });
  return performance.now() - started;
}

function formatMeasurement(measurement: Measurement): string {
  if (measurement.durationsMs.length === 0) {
    return "no samples";
  }
  return `${measurement.medianMs.toFixed(2)}ms (min=${Math.min(...measurement.durationsMs).toFixed(
    2,
  )} max=${Math.max(...measurement.durationsMs).toFixed(2)})`;
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
