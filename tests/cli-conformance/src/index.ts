import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";

import { program } from "commander";

import { cases, validateCases } from "./cases.ts";
import { compareExpectedBehavior, compareParityResults } from "./comparators.ts";
import { createCleanDirectory, safeCaseDirectoryName } from "./fixtures.ts";
import { printCaseResult, printSummary, writeJsonReport } from "./reporter.ts";
import { runCliTestCase } from "./runner.ts";
import type { CaseRunResult, CliConformanceOptions, CliTestCase } from "./types.ts";

type ProgramOptions = {
  dataDir: string;
  goBin: string;
  rustBin: string;
  workDir?: string;
  caseRegex?: RegExp;
  includeKnownDifferences: boolean;
  failKnownDifferences: boolean;
  timeoutMs: number;
  keepWorkDir: boolean;
  reportJson?: string;
};

function repoRoot(): string {
  return path.resolve(import.meta.dirname, "../../..");
}

async function main(options: ProgramOptions): Promise<void> {
  const root = repoRoot();
  const workDir = path.resolve(
    options.workDir ?? (await fs.mkdtemp(path.join(os.tmpdir(), "mcap-cli-conformance-"))),
  );
  const runOptions: CliConformanceOptions = {
    repoRoot: root,
    dataDir: path.resolve(options.dataDir),
    goBin: path.resolve(options.goBin),
    rustBin: path.resolve(options.rustBin),
    workDir,
    caseRegex: options.caseRegex,
    includeKnownDifferences: options.includeKnownDifferences,
    failKnownDifferences: options.failKnownDifferences,
    timeoutMs: options.timeoutMs,
    keepWorkDir: options.keepWorkDir,
    reportJson: options.reportJson == undefined ? undefined : path.resolve(options.reportJson),
  };

  const validationErrors = validateCases(cases);
  if (validationErrors.length > 0) {
    throw new Error(`Invalid CLI conformance case manifest:\n${validationErrors.join("\n")}`);
  }

  await createCleanDirectory(runOptions.workDir);

  const selectedCases = cases.filter((testCase) => shouldRunCase(testCase, runOptions));
  if (selectedCases.length === 0) {
    throw new Error("No CLI conformance cases selected");
  }

  const results: CaseRunResult[] = [];
  for (const testCase of selectedCases) {
    const isolatedCase = {
      ...testCase,
      id: safeCaseDirectoryName(testCase.id),
    };
    const { go, rust } = await runCliTestCase(isolatedCase, runOptions);
    const result = await evaluateCase(testCase, go, rust, runOptions);
    results.push(result);
    printCaseResult(result);
  }

  printSummary(results);
  await writeJsonReport(results, runOptions);

  if (!runOptions.keepWorkDir) {
    await fs.rm(runOptions.workDir, { recursive: true, force: true });
  } else {
    console.log(`kept work directory: ${runOptions.workDir}`);
  }

  if (results.some((result) => result.status === "failed")) {
    process.exit(1);
  }
}

function shouldRunCase(testCase: CliTestCase, options: CliConformanceOptions): boolean {
  if (!options.includeKnownDifferences && testCase.knownDifference != undefined) {
    return false;
  }
  if (options.caseRegex != undefined && !options.caseRegex.test(testCase.id)) {
    return false;
  }
  return true;
}

async function evaluateCase(
  testCase: CliTestCase,
  go: CaseRunResult["go"],
  rust: CaseRunResult["rust"],
  options: CliConformanceOptions,
): Promise<CaseRunResult> {
  const timeoutMessages = [go, rust]
    .filter((result) => result.timedOut)
    .map((result) => `${result.implementation} timed out after ${options.timeoutMs}ms`);

  if (testCase.knownDifference != undefined) {
    const known = testCase.knownDifference;
    const messages = [
      ...timeoutMessages,
      ...(await compareExpectedBehavior("go", go, known.goBehavior)),
      ...(await compareExpectedBehavior("rust", rust, known.rustBehavior)),
    ];
    const status =
      messages.length === 0 && !options.failKnownDifferences ? "known-difference" : "failed";
    return {
      testCase,
      go,
      rust,
      status,
      messages:
        status === "known-difference"
          ? [
              `${known.summary}`,
              `reason: ${known.reason}`,
              `desired behavior: ${known.desiredBehavior}`,
            ]
          : messages,
    };
  }

  const messages = [
    ...timeoutMessages,
    ...(await compareParityResults(go, rust, testCase.comparison)),
  ];
  return {
    testCase,
    go,
    rust,
    status: messages.length === 0 ? "passed" : "failed",
    messages,
  };
}

program
  .requiredOption("--data-dir <dataDir>", "directory containing generated MCAP conformance data")
  .option("--go-bin <path>", "path to legacy Go mcap binary", "go/cli/mcap/bin/mcap")
  .option("--rust-bin <path>", "path to Rust mcap binary", "rust/target/debug/mcap")
  .option("--work-dir <path>", "directory for temporary per-case workspaces")
  .option("--case-regex <pattern>", "only run matching case ids", (value) => new RegExp(value, "u"))
  .option("--include-known-differences", "run known-difference cases", true)
  .option("--fail-known-differences", "treat matching known-difference behavior as failure", false)
  .option(
    "--timeout-ms <number>",
    "per-process timeout in milliseconds",
    (value) => Number(value),
    30_000,
  )
  .option("--keep-work-dir", "keep temporary work directory for debugging", false)
  .option("--report-json <path>", "write machine-readable JSON report")
  .action((options: ProgramOptions) => {
    main(options).catch((error: unknown) => {
      console.error(error instanceof Error ? error.message : error);
      process.exit(1);
    });
  })
  .parse();
