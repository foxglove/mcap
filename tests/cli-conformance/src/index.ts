import { program } from "commander";
import fs from "node:fs/promises";
import path from "node:path";

import { cases, validateCases } from "./cases.ts";
import { compareExpectedBehavior, compareParityResults } from "./comparators.ts";
import {
  cleanupManagedWorkDirectory,
  createManagedWorkDirectory,
  resolvePlaceholders,
  safeCaseDirectoryName,
} from "./fixtures.ts";
import { printCaseResult, printSummary, writeJsonReport } from "./reporter.ts";
import { mergeInvocation, runCliTestCase } from "./runner.ts";
import type {
  CaseRunResult,
  CliConformanceOptions,
  CliInvocation,
  CliTestCase,
  PathContext,
} from "./types.ts";

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
  const validationErrors = validateCases(cases);
  if (validationErrors.length > 0) {
    throw new Error(`Invalid CLI conformance case manifest:\n${validationErrors.join("\n")}`);
  }

  const workDirectory = await createManagedWorkDirectory(options.workDir, "mcap-cli-conformance-");
  const runOptions: CliConformanceOptions = {
    repoRoot: root,
    dataDir: path.resolve(options.dataDir),
    goBin: path.resolve(options.goBin),
    rustBin: path.resolve(options.rustBin),
    workDir: workDirectory.path,
    caseRegex: options.caseRegex,
    includeKnownDifferences: options.includeKnownDifferences,
    failKnownDifferences: options.failKnownDifferences,
    timeoutMs: options.timeoutMs,
    keepWorkDir: options.keepWorkDir,
    reportJson: options.reportJson == undefined ? undefined : path.resolve(options.reportJson),
  };

  try {
    const selectedCases = cases.filter((testCase) => shouldRunCase(testCase, runOptions));
    if (selectedCases.length === 0) {
      throw new Error("No CLI conformance cases selected");
    }
    await verifyReferencedFixtures(selectedCases, runOptions);

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

    if (runOptions.keepWorkDir) {
      console.log(`kept work directory: ${runOptions.workDir}`);
    }

    if (results.some((result) => result.status === "failed")) {
      process.exitCode = 1;
    }
  } finally {
    await cleanupManagedWorkDirectory(workDirectory, {
      cleanup: runOptions.keepWorkDir ? "keep" : "remove",
    });
  }
}

async function verifyReferencedFixtures(
  selectedCases: readonly CliTestCase[],
  options: CliConformanceOptions,
): Promise<void> {
  const context: PathContext = {
    repoRoot: options.repoRoot,
    dataDir: options.dataDir,
    workDir: options.workDir,
    caseWorkDir: options.workDir,
  };
  const references = new Set<string>();
  for (const testCase of selectedCases) {
    for (const action of testCase.setup ?? []) {
      if (action.type === "copy") {
        collectReference(references, action.from, context);
      }
    }
    collectInvocationReferences(references, testCase.invocation, context);
    collectInvocationReferences(
      references,
      mergeInvocation(testCase.invocation, testCase.goInvocation),
      context,
    );
    collectInvocationReferences(
      references,
      mergeInvocation(testCase.invocation, testCase.rustInvocation),
      context,
    );
  }

  const missing: string[] = [];
  for (const filePath of references) {
    const error = await fixtureFileError(filePath);
    if (error != undefined) {
      missing.push(error);
    }
  }
  if (missing.length > 0) {
    throw new Error(`Invalid CLI conformance fixture files:\n${missing.sort().join("\n")}`);
  }
}

async function fixtureFileError(filePath: string): Promise<string | undefined> {
  const stat = await fs
    .stat(filePath)
    .then((result) => result)
    .catch(() => undefined);
  if (stat == undefined || !stat.isFile()) {
    return `missing file: ${filePath}`;
  }

  const handle = await fs.open(filePath, "r");
  try {
    const prefix = Buffer.alloc(8);
    const { bytesRead } = await handle.read(prefix, 0, prefix.length, 0);
    if (prefix.subarray(0, bytesRead).toString("utf8").startsWith("version ")) {
      return `Git LFS pointer instead of fixture data: ${filePath} (run git lfs pull)`;
    }
  } finally {
    await handle.close();
  }
  return undefined;
}

function collectInvocationReferences(
  references: Set<string>,
  invocation: CliInvocation,
  context: PathContext,
): void {
  for (const arg of invocation.args) {
    collectReference(references, arg, context);
  }
  if (typeof invocation.stdin === "object") {
    collectReference(references, invocation.stdin.path, context);
  }
}

function collectReference(references: Set<string>, value: string, context: PathContext): void {
  if (value.includes("{dataDir}") || value.includes("{repoRoot}")) {
    references.add(resolvePlaceholders(value, context));
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
          : messages.length === 0
            ? [
                `known difference '${known.id}' matched documented behavior; promoted to failure by --fail-known-differences`,
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
  .option("--no-include-known-differences", "skip known-difference cases")
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
