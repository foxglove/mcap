import colors from "colors";
import fs from "node:fs/promises";

import type { CaseRunResult, CliConformanceOptions, CliProcessResult } from "./types.ts";

export function printCaseResult(result: CaseRunResult): void {
  const prefix =
    result.status === "passed"
      ? colors.green("pass")
      : result.status === "known-difference"
        ? colors.yellow("known")
        : colors.red("fail");

  console.log(`${prefix} ${result.testCase.id}`);
  if (result.status !== "passed") {
    console.log(`  ${result.testCase.description}`);
    console.log(`  go:   ${formatCommand(result.go)}`);
    console.log(`  rust: ${formatCommand(result.rust)}`);
    for (const message of result.messages) {
      console.log(indent(message, "  "));
    }
  }
}

export function printSummary(results: readonly CaseRunResult[]): void {
  const passed = results.filter((result) => result.status === "passed").length;
  const known = results.filter((result) => result.status === "known-difference").length;
  const failed = results.filter((result) => result.status === "failed").length;
  const summary = `${passed} passed, ${known} known differences, ${failed} failed`;
  console.log(failed === 0 ? colors.green(summary) : colors.red(summary));
}

export async function writeJsonReport(
  results: readonly CaseRunResult[],
  options: CliConformanceOptions,
): Promise<void> {
  if (!options.reportJson) {
    return;
  }
  const report = results.map((result) => ({
    id: result.testCase.id,
    description: result.testCase.description,
    status: result.status,
    knownDifference: result.testCase.knownDifference,
    messages: result.messages,
    go: processReport(result.go),
    rust: processReport(result.rust),
  }));
  await fs.writeFile(options.reportJson, JSON.stringify(report, undefined, 2) + "\n");
}

function processReport(result: CliProcessResult) {
  return {
    command: result.command,
    args: result.args,
    cwd: result.cwd,
    exitCode: result.exitCode,
    signal: result.signal,
    durationMs: result.durationMs,
    timedOut: result.timedOut,
    stdout: result.stdout.toString("utf8"),
    stderr: result.stderr.toString("utf8"),
  };
}

function formatCommand(result: CliProcessResult): string {
  const args = result.args.map((arg) => JSON.stringify(arg)).join(" ");
  const status = result.timedOut
    ? "timed out"
    : result.signal != undefined
      ? `signal ${result.signal}`
      : `exit ${result.exitCode ?? "unknown"}`;
  return `${JSON.stringify(result.command)} ${args} (${status}, ${result.durationMs.toFixed(1)}ms)`;
}

function indent(value: string, prefix: string): string {
  return value
    .trimEnd()
    .split("\n")
    .map((line) => `${prefix}${line}`)
    .join("\n");
}
