/* cspell:words ndjson */

import * as Diff from "diff";
import fs from "node:fs/promises";
import path from "node:path";

import { compareMcapBuffers, compareMcapSummary } from "./mcapCompare.ts";
import { stableStringify } from "./stableJson.ts";
import { normalizeTable } from "./tableNormalize.ts";
import { bufferToUtf8, normalizeText } from "./textNormalize.ts";
import type {
  CliProcessResult,
  ComparatorSpec,
  ExpectedFile,
  ExpectedImplementationBehavior,
  ExpectedOutput,
  ParityComparison,
} from "./types.ts";

export async function compareParityResults(
  go: CliProcessResult,
  rust: CliProcessResult,
  comparison: ParityComparison | undefined,
): Promise<string[]> {
  const spec = comparison ?? {
    exitCode: "same",
    stdout: { kind: "text" },
    stderr: { kind: "text" },
  };
  const messages: string[] = [];
  for (const result of [go, rust]) {
    if (result.spawnError != undefined) {
      messages.push(`${result.implementation} spawn failed: ${result.spawnError}`);
    }
  }
  if (messages.length > 0) {
    return messages;
  }

  const exitCodeSpec = spec.exitCode ?? "same";
  if (exitCodeSpec === "same") {
    if (go.exitCode !== rust.exitCode) {
      messages.push(
        `exit code mismatch: go=${go.exitCode ?? "signal"} rust=${rust.exitCode ?? "signal"}`,
      );
    }
    if (go.signal !== rust.signal) {
      messages.push(`signal mismatch: go=${go.signal ?? "none"} rust=${rust.signal ?? "none"}`);
    }
  } else {
    if (go.exitCode !== exitCodeSpec) {
      messages.push(
        `go exit code ${go.exitCode ?? "signal"} did not match expected ${exitCodeSpec}`,
      );
    }
    if (rust.exitCode !== exitCodeSpec) {
      messages.push(
        `rust exit code ${rust.exitCode ?? "signal"} did not match expected ${exitCodeSpec}`,
      );
    }
  }

  if (spec.stdout) {
    messages.push(...(await compareBuffers("stdout", go.stdout, rust.stdout, spec.stdout)));
  }
  if (spec.stderr) {
    messages.push(...(await compareBuffers("stderr", go.stderr, rust.stderr, spec.stderr)));
  }
  for (const fileComparison of spec.files ?? []) {
    const goFile = path.join(go.cwd, fileComparison.path);
    const rustFile = path.join(rust.cwd, fileComparison.path);
    const [goRead, rustRead] = await Promise.all([
      readComparedFile("go", goFile),
      readComparedFile("rust", rustFile),
    ]);
    if (typeof goRead === "string" || typeof rustRead === "string") {
      messages.push(
        ...[goRead, rustRead].filter((read): read is string => typeof read === "string"),
      );
      continue;
    }
    messages.push(
      ...(await compareBuffers(
        `file ${fileComparison.path}`,
        goRead,
        rustRead,
        fileComparison.comparator,
      )),
    );
  }

  return messages;
}

export async function compareExpectedBehavior(
  implementation: "go" | "rust",
  result: CliProcessResult,
  expected: ExpectedImplementationBehavior,
): Promise<string[]> {
  if (result.spawnError != undefined) {
    return [`${implementation} spawn failed: ${result.spawnError}`];
  }

  const messages: string[] = [];
  if (expected.exitCode != undefined) {
    if (expected.exitCode === "nonzero") {
      if (result.exitCode === 0) {
        messages.push(`${implementation} expected nonzero exit code, got 0`);
      } else if (result.exitCode == undefined) {
        messages.push(
          `${implementation} expected nonzero exit code, got ${
            result.signal == undefined ? "unknown exit status" : `signal ${result.signal}`
          }`,
        );
      }
    } else if (result.exitCode !== expected.exitCode) {
      messages.push(
        `${implementation} exit code ${result.exitCode ?? "signal"} did not match expected ${
          expected.exitCode
        }`,
      );
    }
  }
  if (expected.stdout) {
    messages.push(
      ...compareExpectedOutput(`${implementation} stdout`, result.stdout, expected.stdout),
    );
  }
  if (expected.stderr) {
    messages.push(
      ...compareExpectedOutput(`${implementation} stderr`, result.stderr, expected.stderr),
    );
  }
  for (const file of expected.files ?? []) {
    messages.push(...(await compareExpectedFile(implementation, result.cwd, file)));
  }
  return messages;
}

async function compareBuffers(
  label: string,
  expected: Buffer,
  actual: Buffer,
  spec: ComparatorSpec,
): Promise<string[]> {
  switch (spec.kind) {
    case "bytes":
      if (expected.equals(actual)) {
        return [];
      }
      return [`${label} byte mismatch: go=${expected.length} bytes rust=${actual.length} bytes`];
    case "ignore":
      return [];
    case "nonempty": {
      const expectedText = normalizeText(bufferToUtf8(expected));
      const actualText = normalizeText(bufferToUtf8(actual));
      const messages: string[] = [];
      if (expectedText.length === 0) {
        messages.push(`${label} go output expected to be nonempty`);
      }
      if (actualText.length === 0) {
        messages.push(`${label} rust output expected to be nonempty`);
      }
      return messages;
    }
    case "json": {
      const expectedJson = parseCanonicalJson("go", bufferToUtf8(expected));
      const actualJson = parseCanonicalJson("rust", bufferToUtf8(actual));
      if (typeof expectedJson !== "string" || typeof actualJson !== "string") {
        return [expectedJson, actualJson]
          .filter((result): result is { error: string } => typeof result !== "string")
          .map((result) => `${label} ${result.error}`);
      }
      return expectedJson === actualJson ? [] : [formatPatch(label, expectedJson, actualJson)];
    }
    case "ndjson": {
      const expectedJson = parseCanonicalNdjson("go", bufferToUtf8(expected));
      const actualJson = parseCanonicalNdjson("rust", bufferToUtf8(actual));
      if (typeof expectedJson !== "string" || typeof actualJson !== "string") {
        return [expectedJson, actualJson]
          .filter((result): result is { error: string } => typeof result !== "string")
          .map((result) => `${label} ${result.error}`);
      }
      return expectedJson === actualJson ? [] : [formatPatch(label, expectedJson, actualJson)];
    }
    case "table": {
      const expectedTable = normalizeTable(bufferToUtf8(expected));
      const actualTable = normalizeTable(bufferToUtf8(actual));
      return expectedTable === actualTable ? [] : [formatPatch(label, expectedTable, actualTable)];
    }
    case "command-list": {
      const expectedCommands = normalizeCommandList(bufferToUtf8(expected), spec.ignoreCommands);
      const actualCommands = normalizeCommandList(bufferToUtf8(actual), spec.ignoreCommands);
      return expectedCommands === actualCommands
        ? []
        : [formatPatch(label, expectedCommands, actualCommands)];
    }
    case "info": {
      const expectedInfo = normalizeInfo(bufferToUtf8(expected));
      const actualInfo = normalizeInfo(bufferToUtf8(actual));
      return expectedInfo === actualInfo ? [] : [formatPatch(label, expectedInfo, actualInfo)];
    }
    case "mcap": {
      const result = await compareMcapBuffers(expected, actual, {
        mode: spec.mode,
        allowSemanticFallback: spec.allowSemanticFallback ?? false,
      });
      return result.equal
        ? []
        : [
            result.mode === "semantic"
              ? formatPatch(label, result.expected, result.actual)
              : `${label} MCAP byte mismatch and semantic fallback is disabled: go=${result.expected} rust=${result.actual}`,
          ];
    }
    case "text":
      break;
  }

  const expectedText = normalizeText(bufferToUtf8(expected), spec);
  const actualText = normalizeText(bufferToUtf8(actual), spec);
  return expectedText === actualText ? [] : [formatPatch(label, expectedText, actualText)];
}

async function readComparedFile(
  implementation: "go" | "rust",
  filePath: string,
): Promise<Buffer | string> {
  try {
    return await fs.readFile(filePath);
  } catch (error) {
    return `${implementation} expected output file is not readable: ${filePath}\n${
      error instanceof Error ? error.message : String(error)
    }`;
  }
}

function compareExpectedOutput(label: string, buffer: Buffer, expected: ExpectedOutput): string[] {
  const text = normalizeText(bufferToUtf8(buffer));
  switch (expected.kind) {
    case "anything":
      return [];
    case "empty":
      return text.length === 0
        ? []
        : [`${label} expected to be empty, got ${JSON.stringify(text)}`];
    case "nonempty":
      return text.length > 0 ? [] : [`${label} expected to be nonempty`];
    case "contains":
      return text.includes(expected.value)
        ? []
        : [
            `${label} expected to contain ${JSON.stringify(expected.value)}, got ${JSON.stringify(
              text,
            )}`,
          ];
    case "matches":
      return new RegExp(expected.pattern, "u").test(text)
        ? []
        : [`${label} expected to match /${expected.pattern}/u, got ${JSON.stringify(text)}`];
  }
}

async function compareExpectedFile(
  implementation: "go" | "rust",
  cwd: string,
  expected: ExpectedFile,
): Promise<string[]> {
  const filePath = path.join(cwd, expected.path);
  const exists = await fs
    .stat(filePath)
    .then(() => true)
    .catch(() => false);
  if (exists === expected.exists) {
    if (!exists || expected.mcapSummary == undefined) {
      return [];
    }
    const data = await readComparedFile(implementation, filePath);
    if (typeof data === "string") {
      return [data];
    }
    return (await compareMcapSummary(data, expected.mcapSummary)).map(
      (message) => `${implementation} file ${expected.path}: ${message}`,
    );
  }
  return [
    `${implementation} expected file ${expected.path} to ${
      expected.exists ? "exist" : "not exist"
    }`,
  ];
}

function parseCanonicalJson(
  implementation: "go" | "rust",
  text: string,
): string | { error: string } {
  try {
    return stableStringify(JSON.parse(text));
  } catch (error) {
    return {
      error: `${implementation} JSON parse failed: ${
        error instanceof Error ? error.message : String(error)
      }`,
    };
  }
}

function parseCanonicalNdjson(
  implementation: "go" | "rust",
  text: string,
): string | { error: string } {
  const lines = text
    .split(/\r?\n/u)
    .map((line) => line.trim())
    .filter((line) => line.length > 0);
  const parsedLines = [];
  for (const [index, line] of lines.entries()) {
    try {
      parsedLines.push(JSON.parse(line));
    } catch (error) {
      return {
        error: `${implementation} NDJSON parse failed on line ${index + 1}: ${
          error instanceof Error ? error.message : String(error)
        }`,
      };
    }
  }
  return stableStringify(parsedLines);
}

function normalizeInfo(text: string): string {
  return (
    normalizeText(text)
      .split("\n")
      .map((line) => line.trim().replaceAll(/[ \t]+/g, " "))
      // Drop duration/start/end: their rendering intentionally differs between the CLIs (Rust always
      // RFC3339+decimal, Go decimal-only for non-recent times; `0ns` vs `0s`). That difference is
      // asserted by the info-timestamp-format known-difference case, so parity cases ignore it here.
      .filter((line) => !/^(duration|start|end):/i.test(line))
      .map((line) => {
        const channel = /^\((\d+)\) ([^ ]+) (\d+) msgs? .*: (.+)$/.exec(line);
        if (channel) {
          return `channel ${channel[1]} ${channel[2]} ${channel[3]} : ${channel[4]}`;
        }
        return line;
      })
      .join("\n")
  );
}

function normalizeCommandList(text: string, ignoreCommands: readonly string[] = []): string {
  const ignored = new Set(ignoreCommands);
  const lines = normalizeText(text).split("\n");
  const commands: string[] = [];
  let inCommandSection = false;

  for (const line of lines) {
    const trimmed = line.trim();
    if (/^(available )?commands:/iu.test(trimmed)) {
      inCommandSection = true;
      continue;
    }
    if (!inCommandSection) {
      continue;
    }
    if (trimmed.length === 0 || /^[A-Z][A-Za-z ]+:/u.test(trimmed)) {
      break;
    }

    const command = /^([a-z][a-z0-9-]*)\b/u.exec(trimmed)?.[1];
    if (command != undefined && !ignored.has(command)) {
      commands.push(command);
    }
  }

  return [...new Set(commands)].sort().join("\n");
}

function formatPatch(label: string, expected: string, actual: string): string {
  return Diff.createPatch(label, expected + "\n", actual + "\n", "go", "rust");
}
