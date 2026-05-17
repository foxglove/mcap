import { spawn } from "node:child_process";
import fs from "node:fs/promises";
import path from "node:path";
import { performance } from "node:perf_hooks";

import {
  applyFixtureActions,
  createCleanDirectory,
  resolveArgs,
  resolvePlaceholders,
} from "./fixtures.ts";
import type {
  CliConformanceOptions,
  CliImplementation,
  CliInvocation,
  CliProcessResult,
  CliTestCase,
  PathContext,
} from "./types.ts";

const KILL_TIMEOUT_MS = 5_000;

export async function runCliTestCase(
  testCase: CliTestCase,
  options: CliConformanceOptions,
): Promise<{ go: CliProcessResult; rust: CliProcessResult }> {
  const caseDir = path.join(options.workDir, testCase.id);
  const goDir = path.join(caseDir, "go");
  const rustDir = path.join(caseDir, "rust");

  const goContext = await prepareImplementationDirectory(testCase, options, goDir);
  const rustContext = await prepareImplementationDirectory(testCase, options, rustDir);

  const [go, rust] = await Promise.all([
    runInvocation(
      "go",
      options.goBin,
      mergeInvocation(testCase.invocation, testCase.goInvocation),
      goContext,
      testCase.timeoutMs ?? options.timeoutMs,
    ),
    runInvocation(
      "rust",
      options.rustBin,
      mergeInvocation(testCase.invocation, testCase.rustInvocation),
      rustContext,
      testCase.timeoutMs ?? options.timeoutMs,
    ),
  ]);

  return { go, rust };
}

async function prepareImplementationDirectory(
  testCase: CliTestCase,
  options: CliConformanceOptions,
  caseWorkDir: string,
): Promise<PathContext> {
  await createCleanDirectory(caseWorkDir);
  const context = {
    repoRoot: options.repoRoot,
    dataDir: options.dataDir,
    workDir: options.workDir,
    caseWorkDir,
  };
  await applyFixtureActions(testCase.setup, context);
  return context;
}

function mergeInvocation(
  base: CliInvocation,
  override: Partial<CliInvocation> | undefined,
): CliInvocation {
  return {
    ...base,
    ...override,
    env: {
      ...base.env,
      ...override?.env,
    },
  };
}

async function runInvocation(
  implementation: CliImplementation,
  command: string,
  invocation: CliInvocation,
  context: PathContext,
  timeoutMs: number,
): Promise<CliProcessResult> {
  const args = resolveArgs(invocation.args, context);
  const cwd =
    invocation.cwd == undefined
      ? context.caseWorkDir
      : resolvePlaceholders(invocation.cwd, context);
  const env = {
    ...process.env,
    // Marker for subprocess diagnostics; neither CLI currently changes behavior based on it.
    MCAP_CLI_CONFORMANCE: "1",
    ...(invocation.env ?? {}),
  };
  const stdin = await resolveStdin(invocation.stdin, context);

  const started = performance.now();
  return await new Promise((resolve) => {
    const child = spawn(command, args, {
      cwd,
      env,
      stdio: ["pipe", "pipe", "pipe"],
    });

    const stdout: Buffer[] = [];
    const stderr: Buffer[] = [];
    let timedOut = false;
    let exitCode: number | undefined;
    let signal: NodeJS.Signals | undefined;
    let killTimer: NodeJS.Timeout | undefined;

    const timer = setTimeout(() => {
      timedOut = true;
      child.kill("SIGTERM");
      killTimer = setTimeout(() => {
        child.kill("SIGKILL");
      }, KILL_TIMEOUT_MS);
    }, timeoutMs);

    child.stdout.on("data", (chunk: Buffer) => {
      stdout.push(chunk);
    });
    child.stderr.on("data", (chunk: Buffer) => {
      stderr.push(chunk);
    });
    child.on("error", (error) => {
      stderr.push(Buffer.from(error.stack ?? error.message));
    });
    child.on("exit", (code, exitSignal) => {
      exitCode = code ?? undefined;
      signal = exitSignal ?? undefined;
    });
    child.on("close", () => {
      clearTimeout(timer);
      if (killTimer != undefined) {
        clearTimeout(killTimer);
      }
      resolve({
        implementation,
        command,
        args,
        cwd,
        exitCode,
        signal,
        stdout: Buffer.concat(stdout),
        stderr: Buffer.concat(stderr),
        durationMs: performance.now() - started,
        timedOut,
      });
    });

    child.stdin.on("error", () => {
      // Some negative/known-difference cases exit before reading stdin.
    });
    if (stdin != undefined) {
      child.stdin.end(stdin);
    } else {
      child.stdin.end();
    }
  });
}

async function resolveStdin(
  stdin: CliInvocation["stdin"],
  context: PathContext,
): Promise<Buffer | string | undefined> {
  if (stdin == undefined) {
    return undefined;
  }
  if (typeof stdin === "string") {
    return resolvePlaceholders(stdin, context);
  }
  return await fs.readFile(resolvePlaceholders(stdin.path, context));
}
