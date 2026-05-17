import fs from "node:fs/promises";
import path from "node:path";

import type { FixtureAction, PathContext } from "./types.ts";

const PLACEHOLDER_PATTERN = /\{(repoRoot|dataDir|workDir|caseWorkDir)\}/g;

export function resolvePlaceholders(value: string, context: PathContext): string {
  return value.replace(PLACEHOLDER_PATTERN, (_match, key: keyof PathContext) => context[key]);
}

export function resolveArgs(args: string[], context: PathContext): string[] {
  return args.map((arg) => resolvePlaceholders(arg, context));
}

export async function applyFixtureActions(
  actions: readonly FixtureAction[] | undefined,
  context: PathContext,
): Promise<void> {
  for (const action of actions ?? []) {
    switch (action.type) {
      case "copy": {
        const from = resolvePlaceholders(action.from, context);
        const to = resolvePlaceholders(action.to, context);
        await fs.mkdir(path.dirname(to), { recursive: true });
        await fs.copyFile(from, to);
        break;
      }
      case "writeText": {
        const to = resolvePlaceholders(action.to, context);
        await fs.mkdir(path.dirname(to), { recursive: true });
        await fs.writeFile(to, action.contents, "utf8");
        break;
      }
      case "writeBytes": {
        const to = resolvePlaceholders(action.to, context);
        await fs.mkdir(path.dirname(to), { recursive: true });
        await fs.writeFile(to, Buffer.from(action.bytes));
        break;
      }
      case "mkdir": {
        await fs.mkdir(resolvePlaceholders(action.path, context), { recursive: true });
        break;
      }
    }
  }
}

export async function createCleanDirectory(dir: string): Promise<void> {
  await fs.rm(dir, { recursive: true, force: true });
  await fs.mkdir(dir, { recursive: true });
}

export function safeCaseDirectoryName(caseId: string): string {
  return caseId.replaceAll(/[^a-zA-Z0-9_.-]/g, "_");
}
