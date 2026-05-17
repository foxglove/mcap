import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";

import type { FixtureAction, PathContext } from "./types.ts";

const PLACEHOLDER_PATTERN = /\{(repoRoot|dataDir|workDir|caseWorkDir)\}/g;

export type ManagedWorkDirectory = {
  path: string;
  removeRootOnCleanup: boolean;
};

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

export async function createManagedWorkDirectory(
  requestedDir: string | undefined,
  tempPrefix: string,
): Promise<ManagedWorkDirectory> {
  if (requestedDir == undefined) {
    return {
      path: await fs.mkdtemp(path.join(os.tmpdir(), tempPrefix)),
      removeRootOnCleanup: true,
    };
  }

  const dir = path.resolve(requestedDir);
  const exists = await fs
    .stat(dir)
    .then((stat) => {
      if (!stat.isDirectory()) {
        throw new Error(`work directory is not a directory: ${dir}`);
      }
      return true;
    })
    .catch((error: NodeJS.ErrnoException) => {
      if (error.code === "ENOENT") {
        return false;
      }
      throw error;
    });

  if (exists) {
    const entries = await fs.readdir(dir);
    if (entries.length > 0) {
      throw new Error(
        `refusing to use non-empty --work-dir ${dir}; choose an empty directory or omit --work-dir`,
      );
    }
  } else {
    await fs.mkdir(dir, { recursive: true });
  }

  return { path: dir, removeRootOnCleanup: false };
}

export async function cleanupManagedWorkDirectory(
  workDirectory: ManagedWorkDirectory,
  keepWorkDir: boolean,
): Promise<void> {
  if (keepWorkDir) {
    return;
  }
  if (workDirectory.removeRootOnCleanup) {
    await fs.rm(workDirectory.path, { recursive: true, force: true });
    return;
  }
  for (const entry of await fs.readdir(workDirectory.path)) {
    await fs.rm(path.join(workDirectory.path, entry), { recursive: true, force: true });
  }
}

export function safeCaseDirectoryName(caseId: string): string {
  return caseId.replaceAll(/[^a-zA-Z0-9_.-]/g, "_");
}
