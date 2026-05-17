export type CliImplementation = "go" | "rust";

export type PathContext = {
  repoRoot: string;
  dataDir: string;
  workDir: string;
  caseWorkDir: string;
};

export type FixtureAction =
  | {
      type: "copy";
      from: string;
      to: string;
    }
  | {
      type: "writeText";
      to: string;
      contents: string;
    }
  | {
      type: "writeBytes";
      to: string;
      bytes: number[];
    }
  | {
      type: "mkdir";
      path: string;
    };

export type CliInvocation = {
  args: string[];
  stdin?: string | { path: string };
  env?: Record<string, string>;
  cwd?: string;
};

export type ExpectedImplementationBehavior = {
  exitCode?: number | "nonzero";
  stdout?: ExpectedOutput;
  stderr?: ExpectedOutput;
  files?: ExpectedFile[];
};

export type ExpectedOutput =
  | { kind: "anything" }
  | { kind: "empty" }
  | { kind: "nonempty" }
  | { kind: "contains"; value: string }
  | { kind: "matches"; pattern: string };

export type ExpectedFile = {
  path: string;
  exists: boolean;
};

export type KnownDifference = {
  id: string;
  summary: string;
  reason: string;
  desiredBehavior: string;
  goBehavior: ExpectedImplementationBehavior;
  rustBehavior: ExpectedImplementationBehavior;
  trackingIssue?: string;
};

export type TextComparatorSpec = {
  kind: "text";
  collapseWhitespace?: boolean;
  trim?: boolean;
  ignoreAnsi?: boolean;
};

export type JsonComparatorSpec = {
  kind: "json";
};

export type TableComparatorSpec = {
  kind: "table";
};

export type CommandListComparatorSpec = {
  kind: "command-list";
  ignoreCommands?: string[];
};

export type InfoComparatorSpec = {
  kind: "info";
};

export type BytesComparatorSpec = {
  kind: "bytes";
};

export type IgnoreComparatorSpec = {
  kind: "ignore";
};

export type NonEmptyComparatorSpec = {
  kind: "nonempty";
};

export type McapComparatorSpec = {
  kind: "mcap";
  mode: "records" | "messages" | "content";
  allowSemanticFallback?: boolean;
};

export type ComparatorSpec =
  | TextComparatorSpec
  | JsonComparatorSpec
  | TableComparatorSpec
  | CommandListComparatorSpec
  | InfoComparatorSpec
  | BytesComparatorSpec
  | IgnoreComparatorSpec
  | NonEmptyComparatorSpec
  | McapComparatorSpec;

export type OutputComparison = {
  stream: "stdout" | "stderr";
  comparator: ComparatorSpec;
};

export type FileComparison = {
  path: string;
  comparator: ComparatorSpec;
};

export type ParityComparison = {
  exitCode?: "same" | number;
  stdout?: ComparatorSpec;
  stderr?: ComparatorSpec;
  files?: FileComparison[];
};

export type CliTestCase = {
  id: string;
  description: string;
  tags?: string[];
  setup?: FixtureAction[];
  invocation: CliInvocation;
  goInvocation?: Partial<CliInvocation>;
  rustInvocation?: Partial<CliInvocation>;
  comparison?: ParityComparison;
  knownDifference?: KnownDifference;
  timeoutMs?: number;
};

export type CliProcessResult = {
  implementation: CliImplementation;
  command: string;
  args: string[];
  cwd: string;
  exitCode: number | undefined;
  signal: NodeJS.Signals | undefined;
  stdout: Buffer;
  stderr: Buffer;
  durationMs: number;
  timedOut: boolean;
  spawnError?: string;
};

export type CaseRunResult = {
  testCase: CliTestCase;
  go: CliProcessResult;
  rust: CliProcessResult;
  status: "passed" | "failed" | "known-difference";
  messages: string[];
};

export type CliConformanceOptions = {
  repoRoot: string;
  dataDir: string;
  goBin: string;
  rustBin: string;
  workDir: string;
  caseRegex?: RegExp;
  includeKnownDifferences: boolean;
  failKnownDifferences: boolean;
  timeoutMs: number;
  keepWorkDir: boolean;
  reportJson?: string;
};
