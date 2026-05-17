import type { CliTestCase } from "./types.ts";

const ONE_MESSAGE = "{dataDir}/OneMessage/OneMessage-ch-chx-mx-pad-rch-rsh-st-sum.mcap";
const ONE_SCHEMALESS =
  "{dataDir}/OneSchemalessMessage/OneSchemalessMessage-ch-chx-mx-pad-rch-st.mcap";
const TEN_MESSAGES = "{dataDir}/TenMessages/TenMessages-ch-chx-mx-pad-rch-rsh-st-sum.mcap";
const ONE_ATTACHMENT = "{dataDir}/OneAttachment/OneAttachment-ax-st-sum.mcap";
const ONE_METADATA = "{dataDir}/OneMetadata/OneMetadata-mdx-st-sum.mcap";

export const cases: CliTestCase[] = [
  {
    id: "version-command-exits-successfully",
    description:
      "Both CLIs expose a version command. Version strings are not compared while Rust CLI is unreleased.",
    tags: ["surface", "version"],
    invocation: { args: ["version"] },
    comparison: { exitCode: 0 },
  },
  {
    id: "cat-one-message",
    description: "Basic cat output matches for an indexed MCAP with one schematized message.",
    tags: ["cat", "stdout"],
    invocation: { args: ["cat", ONE_MESSAGE] },
    comparison: {
      exitCode: 0,
      stdout: { kind: "text", trim: false },
      stderr: { kind: "text" },
    },
  },
  {
    id: "cat-one-schemaless-message",
    description: "Basic cat output matches for an indexed MCAP with one schemaless message.",
    tags: ["cat", "stdout"],
    invocation: { args: ["cat", ONE_SCHEMALESS] },
    comparison: {
      exitCode: 0,
      stdout: { kind: "text", trim: false },
      stderr: { kind: "text" },
    },
  },
  {
    id: "list-channels",
    description: "Channel listing contains the same table data despite formatter differences.",
    tags: ["list", "table"],
    invocation: { args: ["list", "channels", ONE_MESSAGE] },
    comparison: {
      exitCode: 0,
      stdout: { kind: "table" },
      stderr: { kind: "text" },
    },
  },
  {
    id: "list-attachments",
    description: "Attachment listing contains the same table data despite formatter differences.",
    tags: ["list", "table", "attachments"],
    invocation: { args: ["list", "attachments", ONE_ATTACHMENT] },
    comparison: {
      exitCode: 0,
      stdout: { kind: "table" },
      stderr: { kind: "text" },
    },
  },
  {
    id: "get-metadata-json",
    description: "Metadata extraction returns the same JSON object.",
    tags: ["get", "metadata", "json"],
    invocation: { args: ["get", "metadata", ONE_METADATA, "--name", "myMetadata"] },
    comparison: {
      exitCode: 0,
      stdout: { kind: "json" },
      stderr: { kind: "text" },
    },
  },
  {
    id: "get-attachment-by-name",
    description: "Attachment extraction by name writes identical payload bytes.",
    tags: ["get", "attachments", "bytes"],
    invocation: {
      args: ["get", "attachment", ONE_ATTACHMENT, "--name", "myFile", "-o", "attachment.bin"],
    },
    comparison: {
      exitCode: 0,
      stdout: { kind: "text" },
      stderr: { kind: "text" },
      files: [{ path: "attachment.bin", comparator: { kind: "bytes" } }],
    },
  },
  {
    id: "filter-topic-output-messages",
    description: "Filtering by topic preserves the same message stream.",
    tags: ["filter", "mcap-output"],
    invocation: {
      args: [
        "filter",
        TEN_MESSAGES,
        "-o",
        "filtered.mcap",
        "-y",
        "example",
        "--output-compression",
        "none",
      ],
    },
    comparison: {
      exitCode: 0,
      stdout: { kind: "text" },
      stderr: { kind: "text", collapseWhitespace: true },
      files: [{ path: "filtered.mcap", comparator: { kind: "mcap", mode: "messages" } }],
    },
  },
  {
    id: "compress-none-output-messages",
    description: "Rewriting an MCAP with no output compression preserves the same message stream.",
    tags: ["compress", "mcap-output"],
    invocation: {
      args: ["compress", TEN_MESSAGES, "-o", "compressed.mcap", "--compression", "none"],
    },
    comparison: {
      exitCode: 0,
      stdout: { kind: "text" },
      stderr: { kind: "text", collapseWhitespace: true },
      files: [{ path: "compressed.mcap", comparator: { kind: "mcap", mode: "messages" } }],
    },
  },
  {
    id: "sort-output-messages",
    description: "Sorting an indexed MCAP preserves the same message stream.",
    tags: ["sort", "mcap-output"],
    invocation: {
      args: ["sort", TEN_MESSAGES, "-o", "sorted.mcap", "--compression", "none"],
    },
    comparison: {
      exitCode: 0,
      stdout: { kind: "text" },
      stderr: { kind: "text", collapseWhitespace: true },
      files: [{ path: "sorted.mcap", comparator: { kind: "mcap", mode: "messages" } }],
    },
  },
  {
    id: "known-difference-completion-command",
    description: "The Go CLI exposes Cobra-generated shell completion; Rust CLI does not yet.",
    tags: ["known-difference", "surface"],
    invocation: { args: ["completion", "bash"] },
    knownDifference: {
      id: "completion-command",
      summary: "Go CLI exposes a completion command; Rust CLI currently has no completion command.",
      reason: "Rust CLI is still being prepared for v1.0 parity.",
      desiredBehavior:
        "Rust CLI should either provide completion behavior compatible with Go CLI or document an intentional replacement.",
      goBehavior: {
        exitCode: 0,
        stdout: { kind: "nonempty" },
      },
      rustBehavior: {
        exitCode: "nonzero",
        stderr: { kind: "contains", value: "unrecognized" },
      },
    },
  },
  {
    id: "known-difference-cat-topics-flag",
    description: "Go CLI supports topic filtering in cat; Rust CLI does not yet.",
    tags: ["known-difference", "cat"],
    invocation: { args: ["cat", ONE_MESSAGE, "--topics", "example"] },
    knownDifference: {
      id: "cat-topics-flag",
      summary: "Go cat supports --topics but Rust cat currently rejects it.",
      reason: "The Rust cat command implementation is still partial.",
      desiredBehavior: "Rust cat should support Go-compatible topic filtering before v1.0.",
      goBehavior: {
        exitCode: 0,
        stdout: { kind: "contains", value: "example" },
      },
      rustBehavior: {
        exitCode: "nonzero",
        stderr: { kind: "contains", value: "unexpected argument" },
      },
    },
  },
  {
    id: "known-difference-cat-stdin",
    description: "Go CLI reads cat input from stdin; Rust CLI currently requires file arguments.",
    tags: ["known-difference", "cat", "stdin"],
    invocation: { args: ["cat"], stdin: { path: ONE_MESSAGE } },
    knownDifference: {
      id: "cat-stdin",
      summary: "Go cat reads stdin when no file is supplied but Rust cat requires file arguments.",
      reason: "The Rust cat command implementation is still partial.",
      desiredBehavior: "Rust cat should support stdin input before v1.0.",
      goBehavior: {
        exitCode: 0,
        stdout: { kind: "contains", value: "example" },
      },
      rustBehavior: {
        exitCode: "nonzero",
        stdout: { kind: "contains", value: "Usage:" },
      },
    },
  },
  {
    id: "known-difference-config-global",
    description: "Go CLI accepts a config file; Rust parses --config but reports it unimplemented.",
    tags: ["known-difference", "global-options"],
    setup: [{ type: "writeText", to: "{caseWorkDir}/config.yaml", contents: "{}\n" }],
    invocation: { args: ["--config", "{caseWorkDir}/config.yaml", "version"] },
    knownDifference: {
      id: "global-config",
      summary: "Go CLI accepts --config but Rust CLI currently bails out as unimplemented.",
      reason: "Global option support is incomplete in the Rust CLI.",
      desiredBehavior:
        "Rust CLI should either implement Go-compatible config handling or remove the flag.",
      goBehavior: {
        exitCode: 0,
        stderr: { kind: "contains", value: "Using config file:" },
      },
      rustBehavior: {
        exitCode: "nonzero",
        stderr: { kind: "contains", value: "--config" },
      },
    },
  },
];

export function validateCases(testCases: readonly CliTestCase[]): string[] {
  const errors: string[] = [];
  const ids = new Set<string>();
  for (const testCase of testCases) {
    if (ids.has(testCase.id)) {
      errors.push(`duplicate case id '${testCase.id}'`);
    }
    ids.add(testCase.id);
    if (testCase.knownDifference != undefined) {
      const known = testCase.knownDifference;
      for (const [field, value] of Object.entries({
        id: known.id,
        summary: known.summary,
        reason: known.reason,
        desiredBehavior: known.desiredBehavior,
      })) {
        if (value.trim().length === 0) {
          errors.push(`known-difference case '${testCase.id}' must provide ${field}`);
        }
      }
      if (known.goBehavior.exitCode == undefined || known.rustBehavior.exitCode == undefined) {
        errors.push(
          `known-difference case '${testCase.id}' must document go and rust exit behavior`,
        );
      }
    }
  }
  return errors;
}
