import { McapStreamReader } from "@mcap/core";
import type { TypedMcapRecord } from "@mcap/core";
import colors from "colors";
import { program } from "commander";
import * as Diff from "diff";
import fs from "node:fs/promises";
import stableStringify from "json-stable-stringify";
import { chunk } from "lodash";
import path from "node:path";

import runners from "./runners/index.ts";
import {
  IndexedReadTestRunner,
  StreamedReadTestRunner,
  WriteTestRunner,
} from "./runners/TestRunner.ts";
import { toSerializableMcapRecord } from "./toSerializableMcapRecord.ts";
import {
  type IndexedReadTestResult,
  type SerializableMcapRecord,
  type StreamedReadTestResult,
  type TestCase,
} from "./types.ts";
import { splitMcapRecords } from "../../util/splitMcapRecords.ts";
import generateTestVariants from "../../variants/generateTestVariants.ts";

type TestOptions = {
  dataDir: string;
  runner?: string;
  update: boolean;
  testRegex?: RegExp;
};

function asNormalizedJSON(data: object): string {
  return stableStringify(data, { space: 2 }) + "\n";
}

function spaceHexString(s: string): string {
  return [[s.substring(0, 2)], ...chunk(s.substring(2), 8)].map((p) => p.join("")).join(" ");
}

async function runReaderTest(
  runner: StreamedReadTestRunner | IndexedReadTestRunner,
  options: TestOptions,
): Promise<{ foundAnyTests: boolean; hadError: boolean }> {
  let foundAnyTests = false;
  let hadError = false;
  console.log("running", runner.name);
  for (const variant of generateTestVariants()) {
    if (options.testRegex && !options.testRegex.test(variant.name)) {
      continue;
    }
    foundAnyTests = true;
    const filePath = path.join(options.dataDir, variant.baseName, `${variant.name}.mcap`);

    if (runner.supportsVariant(variant)) {
      console.log("  testing", filePath);
    } else {
      console.log("  not supported", filePath);
      continue;
    }

    let output: StreamedReadTestResult | IndexedReadTestResult;
    try {
      output = await runner.runReadTest(filePath);
    } catch (error) {
      console.error(error);
      hadError = true;
      continue;
    }
    const expectedOutputPath = filePath.replace(/\.mcap$/, ".json");
    if (options.update) {
      const stringifyCompact = (await import("json-stringify-pretty-compact")).default;
      if (runner instanceof StreamedReadTestRunner) {
        const testCase: TestCase = {
          records: (output as StreamedReadTestResult).records,
          meta: { variant: { features: Array.from(variant.features) } },
        };
        await fs.writeFile(expectedOutputPath, stringifyCompact(testCase).trim() + "\n");
      }
    } else {
      const testCase = await fs
        .readFile(expectedOutputPath, { encoding: "utf-8" })
        .catch(() => undefined);
      if (testCase == undefined) {
        console.error(`Error: missing test case file ${expectedOutputPath}`);
        hadError = true;
        continue;
      }
      const expectedTestResult: typeof output = runner.expectedResult(
        JSON.parse(testCase) as TestCase,
      );

      const outputNorm = asNormalizedJSON(output);
      const expectedNorm = asNormalizedJSON(expectedTestResult);
      if (outputNorm !== expectedNorm) {
        console.error(`Error: output did not match expected for ${filePath}:`);
        console.error(
          Diff.createPatch(expectedOutputPath, expectedNorm, outputNorm, "expected", "actual"),
        );
        hadError = true;
        continue;
      }
    }
  }

  return { foundAnyTests, hadError };
}

async function runWriterTest(
  runner: WriteTestRunner,
  options: TestOptions,
): Promise<{ foundAnyTests: boolean; hadError: boolean }> {
  let foundAnyTests = false;
  let hadError = false;
  console.log("running", runner.name);
  for (const variant of generateTestVariants()) {
    if (options.testRegex && !options.testRegex.test(variant.name)) {
      continue;
    }
    foundAnyTests = true;
    const testCasePath = path.join(options.dataDir, variant.baseName, `${variant.name}.json`);
    const expectedOutputPath = testCasePath.replace(/\.json$/, ".mcap");
    const basePath = path.basename(testCasePath);

    if (!runner.supportsVariant(variant)) {
      console.log(colors.yellow("unsupported"), basePath);
      continue;
    }

    let output: Uint8Array;
    try {
      output = await runner.runWriteTest(testCasePath, variant);
    } catch (error) {
      console.error(error);
      hadError = true;
      continue;
    }

    const expectedOutput = await fs.readFile(expectedOutputPath).catch(() => undefined);
    if (expectedOutput == undefined) {
      console.error(`Error: missing expected output file ${expectedOutputPath}`);
      hadError = true;
      continue;
    }
    const expectedParts = splitMcapRecords(new Uint8Array(expectedOutput))
      .map(spaceHexString)
      .join("\n");
    const outputParts = splitMcapRecords(output).map(spaceHexString).join("\n");
    if (!expectedOutput.equals(output)) {
      console.error(colors.red("fail       "), path.basename(testCasePath));
      try {
        const testCase: TestCase = JSON.parse(
          await fs.readFile(testCasePath, { encoding: "utf-8" }),
        ) as TestCase;
        const reader = new McapStreamReader({ validateCrcs: true });
        reader.append(output);
        const records: TypedMcapRecord[] = [];
        for (let record; (record = reader.nextRecord()); ) {
          records.push(record);
        }
        const actualRecords: SerializableMcapRecord[] = records
          .map(toSerializableMcapRecord)
          .filter((r) => r.type !== "MessageIndex");
        const expectedRecords: SerializableMcapRecord[] = testCase.records;

        const outputNorm = asNormalizedJSON(actualRecords);
        const expectedNorm = asNormalizedJSON(expectedRecords);
        if (outputNorm !== expectedNorm) {
          console.error(Diff.createPatch(basePath, expectedNorm, outputNorm, "expected", "output"));
        }
      } catch (err) {
        console.error("Invalid mcap:", err);
      }
      const diff = Diff.diffLines(expectedParts, outputParts);
      diff.forEach((part) => {
        if (part.added === true) {
          console.error(colors.green(part.value.trimEnd()));
        } else if (part.removed === true) {
          console.error(colors.red(part.value.trimEnd()));
        } else {
          console.error(part.value.trim());
        }
      });
      console.error();
      hadError = true;
      continue;
    }

    console.error(colors.green("pass       "), basePath);
  }

  return { foundAnyTests, hadError };
}

async function main(options: TestOptions) {
  if (options.update && !options.runner) {
    throw new Error(
      "A test runner must be specified using --runner when updating expected outputs",
    );
  }

  const enabledRunners =
    options.runner == undefined ? runners : runners.filter((r) => r.name.includes(options.runner!));
  if (enabledRunners.length === 0) {
    if (options.runner) {
      throw new Error(
        `No runner named ${options.runner}. Allowed choices are ${runners
          .map((r) => r.name)
          .join(", ")}`,
      );
    } else {
      throw new Error("No runners available");
    }
  }
  await fs.mkdir(options.dataDir, { recursive: true });

  let hadError = false;
  let foundAnyTests = false;
  for (const runner of enabledRunners) {
    if (runner instanceof StreamedReadTestRunner || runner instanceof IndexedReadTestRunner) {
      const { hadError: newHadError, foundAnyTests: newFoundAnyTests } = await runReaderTest(
        runner,
        options,
      );
      hadError ||= newHadError;
      foundAnyTests ||= newFoundAnyTests;
    } else if (runner instanceof WriteTestRunner) {
      const { hadError: newHadError, foundAnyTests: newFoundAnyTests } = await runWriterTest(
        runner,
        options,
      );

      hadError ||= newHadError;
      foundAnyTests ||= newFoundAnyTests;
    }
  }

  if (!foundAnyTests) {
    console.error("No tests found");
    hadError = true;
  }

  if (hadError) {
    process.exit(1);
  }
}

program
  .requiredOption("--data-dir <dataDir>", "directory to read test data and output results")
  .addOption(program.createOption("--runner <runner>", "test runner to use"))
  .option("--update", "update expected output files", false)
  .option(
    "--test-regex <pattern>",
    "filter tests to run",
    (value: string) => new RegExp(value, "i"),
  )
  .action(main)
  .parse();
