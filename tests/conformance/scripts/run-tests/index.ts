import { Mcap0StreamReader, Mcap0Types } from "@mcap/core";
import colors from "colors";
import { program } from "commander";
import * as Diff from "diff";
import fs from "fs/promises";
import stableStringify from "json-stable-stringify";
import { chunk } from "lodash";
import path from "path";

import { splitMcapRecords } from "../../util/splitMcapRecords";
import generateTestVariants from "../../variants/generateTestVariants";
import { TestFeatures } from "../../variants/types";
import runners from "./runners";
import { ReadTestRunner, WriteTestRunner } from "./runners/TestRunner";
import { stringifyRecords } from "./runners/stringifyRecords";

type TestOptions = {
  dataDir: string;
  runner?: string;
  update: boolean;
  testRegex?: RegExp;
};

type TestJson = {
  records: { type: string }[];
  meta?: { variant: { features: TestFeatures[] } };
};

/**
 * @param ignoreDataEnd Used to exempt indexed readers from outputting a dataSectionCrc.
 */
function normalizeJson(json: string, { ignoreDataEnd }: { ignoreDataEnd: boolean }): string {
  const data = JSON.parse(json) as TestJson;
  delete data.meta;
  if (ignoreDataEnd) {
    data.records = data.records.filter((record) => record.type !== "DataEnd");
  }
  return stableStringify(data, { space: 2 }) + "\n";
}

function spaceHexString(s: string): string {
  return [[s.substring(0, 2)], ...chunk(s.substring(2), 8)].map((p) => p.join("")).join(" ");
}

async function runReaderTest(
  runner: ReadTestRunner,
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

    let output: string;
    try {
      output = await runner.runReadTest(filePath, variant);
    } catch (error) {
      console.error(error);
      hadError = true;
      continue;
    }
    const expectedOutputPath = filePath.replace(/\.mcap$/, ".json");
    if (options.update) {
      await fs.writeFile(expectedOutputPath, output);
    } else {
      const expectedOutput = await fs
        .readFile(expectedOutputPath, { encoding: "utf-8" })
        .catch(() => undefined);
      if (expectedOutput == undefined) {
        console.error(`Error: missing expected output file ${expectedOutputPath}`);
        hadError = true;
        continue;
      }
      const outputNorm = normalizeJson(output, { ignoreDataEnd: !runner.readsDataEnd });
      const expectedNorm = normalizeJson(expectedOutput, { ignoreDataEnd: !runner.readsDataEnd });
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
    const filePath = path.join(options.dataDir, variant.baseName, `${variant.name}.json`);
    const basePath = path.basename(filePath);

    if (!runner.supportsVariant(variant)) {
      console.log(colors.yellow("unsupported"), basePath);
      continue;
    }

    let output: Uint8Array;
    try {
      output = await runner.runWriteTest(filePath, variant);
    } catch (error) {
      console.error(error);
      hadError = true;
      continue;
    }

    const expectedOutputPath = filePath.replace(/\.json$/, ".mcap");
    const expectedOutput = await fs.readFile(expectedOutputPath).catch(() => undefined);
    if (expectedOutput == undefined) {
      console.error(`Error: missing expected output file ${expectedOutputPath}`);
      hadError = true;
      continue;
    }
    const expectedParts = splitMcapRecords(expectedOutput).map(spaceHexString).join("\n");
    const outputParts = splitMcapRecords(output).map(spaceHexString).join("\n");
    if (!expectedOutput.equals(output)) {
      console.error(colors.red("fail       "), path.basename(filePath));
      try {
        const expectedOutputJson = await fs.readFile(filePath, { encoding: "utf-8" });
        const reader = new Mcap0StreamReader({ validateCrcs: true });
        reader.append(output);
        const records: Mcap0Types.TypedMcapRecord[] = [];
        for (let record; (record = reader.nextRecord()); ) {
          records.push(record);
        }

        const actualOutputJson = stringifyRecords(records, variant);
        const outputNorm = normalizeJson(actualOutputJson, { ignoreDataEnd: false });
        const expectedNorm = normalizeJson(expectedOutputJson, { ignoreDataEnd: false });
        if (outputNorm !== expectedNorm) {
          console.error(
            Diff.createPatch(
              path.basename(filePath),
              expectedNorm,
              outputNorm,
              "expected",
              "output",
            ),
          );
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
    if (runner instanceof ReadTestRunner) {
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
