import colors from "colors";
import { program } from "commander";
import * as Diff from "diff";
import fs from "fs/promises";
import stringify from "json-stringify-pretty-compact";
import path from "path";
import generateTestVariants from "variants/generateTestVariants";

import runners from "./runners";
import { ReadTestRunner, WriteTestRunner } from "./runners/TestRunner";

type TestOptions = {
  dataDir: string;
  runner?: string;
  update: boolean;
  testRegex?: RegExp;
};

function normalizeJson(json: string): string {
  // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment
  const data = JSON.parse(json);
  // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access
  delete data.meta;
  return stringify(data);
}

function bytesToHex(bytes: Uint8Array): string {
  return Array.from(bytes)
    .map((b) => b.toString(16).padStart(2, "0"))
    .join("");
}

function splitAnsiString(s: string, length: number, replace: string): string {
  const regex = RegExp(String.raw`(?:(?:\033\[[0-9;]*m)*.?){1,${length}}`, "g");
  const chunks = s.match(regex);
  const arr: string[] = [];
  (chunks ?? []).forEach((a) => {
    if (!/^(?:\033\[[0-9;]*m)*$/.test(a)) {
      arr.push(a);
    }
  });
  return arr.join(replace);
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
      const outputNorm = normalizeJson(output);
      const expectedNorm = normalizeJson(expectedOutput);
      if (outputNorm !== expectedNorm) {
        console.error(`Error: output did not match expected for ${filePath}:`);
        console.error(Diff.createPatch(expectedOutputPath, expectedNorm, outputNorm));
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

    if (!runner.supportsVariant(variant)) {
      console.log(colors.yellow("unsupported"), filePath);
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
    const outputHex = bytesToHex(output);
    const expectedOutputHex = bytesToHex(expectedOutput as Uint8Array);
    if (outputHex !== expectedOutputHex) {
      console.error(colors.red("fail       "), filePath);
      let colorDiff = "";
      const charDiff = Diff.diffChars(expectedOutputHex, outputHex);
      charDiff.forEach((part) => {
        const color =
          part.added === true ? colors.green : part.removed === true ? colors.red : colors.grey;
        colorDiff += color(part.value);
      });
      console.error(splitAnsiString(splitAnsiString(colorDiff, 8, " "), 81, "\n"));
      console.error();
      hadError = true;
      continue;
    }

    if (!hadError) {
      console.error(colors.green("pass       "), filePath);
    }
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
    options.runner == undefined ? runners : runners.filter((r) => r.name === options.runner);
  if (enabledRunners.length === 0) {
    if (options.runner) {
      throw new Error(`No runner named ${options.runner}`);
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
  .addOption(
    program
      .createOption("--runner <runner>", "test runner to use")
      .choices(runners.map((r) => r.name)),
  )
  .option("--update", "update expected output files", false)
  .option(
    "--test-regex <pattern>",
    "filter tests to run",
    (value: string) => new RegExp(value, "i"),
  )
  .action(main)
  .parse();
