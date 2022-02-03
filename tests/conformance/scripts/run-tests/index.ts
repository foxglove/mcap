import { program } from "commander";
import * as Diff from "diff";
import fs from "fs/promises";
import path from "path";
import listDirRecursive from "scripts/util/listDirRecursive";

import runners from "./runners";

async function main(options: { dataDir: string; runner?: string; update: boolean }) {
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

  for (const runner of enabledRunners) {
    console.log("running", runner.name);
    for await (const fileName of listDirRecursive(options.dataDir)) {
      if (!fileName.endsWith(".mcap")) {
        continue;
      }
      const filePath = path.join(options.dataDir, fileName);

      console.log("  testing", filePath);
      const outputLines = await runner.run(filePath);
      const output = outputLines.join("\n") + "\n";

      const expectedOutputPath = filePath.replace(/\.mcap$/, ".expected.txt");
      if (options.update) {
        await fs.writeFile(expectedOutputPath, output);
      } else {
        const expectedOutput = await fs
          .readFile(expectedOutputPath, { encoding: "utf-8" })
          .catch(() => {
            throw new Error(`Missing expected output file ${expectedOutputPath}`);
          });
        if (output !== expectedOutput) {
          console.error(Diff.createPatch(expectedOutputPath, expectedOutput, output));
          throw new Error(`output did not match expected for ${filePath}`);
        }
      }
    }
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
  .action(main)
  .parse();
