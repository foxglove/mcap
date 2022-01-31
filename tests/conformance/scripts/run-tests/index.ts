import { program } from "commander";
import fs from "fs/promises";
import path from "path";

import runners from "./runners";

async function main(options: { dataDir: string; runner: string; update: boolean }) {
  const runner = runners.find((r) => r.name === options.runner);
  if (!runner) {
    throw new Error(`No runner named ${options.runner}`);
  }
  await fs.mkdir(options.dataDir, { recursive: true });

  for (const fileName of await fs.readdir(options.dataDir)) {
    if (!fileName.endsWith(".mcap")) {
      continue;
    }
    const filePath = path.join(options.dataDir, fileName);

    console.log("running", filePath);
    const output = await runner.run(filePath);

    const expectedOutputPath = path.basename(fileName, ".mcap") + ".txt";
    if (options.update) {
      await fs.writeFile(expectedOutputPath, output.join("\n"));
    } else {
      const expectedOutput = await fs.readFile(expectedOutputPath, { encoding: "utf-8" });
      if (output.join("\n") !== expectedOutput) {
        throw new Error("output did not match expected");
      }
    }
  }
}

program
  .requiredOption("--data-dir <dataDir>", "directory to read test data and output results")
  .addOption(
    program
      .createOption("--runner <runner>", "test runner to use")
      .makeOptionMandatory()
      .choices(runners.map((r) => r.name)),
  )
  .option("--update", "update expected output files", false)
  .action(main)
  .parse();
