import { ULog } from "@foxglove/ulog";
import { FileReader } from "@foxglove/ulog/node";
import { McapWriter } from "@mcap/core";
import { Metadata } from "@mcap/core/src/types";
import { FileHandleWritable } from "@mcap/nodejs";
import { program } from "commander";
import { open } from "fs/promises";

import { convertULogFileToMCAP } from "./convert";

type ConvertOptions = {
  metadata: string[];
  metadataName: string;
  startDate?: bigint;
};

async function convert(inputFilePath: string, outputFilePath: string, options: ConvertOptions) {
  const inputFileHandle = new FileReader(inputFilePath);
  const ulogReader = new ULog(inputFileHandle);

  const outputFileHandle = await open(outputFilePath, "w");
  const mcapFile = new McapWriter({
    writable: new FileHandleWritable(outputFileHandle),
    useStatistics: true,
    useChunks: true,
    useChunkIndex: true,
  });

  let metadata: Metadata | undefined = undefined;
  if (options.metadata != undefined) {
    const metadataFields = new Map<string, string>();
    for (const field of options.metadata) {
      const [key, value] = field.split("=", 2);
      if (key != undefined && value != undefined) {
        metadataFields.set(key, value);
      }
    }
    metadata = { name: options.metadataName, metadata: metadataFields } as Metadata;
  }

  await convertULogFileToMCAP(ulogReader, mcapFile, {
    startTime: options.startDate,
    metadata: metadata ? [metadata] : undefined,
  });
}

function parseMicrosecondsDate(value: string): bigint {
  const date = Date.parse(value);
  if (isNaN(date)) {
    return BigInt(value);
  } else {
    return BigInt(date) * 1000n;
  }
}

program
  .description("Convert a PX4 ULog file to an MCAP file using protobug schemas")
  .argument("<input-file>", "Path to a ULog file")
  .argument("<output-file>", "Path for the output MCAP file")
  .option(
    "-d, --start-date <date>",
    "Adjusted start time for message timestamps, useful since ulog timestamps are stored in time-since-startup (use either timestamp in microseconds or ISO 8601 format)",
    parseMicrosecondsDate,
  )
  .option("-m, --metadata <key=value...>", "Additional file-level metadata")
  .option(
    "-n, --metadata-name <name>",
    "Name for metadata group, if adding metadata",
    "ulog-metadata",
  )
  .action(async (inputFilePath: string, outputFilePath: string, options: ConvertOptions) => {
    await convert(inputFilePath, outputFilePath, options);
  });
program.parse();
