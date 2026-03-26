#!/usr/bin/env -S npx tsx
/**
 * MCAP read benchmark for TypeScript.
 */

import { open, readFile, stat } from "fs/promises";

import {
  McapIndexedReader,
  McapStreamReader,
} from "../../typescript/core/src/index.ts";
import { FileHandleReadable } from "../../typescript/nodejs/src/index.ts";
import { loadDecompressHandlers } from "../../typescript/support/src/decompressHandlers.ts";

async function main(): Promise<number> {
  if (process.argv.length < 3 || process.argv.length > 7) {
    process.stderr.write(
      `Usage: ${process.argv[1]} <input_file> [mode] [num_messages] [payload_size] [filter]\n` +
        `  filter: topic | timerange | topic_timerange\n`,
    );
    return 1;
  }

  const filename = process.argv[2]!;
  const mode = process.argv[3] ?? "unknown";
  const numMessagesStr = process.argv[4] ?? "0";
  const payloadSizeStr = process.argv[5] ?? "0";
  const filter = process.argv[6];

  const decompressHandlers = await loadDecompressHandlers();

  let msgCount = 0;

  // Time file open + reader creation + message iteration
  const tStart = process.hrtime.bigint();

  if (filter != null && filter !== "") {
    // Filtered reads: use McapIndexedReader to leverage the chunk index
    const fileHandle = await open(filename, "r");
    const reader = await McapIndexedReader.Initialize({
      readable: new FileHandleReadable(fileHandle),
      decompressHandlers,
    });

    const readArgs: {
      topics?: string[];
      startTime?: bigint;
      endTime?: bigint;
    } = {};
    if (filter === "topic") {
      readArgs.topics = ["/imu"];
    } else if (filter === "timerange") {
      readArgs.startTime = 3000000000n;
      readArgs.endTime = 5000000000n;
    } else if (filter === "topic_timerange") {
      readArgs.topics = ["/lidar"];
      readArgs.startTime = 4000000000n;
      readArgs.endTime = 6000000000n;
    }

    for await (const message of reader.readMessages(readArgs)) {
      if (message.data.length === 0) {
        process.stderr.write("Empty message\n");
      }
      msgCount++;
    }

    await fileHandle.close();
  } else {
    // Unfiltered reads: use McapStreamReader to handle all file types
    const data = await readFile(filename);
    const reader = new McapStreamReader({ decompressHandlers });
    reader.append(data);
    for (;;) {
      const record = reader.nextRecord();
      if (record == null) {
        break;
      }
      if (record.type === "Message") {
        if (record.data.length === 0) {
          process.stderr.write("Empty message\n");
        }
        msgCount++;
      }
    }
  }

  const tEnd = process.hrtime.bigint();

  const elapsedNs = tEnd - tStart;
  const wallSec = Number(elapsedNs) / 1e9;
  const fileSize = (await stat(filename)).size;

  const peakRssKb = process.resourceUsage().maxRSS;

  // TSV output: op lang mode num_msgs payload_size file_size elapsed_ns wall_sec peak_rss_kb
  process.stdout.write(
    `read\ttypescript\t${mode}\t${numMessagesStr}\t${payloadSizeStr}\t${fileSize}\t${elapsedNs}\t${wallSec.toFixed(
      6,
    )}\t${peakRssKb}\n`,
  );

  void msgCount;

  return 0;
}

main().then((code) => process.exit(code));
