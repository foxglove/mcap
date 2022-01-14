import { parse as parseMessageDefinition, RosMsgDefinition } from "@foxglove/rosmsg";
import { LazyMessageReader as ROS1LazyMessageReader } from "@foxglove/rosmsg-serialization";
import { MessageReader as ROS2MessageReader } from "@foxglove/rosmsg2-serialization";
import { program } from "commander";
import { createReadStream } from "fs";
import fs from "fs/promises";
import { isEqual } from "lodash";
import { performance } from "perf_hooks";
import decompressLZ4 from "wasm-lz4";

import detectVersion, { DETECT_VERSION_BYTES_REQUIRED, McapVersion } from "../src/detectVersion";
import McapPre0LatestStreamReader from "../src/latest/McapPre0LatestStreamReader";
import { McapRecord, ChannelInfo, McapLatestStreamReader } from "../src/latest/types";
import Mcap0StreamReader from "../src/v0/Mcap0StreamReader";

function log(...data: unknown[]) {
  console.log(...data);
}

function formatBytes(totalBytes: number) {
  const units = ["B", "kiB", "MiB", "GiB", "TiB"];
  let bytes = totalBytes;
  let unit = 0;
  while (unit + 1 < units.length && bytes >= 1024) {
    bytes /= 1024;
    unit++;
  }
  return `${bytes.toFixed(2)}${units[unit]!}`;
}

async function validate(
  filePath: string,
  { deserialize, dump }: { deserialize: boolean; dump: boolean },
) {
  await decompressLZ4.isLoaded;

  const recordCounts = new Map<McapRecord["type"], number>();
  const channelInfoById = new Map<
    number,
    {
      info: ChannelInfo;
      messageDeserializer: ROS2MessageReader | ROS1LazyMessageReader;
      parsedDefinitions: RosMsgDefinition[];
    }
  >();

  function processRecord(record: McapRecord) {
    recordCounts.set(record.type, (recordCounts.get(record.type) ?? 0) + 1);

    switch (record.type) {
      default:
        break;

      case "ChannelInfo": {
        const existingInfo = channelInfoById.get(record.channelId);
        if (existingInfo) {
          if (!isEqual(existingInfo.info, record)) {
            throw new Error(`differing channel infos for ${record.channelId}`);
          }
          break;
        }
        let parsedDefinitions;
        let messageDeserializer;
        if (record.encoding === "ros1") {
          parsedDefinitions = parseMessageDefinition(record.schema);
          messageDeserializer = new ROS1LazyMessageReader(parsedDefinitions);
        } else if (record.encoding === "ros2") {
          parsedDefinitions = parseMessageDefinition(record.schema, {
            ros2: true,
          });
          messageDeserializer = new ROS2MessageReader(parsedDefinitions);
        } else {
          throw new Error(`unsupported encoding ${record.encoding}`);
        }
        channelInfoById.set(record.channelId, {
          info: record,
          messageDeserializer,
          parsedDefinitions,
        });
        break;
      }

      case "Message": {
        const channelInfo = channelInfoById.get(record.channelInfo.channelId);
        if (!channelInfo) {
          throw new Error(
            `message for channel ${record.channelInfo.channelId} with no prior channel info`,
          );
        }
        if (deserialize) {
          let message: unknown;
          if (channelInfo.messageDeserializer instanceof ROS1LazyMessageReader) {
            const size = channelInfo.messageDeserializer.size(new DataView(record.messageData));
            if (size !== record.messageData.byteLength) {
              throw new Error(
                `Message size ${size} should match buffer length ${record.messageData.byteLength}`,
              );
            }
            message = channelInfo.messageDeserializer
              .readMessage(new DataView(record.messageData))
              .toJSON();
          } else {
            message = channelInfo.messageDeserializer.readMessage(new DataView(record.messageData));
          }
          if (dump) {
            log(message);
          }
        }
        break;
      }
    }
  }

  log("Reading", filePath);

  let mcapVersion: McapVersion | undefined;
  const handle = await fs.open(filePath, "r");
  try {
    const buffer = new Uint8Array(DETECT_VERSION_BYTES_REQUIRED);
    const readResult = await handle.read({
      buffer,
      offset: 0,
      length: DETECT_VERSION_BYTES_REQUIRED,
    });
    mcapVersion = detectVersion(new DataView(buffer.buffer, 0, readResult.bytesRead));
    if (mcapVersion == undefined) {
      throw new Error(
        `Not a valid MCAP file: unable to detect version with file header ${Array.from(buffer)
          .map((val) => val.toString(16).padStart(2, "0"))
          .join(" ")}`,
      );
    }
    log("Detected MCAP version:", mcapVersion);
  } finally {
    await handle.close();
  }

  let reader: McapLatestStreamReader;
  switch (mcapVersion) {
    case "pre0":
      reader = new McapPre0LatestStreamReader({
        includeChunks: true,
        decompressHandlers: {
          lz4: (buffer, decompressedSize) => decompressLZ4(buffer, Number(decompressedSize)),
        },
      });
      break;

    case "0":
      reader = new Mcap0StreamReader({
        includeChunks: true,
        decompressHandlers: {
          lz4: (buffer, decompressedSize) => decompressLZ4(buffer, Number(decompressedSize)),
        },
        validateCrcs: true,
      });
      break;
  }

  const startTime = performance.now();
  let readBytes = 0n;

  await new Promise<void>((resolve, reject) => {
    const stream = createReadStream(filePath);
    stream.on("data", (data) => {
      try {
        if (typeof data === "string") {
          throw new Error("expected buffer");
        }
        readBytes += BigInt(data.byteLength);
        reader.append(data);
        for (let record; (record = reader.nextRecord()); ) {
          processRecord(record);
        }
      } catch (error) {
        reject(error);
        stream.close();
      }
    });
    stream.on("error", (error) => reject(error));
    stream.on("close", () => resolve());
  });

  if (!reader.done()) {
    throw new Error(`File read incomplete; ${reader.bytesRemaining()} bytes remain after parsing`);
  }

  const durationMs = performance.now() - startTime;
  log(
    `Read ${formatBytes(Number(readBytes))} in ${durationMs.toFixed(2)}ms (${formatBytes(
      Number(readBytes) / (durationMs / 1000),
    )}/sec)`,
  );
  log("Record counts:");
  for (const [type, count] of recordCounts) {
    log(`  ${count.toFixed().padStart(6, " ")} ${type}`);
  }
}

program
  .argument("<file...>", "path to mcap file(s)")
  .option("--deserialize", "deserialize message contents", false)
  .option("--dump", "dump message contents to stdout", false)
  .action(async (files: string[], options: { deserialize: boolean; dump: boolean }) => {
    for (const file of files) {
      await validate(file, options).catch(console.error);
    }
  })
  .parse();
