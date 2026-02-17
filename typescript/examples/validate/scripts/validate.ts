import { parse as parseMessageDefinition } from "@foxglove/rosmsg";
import { LazyMessageReader as ROS1LazyMessageReader } from "@foxglove/rosmsg-serialization";
import { MessageReader as ROS2MessageReader } from "@foxglove/rosmsg2-serialization";
import { hasMcapPrefix, MCAP_MAGIC, McapIndexedReader, McapStreamReader } from "@mcap/core";
import type { Channel, TypedMcapRecord, TypedMcapRecords } from "@mcap/core";
import { FileHandleReadable } from "@mcap/nodejs";
import { loadDecompressHandlers } from "@mcap/support";
import { program } from "commander";
import { createReadStream } from "node:fs";
import fs from "node:fs/promises";
import { isEqual } from "lodash-es";
import { performance } from "node:perf_hooks";
import * as protobufjs from "protobufjs";
import { FileDescriptorSet } from "protobufjs/ext/descriptor/index.js";

function log(...data: unknown[]) {
  console.log(...data);
}

function formatBytes(totalBytes: number) {
  const units = ["Bytes", "kiB", "MiB", "GiB", "TiB"];
  let bytes = totalBytes;
  let unit = 0;
  while (unit + 1 < units.length && bytes >= 1024) {
    bytes /= 1024;
    unit++;
  }
  return `${bytes.toFixed(2)}${units[unit]!}`;
}

async function readStream(
  filePath: string,
  reader: McapStreamReader,
  processRecord: (record: TypedMcapRecord) => void,
) {
  const startTime = performance.now();
  let readBytes = 0n;

  let lastRecordType: TypedMcapRecord["type"] | undefined;
  await new Promise<void>((resolve, reject) => {
    const stream = createReadStream(filePath);
    stream.on("data", (data) => {
      try {
        if (typeof data === "string") {
          throw new Error("expected buffer");
        }
        readBytes += BigInt(data.byteLength);
        reader.append(new Uint8Array(data));
        for (let record; (record = reader.nextRecord()); ) {
          lastRecordType = record.type;
          processRecord(record);
        }
      } catch (error) {
        reject(error);
        stream.close();
      }
    });
    stream.on("error", (error) => {
      reject(error);
    });
    stream.on("close", () => {
      resolve();
    });
  });

  if (!reader.done()) {
    throw new Error(
      `File read incomplete; ${reader.bytesRemaining()} bytes remain after parsing` +
        (lastRecordType != undefined ? ` (last record was ${lastRecordType})` : ""),
    );
  }

  const durationMs = performance.now() - startTime;
  log(
    `Read ${formatBytes(Number(readBytes))} in ${durationMs.toFixed(2)}ms (${formatBytes(
      Number(readBytes) / (durationMs / 1000),
    )}/sec)`,
  );
}

async function validate(
  filePath: string,
  { deserialize, dump, stream }: { deserialize: boolean; dump: boolean; stream: boolean },
) {
  const decompressHandlers = await loadDecompressHandlers();

  const recordCounts = new Map<TypedMcapRecord["type"], number>();
  const schemasById = new Map<number, TypedMcapRecords["Schema"]>();
  const channelInfoById = new Map<
    number,
    {
      info: Channel;
      messageDeserializer?: (data: ArrayBufferView) => unknown;
    }
  >();

  function processRecord(record: TypedMcapRecord) {
    recordCounts.set(record.type, (recordCounts.get(record.type) ?? 0) + 1);

    switch (record.type) {
      default:
        break;

      case "Schema": {
        const existingSchema = schemasById.get(record.id);
        if (existingSchema) {
          if (!isEqual(existingSchema, record)) {
            throw new Error(`differing schemas for id ${record.id}`);
          }
          break;
        }
        schemasById.set(record.id, record);
        break;
      }

      case "Channel": {
        const existingInfo = channelInfoById.get(record.id);
        if (existingInfo) {
          if (!isEqual(existingInfo.info, record)) {
            throw new Error(`differing channels for id ${record.id}`);
          }
          break;
        }
        if (record.schemaId === 0) {
          throw new Error(
            `Channel ${record.id} has no schema; channels without schemas are not supported`,
          );
        }
        const schema = schemasById.get(record.schemaId);
        if (!schema) {
          throw new Error(`Missing schema ${record.schemaId} for channel ${record.id}`);
        }
        let messageDeserializer: (data: ArrayBufferView) => unknown;
        if (schema.encoding === "ros1msg" && record.messageEncoding === "ros1") {
          const reader = new ROS1LazyMessageReader(
            parseMessageDefinition(new TextDecoder().decode(schema.data)),
          );
          messageDeserializer = (data) => {
            const size = reader.size(data);
            if (size !== data.byteLength) {
              throw new Error(`Message size ${size} should match buffer length ${data.byteLength}`);
            }
            return reader.readMessage(data).toJSON();
          };
        } else if (schema.encoding === "ros2msg" && record.messageEncoding === "cdr") {
          const reader = new ROS2MessageReader(
            parseMessageDefinition(new TextDecoder().decode(schema.data), {
              ros2: true,
            }),
          );
          messageDeserializer = (data) => reader.readMessage(data);
        } else if (schema.encoding === "protobuf" && record.messageEncoding === "protobuf") {
          const root = protobufjs.Root.fromDescriptor(FileDescriptorSet.decode(schema.data));
          const type = root.lookupType(schema.name);

          messageDeserializer = (data) =>
            type.decode(new Uint8Array(data.buffer, data.byteOffset, data.byteLength));
        } else if (record.messageEncoding === "json") {
          const textDecoder = new TextDecoder();
          messageDeserializer = (data) => JSON.parse(textDecoder.decode(data));
        } else {
          throw new Error(
            `unsupported message encoding ${record.messageEncoding} with schema encoding ${schema.encoding}`,
          );
        }
        channelInfoById.set(record.id, { info: record, messageDeserializer });
        break;
      }

      case "Message": {
        const channelInfo = channelInfoById.get(record.channelId);
        if (!channelInfo) {
          throw new Error(`message for channel ${record.channelId} with no prior channel info`);
        }
        if (deserialize) {
          if (channelInfo.messageDeserializer == undefined) {
            throw new Error(
              `No deserializer available for channel id: ${channelInfo.info.id} ${channelInfo.info.messageEncoding}`,
            );
          }
          const message = channelInfo.messageDeserializer(record.data);
          if (dump) {
            log(message);
          }
        }
        break;
      }
    }
  }

  log("Reading", filePath);

  {
    const handle = await fs.open(filePath, "r");
    try {
      const buffer = new Uint8Array(MCAP_MAGIC.length);
      const readResult = await handle.read({
        buffer,
        offset: 0,
        length: MCAP_MAGIC.length,
      });
      const isValidMcap = hasMcapPrefix(new DataView(buffer.buffer, 0, readResult.bytesRead));
      if (!isValidMcap) {
        throw new Error(
          `Not a valid MCAP file: prefix not detected in <${Array.from(buffer)
            .map((val) => val.toString(16).padStart(2, "0"))
            .join(" ")}>`,
        );
      }
      log("MCAP prefix detected");
    } finally {
      await handle.close();
    }
  }

  let processed = false;
  if (!stream) {
    const handle = await fs.open(filePath, "r");
    try {
      const reader = await McapIndexedReader.Initialize({
        readable: new FileHandleReadable(handle),
        decompressHandlers,
      });
      for (const record of reader.schemasById.values()) {
        processRecord(record);
      }
      for (const record of reader.channelsById.values()) {
        processRecord(record);
      }
      for await (const record of reader.readMessages()) {
        processRecord(record);
      }
      processed = true;
    } catch (error) {
      log("Unable to read file as indexed; falling back to streaming:", error);
    } finally {
      await handle.close();
    }
  }

  if (!processed) {
    await readStream(
      filePath,
      new McapStreamReader({
        includeChunks: true,
        decompressHandlers,
        validateCrcs: true,
      }),
      processRecord,
    );
  }

  log("Record counts:");
  for (const [type, count] of recordCounts) {
    log(`  ${count.toFixed().padStart(6, " ")} ${type}`);
  }
}

program
  .argument("<file...>", "path to MCAP file(s)")
  .option("--deserialize", "deserialize message contents", false)
  .option("--dump", "dump message contents to stdout", false)
  .option("--stream", "if a file is indexed, ignore the index and read it as a stream", false)
  .action(
    async (files: string[], options: { deserialize: boolean; dump: boolean; stream: boolean }) => {
      for (const file of files) {
        await validate(file, options).catch(console.error);
      }
    },
  )
  .parse();
