import {
  detectVersion,
  DETECT_VERSION_BYTES_REQUIRED,
  McapVersion,
  Mcap0IndexedReader,
  Mcap0StreamReader,
  Mcap0Types,
} from "@foxglove/mcap";
import { parse as parseMessageDefinition } from "@foxglove/rosmsg";
import { LazyMessageReader as ROS1LazyMessageReader } from "@foxglove/rosmsg-serialization";
import { MessageReader as ROS2MessageReader } from "@foxglove/rosmsg2-serialization";
import { program } from "commander";
import { createReadStream } from "fs";
import fs from "fs/promises";
import { isEqual } from "lodash";
import { performance } from "perf_hooks";
import protobufjs from "protobufjs";
import { FileDescriptorSet } from "protobufjs/ext/descriptor";
import decompressLZ4 from "wasm-lz4";

type Channel = Mcap0Types.Channel;
type DecompressHandlers = Mcap0Types.DecompressHandlers;
type McapStreamReader = Mcap0Types.McapStreamReader;
type TypedMcapRecord = Mcap0Types.TypedMcapRecord;

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
}

async function validate(
  filePath: string,
  { deserialize, dump, stream }: { deserialize: boolean; dump: boolean; stream: boolean },
) {
  await decompressLZ4.isLoaded;
  const decompressHandlers: DecompressHandlers = {
    lz4: (buffer, decompressedSize) => decompressLZ4(buffer, Number(decompressedSize)),
  };

  const recordCounts = new Map<TypedMcapRecord["type"], number>();
  const schemasById = new Map<number, Mcap0Types.TypedMcapRecords["Schema"]>();
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
        if (record.messageEncoding === "ros1") {
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
        } else if (record.messageEncoding === "ros2") {
          const reader = new ROS2MessageReader(
            parseMessageDefinition(new TextDecoder().decode(schema.data), {
              ros2: true,
            }),
          );
          messageDeserializer = (data) => reader.readMessage(data);
        } else if (record.messageEncoding === "protobuf") {
          const root = protobufjs.Root.fromDescriptor(FileDescriptorSet.decode(schema.data));
          const type = root.lookupType(schema.name);

          messageDeserializer = (data) =>
            type.decode(new Uint8Array(data.buffer, data.byteOffset, data.byteLength));
        } else if (record.messageEncoding === "json") {
          const textDecoder = new TextDecoder();
          messageDeserializer = (data) => JSON.parse(textDecoder.decode(data));
        } else {
          throw new Error(`unsupported encoding ${record.messageEncoding}`);
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

  let mcapVersion: McapVersion | undefined;
  {
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
  }

  switch (mcapVersion) {
    case "pre0":
      throw new Error("MCAP pre-v0 files are not supported");
      break;

    case "0":
      if (!stream) {
        const handle = await fs.open(filePath, "r");
        try {
          let buffer = new ArrayBuffer(4096);
          const reader = await Mcap0IndexedReader.Initialize({
            readable: {
              size: async () => BigInt((await handle.stat()).size),
              read: async (offset, length) => {
                if (offset > Number.MAX_SAFE_INTEGER || length > Number.MAX_SAFE_INTEGER) {
                  throw new Error(`Read too large: offset ${offset}, length ${length}`);
                }
                if (length > buffer.byteLength) {
                  buffer = new ArrayBuffer(Number(length * 2n));
                }
                const result = await handle.read({
                  buffer: new DataView(buffer, 0, Number(length)),
                  position: Number(offset),
                });
                if (result.bytesRead !== Number(length)) {
                  throw new Error(
                    `Read only ${result.bytesRead} bytes from offset ${offset}, expected ${length}`,
                  );
                }
                return new Uint8Array(
                  result.buffer.buffer,
                  result.buffer.byteOffset,
                  result.bytesRead,
                );
              },
            },
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
          break;
        } catch (error) {
          log("Unable to read file as indexed; falling back to streaming:", error);
        } finally {
          await handle.close();
        }
      }
      await readStream(
        filePath,
        new Mcap0StreamReader({
          includeChunks: true,
          decompressHandlers,
          validateCrcs: true,
        }),
        processRecord,
      );
      break;
  }

  log("Record counts:");
  for (const [type, count] of recordCounts) {
    log(`  ${count.toFixed().padStart(6, " ")} ${type}`);
  }
}

program
  .argument("<file...>", "path to mcap file(s)")
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
