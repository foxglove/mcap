import {
  hasMcapPrefix,
  McapConstants,
  McapIndexedReader,
  McapStreamReader,
  McapTypes,
} from "@mcap/core";
import { FileHandleReadable } from "@mcap/nodejs";
import { loadDecompressHandlers, parseChannel, ParsedChannel } from "@mcap/support";
import { program } from "commander";
import { createReadStream } from "fs";
import fs from "fs/promises";
import { isEqual } from "lodash";
import { performance } from "perf_hooks";

type TypedMcapRecord = McapTypes.TypedMcapRecord;

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
        reader.append(data);
        for (let record; (record = reader.nextRecord()); ) {
          lastRecordType = record.type;
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
  const schemasById = new Map<number, McapTypes.TypedMcapRecords["Schema"]>();
  const channelInfoById = new Map<
    number,
    { info: McapTypes.Channel; parsedChannel: ParsedChannel }
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
        let schema: McapTypes.Schema | undefined;
        if (record.schemaId !== 0) {
          schema = schemasById.get(record.schemaId);
          if (!schema) {
            throw new Error(`Missing schema ${record.schemaId} for channel ${record.id}`);
          }
        }
        channelInfoById.set(record.id, {
          info: record,
          parsedChannel: parseChannel({ schema, messageEncoding: record.messageEncoding }),
        });
        break;
      }

      case "Message": {
        const channelInfo = channelInfoById.get(record.channelId);
        if (!channelInfo) {
          throw new Error(`message for channel ${record.channelId} with no prior channel info`);
        }
        if (deserialize) {
          const message = channelInfo.parsedChannel.deserialize(record.data);
          if (dump) {
            log(message);
          }
        }
        break;
      }
    }
  }

  log("Reading", filePath);

  let isValidMcap = false;
  {
    const handle = await fs.open(filePath, "r");
    try {
      const buffer = new Uint8Array(McapConstants.MCAP_MAGIC.length);
      const readResult = await handle.read({
        buffer,
        offset: 0,
        length: McapConstants.MCAP_MAGIC.length,
      });
      isValidMcap = hasMcapPrefix(new DataView(buffer.buffer, 0, readResult.bytesRead));
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

  if (!isValidMcap) {
    return;
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
