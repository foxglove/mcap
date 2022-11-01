import {
  RosMsgDefinition,
  parse as parseRosMsgDef,
  ParseOptions,
  RosMsgField,
} from "@foxglove/rosmsg";
import { MessageReader as Ros1MessageReader } from "@foxglove/rosmsg-serialization";
import { MessageReader as Ros2MessageReader } from "@foxglove/rosmsg2-serialization";
import { McapIndexedReader, McapTypes } from "@mcap/core";
import { open, FileHandle } from "fs/promises";

const INTEGER_TYPES = new Set<string>([
  "bool",
  "byte",
  "char",
  "int8",
  "uint8",
  "int16",
  "uint16",
  "int32",
  "uint32",
  "int64",
  "uint64",
  "time",
  "duration",
]);
const FLOAT_TYPES = new Set<string>(["float32", "float64"]);

class FileHandleReadable implements McapTypes.IReadable {
  private handle: FileHandle;
  private fileSize?: bigint;

  constructor(handle: FileHandle) {
    this.handle = handle;
  }

  async size(): Promise<bigint> {
    if (this.fileSize != undefined) {
      return this.fileSize;
    }

    const stats = await this.handle.stat({ bigint: true });
    this.fileSize = stats.size;
    return this.fileSize;
  }

  async read(offset: bigint, size: bigint): Promise<Uint8Array> {
    const position = Number(offset);
    const length = Number(size);
    const data = new Uint8Array(length);
    const readResult = await this.handle.read({
      buffer: data,
      position,
      offset: 0,
      length,
    });
    if (readResult.bytesRead !== length) {
      throw new Error(`Attempted to read ${length} bytes but received ${readResult.bytesRead}`);
    }
    return data;
  }
}

type ChannelReader = {
  tableName: string;
  messageReader: Ros1MessageReader | Ros2MessageReader;
  definitions: Map<string, RosMsgDefinition>;
  file: string[];
  arrayFiles: Map<string, string[]>;
};

async function main(): Promise<void> {
  const filename = process.argv[2];
  if (!filename) {
    console.error(`Usage: mcap2sql <input.mcap>`);
    return process.exit(1);
  }

  const handle = await open(filename, "r");
  const readable = new FileHandleReadable(handle);
  const reader = await McapIndexedReader.Initialize({ readable });
  const textDecoder = new TextDecoder();

  // Parse all recognized schemas
  const decodedSchemas = new Map<number, Map<string, RosMsgDefinition>>();
  for (const [schemaId, schema] of reader.schemasById.entries()) {
    switch (schema.encoding) {
      case "ros1msg": {
        const schemaText = textDecoder.decode(schema.data);
        decodedSchemas.set(schemaId, decodeRosSchema(schemaText, { ros2: false }));
        break;
      }
      case "ros2msg": {
        const schemaText = textDecoder.decode(schema.data);
        decodedSchemas.set(schemaId, decodeRosSchema(schemaText, { ros2: true }));
        break;
      }
    }
  }

  // Convert each topic into one or more CREATE TABLE commands
  const topics: string[] = [];
  const channelReaders = new Map<number, ChannelReader>();
  for (const channel of reader.channelsById.values()) {
    if (topics.includes(channel.topic)) {
      continue;
    }
    if (channel.messageEncoding !== "ros1" && channel.messageEncoding !== "cdr") {
      continue;
    }

    const msgdefs = decodedSchemas.get(channel.schemaId);
    if (!msgdefs) {
      const schema = reader.schemasById.get(channel.schemaId);
      if (!schema) {
        throw new Error(`Missing schema id ${channel.schemaId} for topic "${channel.topic}"`);
      }
      throw new Error(
        `Unrecognized schema encoding "${schema.encoding}" for topic "${channel.topic}"`,
      );
    }
    const rootMsgDef = msgdefs.get("")!;

    const tableName = sanitizeSqlTableName(channel.topic);
    writeSchema(`CREATE TABLE "${tableName}" (`);
    writeSchema(`  "_message_id" INTEGER PRIMARY KEY`);
    writeSchema(`, "_publish_time" INTEGER NOT NULL`);
    writeSchema(`, "_log_time" INTEGER NOT NULL`);
    writeSchema(`, "_sequence" INTEGER NOT NULL`);

    for (const field of rootMsgDef.definitions) {
      writeField(field, "", msgdefs);
    }

    writeSchema(");\n");

    topics.push(channel.topic);

    const definitions = Array.from(msgdefs.values());
    if (channel.messageEncoding === "ros1") {
      channelReaders.set(channel.id, {
        tableName,
        messageReader: new Ros1MessageReader(definitions),
        definitions: msgdefs,
        file: [],
      });
    } else if (channel.messageEncoding === "cdr") {
      channelReaders.set(channel.id, {
        tableName,
        messageReader: new Ros2MessageReader(definitions),
        definitions: msgdefs,
        file: [],
      });
    }
  }

  // Convert each topic into a CSV file by reading all messages
  for await (const messageEvent of reader.readMessages({ topics })) {
    const channelReader = channelReaders.get(messageEvent.channelId);
    if (!channelReader) {
      continue;
    }

    const message = channelReader.messageReader.readMessage(messageEvent.data);
    const line = createCsvLine(messageEvent, message, channelReader.definitions);
    channelReader.file.push(line);
  }
}

function decodeRosSchema(schemaText: string, options: ParseOptions): Map<string, RosMsgDefinition> {
  const msgdefs = new Map<string, RosMsgDefinition>();
  for (const msgdef of parseRosMsgDef(schemaText, options)) {
    msgdefs.set(msgdef.name ?? "", msgdef);
  }
  return msgdefs;
}

function writeField(
  field: RosMsgField,
  parentName: string,
  definitions: Map<string, RosMsgDefinition>,
): void {
  if (field.isConstant === true) {
    return;
  }

  const sanitizedName = sanitizeSqlColumnName(field.name);
  const columnName = parentName ? `${parentName}.${sanitizedName}` : sanitizedName;

  if (field.isArray === true) {
    return; // FIXMEE
  } else {
    if (field.isComplex === true) {
      const msgdef = definitions.get(field.type);
      if (!msgdef) {
        throw new Error(`Missing definition for type "${field.type}"`);
      }

      for (const subfield of msgdef.definitions) {
        writeField(subfield, columnName, definitions);
      }
    } else {
      if (INTEGER_TYPES.has(field.type)) {
        writeSchema(`, "${columnName}" INTEGER NOT NULL`);
      } else if (FLOAT_TYPES.has(field.type)) {
        writeSchema(`, "${columnName}" REAL NOT NULL`);
      } else if (field.type === "string") {
        writeSchema(`, "${columnName}" TEXT NOT NULL`);
      } else {
        throw new Error(`Unrecognized field type "${field.type}" for field "${columnName}"`);
      }
    }
  }
}

function writeSchema(output: string): void {
  console.log(output);
}

function writeMessage(messageId: number, messageEvent: McapTypes.Message, message: unknown, definitions: Map<string, RosMsgDefinition>, output: Map<string, ChannelReader>): void {
  const values = [messageId, messageEvent.publishTime, messageEvent.logTime, messageEvent.sequence];

  const rootMsgDef = definitions.get("")!;
  for (const field of rootMsgDef.definitions) {
    // writeFieldValue(field, message, definitions, values);
  }
}

// Return a sanitized variant of the input string suitable for use as a SQL
// table name
function sanitizeSqlTableName(input: string): string {
  return input
    .trim()
    .replace(/^\/+|\/+$/g, "")
    .replace(/\//g, "__")
    .replace(/[^a-zA-Z0-9_]/g, "_");
}

// Return a sanitized variant of the input string suitable for use as a SQL
// column name
function sanitizeSqlColumnName(input: string): string {
  return input.trim().replace(/[^a-zA-Z0-9_]/g, "_");
}

////////////////////////////////////////////////////////////////////////////////

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
