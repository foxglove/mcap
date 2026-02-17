import { LogLevel as FoxLogLevel } from "@foxglove/schemas";
import type { Log } from "@foxglove/schemas";
import {
  ULog,
  type MessageDefinition,
  MessageType,
  LogLevel,
  type FieldArray,
  type FieldPrimitive,
  type FieldStruct,
} from "@foxglove/ulog";
import { McapWriter } from "@mcap/core";
import type { Metadata } from "@mcap/core";
import { protobufToDescriptor } from "@mcap/support";
import { createRequire } from "node:module";
import * as protobufjs from "protobufjs";
import { FileDescriptorSet } from "protobufjs/ext/descriptor/index.js";

const require = createRequire(import.meta.url);
const packageJson = require("../package.json") as { version: string };

function ulogLevelToFoxLogLevel(level: LogLevel): FoxLogLevel {
  switch (level) {
    case LogLevel.Emerg:
      return FoxLogLevel.FATAL;
    case LogLevel.Alert:
      return FoxLogLevel.ERROR;
    case LogLevel.Crit:
      return FoxLogLevel.ERROR;
    case LogLevel.Err:
      return FoxLogLevel.ERROR;
    case LogLevel.Warning:
      return FoxLogLevel.WARNING;
    case LogLevel.Notice:
      return FoxLogLevel.WARNING;
    case LogLevel.Info:
      return FoxLogLevel.INFO;
    case LogLevel.Debug:
      return FoxLogLevel.DEBUG;
    default:
      return FoxLogLevel.UNKNOWN;
  }
}

function ulogFieldTypeToProtobufFieldType(fieldType: string): string | undefined {
  switch (fieldType) {
    case "bool":
      return "bool";
    case "char": // ULog strings are char[length] arrays
      return "string";
    case "float":
      return "float";
    case "double":
      return "double";
    case "int8_t":
    case "int16_t":
    case "int32_t":
      return "int32";
    case "uint8_t":
    case "uint16_t":
    case "uint32_t":
      return "uint32";
    case "int64_t":
      return "int64";
    case "uint64_t":
      return "uint64";
    default:
      return undefined;
  }
}

// ** Construct a map of dependencies between schema definitions so we can build a minimal tree of types later */
function getSchemaDependencies(
  definitions: Map<string, MessageDefinition>,
): Map<string, Array<string>> {
  const dependencies = new Map<string, Array<string>>();
  for (const [schemaName, definition] of definitions) {
    for (const field of definition.fields) {
      // Omit special fields
      // Timestamp is used for message log time so it's not required in the message body
      // _padding0 is a padding field and contains no data
      if (field.name === "timestamp" || field.name.startsWith("_padding")) {
        continue;
      }
      if (definitions.has(field.type)) {
        // Record dependency
        if (!dependencies.has(schemaName)) {
          dependencies.set(schemaName, []);
        }
        dependencies.get(schemaName)!.push(field.type);
      }
    }
  }
  return dependencies;
}

/** Convert ULog message definitions to a Protobuf root and dependencies map. */
function ulogDefinitionToProtobufType(
  schemaName: string,
  definition: MessageDefinition,
): protobufjs.Type {
  const fieldTypeProto = new protobufjs.Type(schemaName);
  let id = 1;
  for (const field of definition.fields) {
    // Omit special fields
    // Timestamp is used for message log time so it's not required in the message body
    // _padding0 is a padding field and contains no data
    if (field.name === "timestamp" || field.name.startsWith("_padding")) {
      continue;
    }

    const primitiveType = ulogFieldTypeToProtobufFieldType(field.type);
    const fieldType = primitiveType ?? field.type;
    const rule =
      field.arrayLength != undefined && primitiveType !== "string" ? "repeated" : "required";

    fieldTypeProto.add(new protobufjs.Field(field.name, id, fieldType, rule));
    id += 1;
  }

  return fieldTypeProto;
}

/** Create a new protobuf Root with only the types required for one root schema */
function getMinimalProtobufRoot(
  rootName: string,
  definitions: Map<string, MessageDefinition>,
  dependencies: Map<string, Array<string>>,
): protobufjs.Root {
  const minimalRoot = new protobufjs.Root();
  const typesToProcess = new Set<string>();
  typesToProcess.add(rootName);
  const processed = new Set<string>();

  for (const typeName of typesToProcess) {
    typesToProcess.delete(typeName);
    if (!definitions.has(typeName)) {
      throw new Error(`Type ${typeName} not found in type map`);
    }
    const type = ulogDefinitionToProtobufType(typeName, definitions.get(typeName)!);
    minimalRoot.add(type);
    processed.add(typeName);
    for (const dependentType of dependencies.get(typeName) ?? []) {
      if (!processed.has(dependentType)) {
        typesToProcess.add(dependentType);
      }
    }
  }
  return minimalRoot;
}

function microsecondToNanosecondTimestamp(startOffset: bigint, timestamp: bigint): bigint {
  return (startOffset + timestamp) * 1000n;
}

async function writeMessageToMCAP(
  mcapWriterInfo: {
    writer: McapWriter;
    channelIdToSequence: Map<number, number>;
  },
  channelId: number,
  timestamp: bigint,
  messageType: protobufjs.Type,
  msgData: Record<string, FieldStruct | FieldPrimitive | FieldArray | Date>,
) {
  const sequenceNumber = mcapWriterInfo.channelIdToSequence.get(channelId) ?? 0;
  const protoMsg = messageType.fromObject(msgData);
  await mcapWriterInfo.writer.addMessage({
    channelId,
    sequence: sequenceNumber,
    publishTime: timestamp,
    logTime: timestamp,
    data: messageType.encode(protoMsg).finish(),
  });
  mcapWriterInfo.channelIdToSequence.set(channelId, sequenceNumber + 1);
}

/**
 * Read a ULog file and convert it to MCAP format.
 * @param inputFile - The ULog file handle to convert
 * @param outputFile - The MCAP file handle to write to
 * @param options - Optional parameters
 * @param options.startTime - The initial time to use for message timestamps (in microseconds).
 *                    Recommended since ULog timestamps are often only relative to device startup.
 * @param options.metadata - Optional list of Metadata objects to add to the file.
 */
export async function convertULogFileToMCAP(
  inputFile: ULog,
  outputFile: McapWriter,
  options?: {
    startTime?: bigint;
    metadata?: Metadata[];
  },
): Promise<void> {
  await inputFile.open();
  if (inputFile.header == undefined) {
    throw new Error("Invalid ULog file: missing header");
  }
  if (inputFile.header.version !== 1) {
    throw new Error(`Unknown ULog file version: ${inputFile.header.version}`);
  }

  await outputFile.start({
    profile: "",
    library: `ulog2mcap ${packageJson.version}`,
  });
  if (options?.metadata != undefined) {
    for (const metadataItem of options.metadata) {
      await outputFile.addMetadata(metadataItem);
    }
  }

  // Ulog records the timestamp at the start of recording and the timestamps of each message as microseconds since device startup
  // When absolute start time is provided, we subtract the recording start timestamp from each message timestamp to get microseconds
  // since recording start and add that to the new start time to get microseconds since epoch
  const deviceRecordingStartTime = inputFile.header.timestamp;
  const startTimestampOffset =
    options?.startTime != undefined ? options.startTime - deviceRecordingStartTime : 0n;

  // Count subscriptions with the same name so we know when to append multiId to channel name
  const numSubscriptions = new Map<string, number>();
  for (const subscription of inputFile.subscriptions.values()) {
    const count = numSubscriptions.get(subscription.name) ?? 0;
    numSubscriptions.set(subscription.name, count + 1);
  }

  // Register schemas and channels
  const dependencies = getSchemaDependencies(inputFile.header.definitions);

  const msgIdToChannelId = new Map<number, number>();
  const msgIdToSchema = new Map<number, protobufjs.Type>();
  const schemaNameToSchemaId = new Map<string, number>();
  const schemaNameToSchema = new Map<string, protobufjs.Type>();

  // For each subscription, create a minimal protobuf root and register a new schema (if needed) and channel
  for (const [msgId, subscription] of inputFile.subscriptions.entries()) {
    const channelName =
      (numSubscriptions.get(subscription.name) ?? 1) === 1
        ? subscription.name
        : `${subscription.name}/${subscription.multiId}`;

    let schemaId = schemaNameToSchemaId.get(subscription.name);
    if (schemaId == undefined) {
      const minimalRoot = getMinimalProtobufRoot(
        subscription.name,
        inputFile.header.definitions,
        dependencies,
      );
      const descriptorSet = protobufToDescriptor(minimalRoot);
      schemaId = await outputFile.registerSchema({
        name: subscription.name,
        encoding: "protobuf",
        data: FileDescriptorSet.encode(descriptorSet).finish(),
      });
      const msgType = minimalRoot.lookupType(subscription.name);
      schemaNameToSchemaId.set(subscription.name, schemaId);
      schemaNameToSchema.set(subscription.name, msgType);
    }

    const channelId = await outputFile.registerChannel({
      schemaId,
      topic: channelName,
      messageEncoding: "protobuf",
      metadata: new Map(),
    });
    msgIdToChannelId.set(msgId, channelId);
    msgIdToSchema.set(msgId, schemaNameToSchema.get(subscription.name)!);
  }

  // Add an additional channel for log messages
  const root = await protobufjs.load(`${import.meta.dirname}/Log.proto`);
  const logType = root.lookupType("foxglove.Log");
  const logSchema = await outputFile.registerSchema({
    name: "foxglove.Log",
    encoding: "protobuf",
    data: FileDescriptorSet.encode(protobufToDescriptor(root)).finish(),
  });
  const logChannel = await outputFile.registerChannel({
    schemaId: logSchema,
    topic: "log_message",
    messageEncoding: "protobuf",
    metadata: new Map(),
  });

  const mcapWriterInfo = {
    writer: outputFile,
    channelIdToSequence: new Map<number, number>(),
  };

  // Read messages and write to MCAP
  for await (const msg of inputFile.readMessages()) {
    if (msg.type === MessageType.Data) {
      // Structured data message
      const { timestamp, ...message } = msg.value;
      const channelId = msgIdToChannelId.get(msg.msgId);
      if (channelId == undefined) {
        throw new Error(`No channel ID found for message ID: ${msg.msgId}`);
      }
      const messageType = msgIdToSchema.get(msg.msgId);
      if (messageType == undefined) {
        throw new Error(`No message schema found for message ID: ${msg.msgId}`);
      }
      const nanoTimestamp = microsecondToNanosecondTimestamp(startTimestampOffset, timestamp);
      await writeMessageToMCAP(mcapWriterInfo, channelId, nanoTimestamp, messageType, message);
    } else if (msg.type === MessageType.Log || msg.type === MessageType.LogTagged) {
      const nanoTimestamp = microsecondToNanosecondTimestamp(startTimestampOffset, msg.timestamp);
      // Log message
      const msgData = {
        timestamp: {
          sec: Number(nanoTimestamp / 1000000000n),
          nsec: Number(nanoTimestamp % 1000000000n),
        },
        level: ulogLevelToFoxLogLevel(msg.logLevel),
        message: msg.message,
      } as Log;
      await writeMessageToMCAP(mcapWriterInfo, logChannel, nanoTimestamp, logType, msgData);
    } else if (msg.type === MessageType.Dropout) {
      console.warn(`Warning: message dropout of ${msg.duration} microseconds detected`);
    }
  }

  await outputFile.end();
}
