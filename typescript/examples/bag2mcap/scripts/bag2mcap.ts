// This Source Code Form is subject to the terms of the Mozilla Public
// License, v2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/

// convert a ROS1 .bag file to an mcap file with protobuf schema and message encoding

import { Mcap0Writer, IWritable, Mcap0Types } from "@foxglove/mcap";
import { Bag } from "@foxglove/rosbag";
import { FileReader } from "@foxglove/rosbag/node";
import { parse as parseMessageDefinition } from "@foxglove/rosmsg";
import { Time } from "@foxglove/rosmsg-serialization";
import Bzip2 from "@foxglove/wasm-bz2";
import { program } from "commander";
import { open, FileHandle } from "fs/promises";
import protobufjs from "protobufjs";
import descriptor from "protobufjs/ext/descriptor";
import decompressLZ4 from "wasm-lz4";

const builtinSrc = `
syntax = "proto3";

package ros;

message Time {
  fixed32 sec = 1;
  fixed32 nsec = 2;
}

message Duration {
    fixed32 sec = 1;
    fixed32 nsec = 2;
  }
`;

const BUILTIN_TYPE_MAP = new Map([
  ["time", "ros.Time"],
  ["duration", "ros.Duration"],
  ["uint8", "int32"],
  ["uint16", "int32"],
  ["int8", "int32"],
  ["int16", "int32"],
  ["float32", "float"],
  ["float64", "double"],
]);

function rosTypenameToProtoPath(typeName: string): string {
  return `ros.${typeName.replace("/", ".")}`;
}

// convert a combined ros string message definition to protobuf Root instance
function rosMsgDefinitionToProto(typeName: string, msgDef: string): protobufjs.Root {
  const definitionArr = parseMessageDefinition(msgDef);
  const root = new protobufjs.Root();

  const BuiltinSrcParse = protobufjs.parse(builtinSrc, { keepCase: true });
  root.add(BuiltinSrcParse.root);

  for (const def of definitionArr) {
    const rosDatatypeName = def.name ?? typeName;
    const nameParts = rosDatatypeName.split("/");
    if (nameParts.length !== 2) {
      throw new Error(`Invalid name ${typeName}`);
    }
    const packageName = nameParts[0]!;
    const msgName = nameParts[1]!;

    const fields: string[] = [];
    let fieldNumber = 1;
    for (const field of def.definitions) {
      if (field.isConstant === true) {
        // eslint-disable-next-line @typescript-eslint/restrict-template-expressions
        fields.push(`// ${field.type} ${field.name} = ${field.valueText ?? field.value ?? ""}`);
        continue;
      }
      const lineComments: string[] = [];
      const qualifiers = [];
      if (field.isArray === true && (field.type === "uint8" || field.type === "int8")) {
        qualifiers.push("bytes");
      } else {
        if (field.isArray === true) {
          qualifiers.push("repeated");
        }
        if (field.isComplex === true) {
          qualifiers.push(rosTypenameToProtoPath(field.type));
        } else if (BUILTIN_TYPE_MAP.has(field.type)) {
          const protoType = BUILTIN_TYPE_MAP.get(field.type)!;
          if (protoType.includes("int")) {
            lineComments.push(`originally ${field.type}`);
          }
          qualifiers.push(BUILTIN_TYPE_MAP.get(field.type)!);
        } else {
          qualifiers.push(field.type);
        }
      }
      if (field.arrayLength != undefined) {
        lineComments.push(`length ${field.arrayLength}`);
      }
      fields.push(
        `${qualifiers.join(" ")} ${field.name} = ${fieldNumber++};${
          lineComments.length > 0 ? " // " + lineComments.join(", ") : ""
        }`,
      );
    }

    const outputSections = [
      `// Generated from ${rosDatatypeName}`,

      'syntax = "proto3";',

      `package ros.${packageName};`,

      `message ${msgName} {\n  ${fields.join("\n  ")}\n}`,
    ];

    const protoSrc = outputSections.filter(Boolean).join("\n\n") + "\n";

    const ProtoSrcParse = protobufjs.parse(protoSrc, { keepCase: true });
    root.add(ProtoSrcParse.root);
  }

  return root;
}

type TopicDetail = {
  channelId: number;
  MsgRoot: protobufjs.Type;
};

// Protobuf fromObject doesn't like being given Float64Arrays
// We need to recursively convert all Float64Arrays into regular arrays
function convertTypedArrays(msg: Record<string, unknown>): Record<string, unknown> {
  for (const [key, value] of Object.entries(msg)) {
    if (value == undefined) {
      continue;
    }
    if (value instanceof Float64Array) {
      msg[key] = Array.from(value);
    } else if (typeof value === "object") {
      msg[key] = convertTypedArrays(value as Record<string, unknown>);
    }
  }

  return msg;
}

// IWritable interface for FileHandle
class FileHandleWritable implements IWritable {
  private handle: FileHandle;
  private totalBytesWritten = 0;

  constructor(handle: FileHandle) {
    this.handle = handle;
  }

  async write(buffer: Uint8Array): Promise<void> {
    const written = await this.handle.write(buffer);
    this.totalBytesWritten += written.bytesWritten;
  }

  position(): bigint {
    return BigInt(this.totalBytesWritten);
  }
}

async function convert(filePath: string, options: { indexed: boolean }) {
  await decompressLZ4.isLoaded;
  const bzip2 = await Bzip2.init();

  const bag = new Bag(new FileReader(filePath));
  await bag.open();

  const mcapFilePath = filePath.replace(".bag", ".mcap");
  console.debug(`Writing to ${mcapFilePath}`);

  const fileHandle = await open(mcapFilePath, "w");
  const fileHandleWritable = new FileHandleWritable(fileHandle);

  const mcapFile = new Mcap0Writer({
    writable: fileHandleWritable,
    useStatistics: true,
    useChunks: options.indexed,
    useChunkIndex: options.indexed,
  });

  await mcapFile.start({
    profile: "",
    library: "mcap typescript bag2proto",
  });

  await mcapFile.addMetadata({
    name: "original file info",
    metadata: new Map([["path", mcapFilePath]]),
  });

  const topicToDetailMap = new Map<string, TopicDetail>();

  for (const [, connection] of bag.connections) {
    if (!connection.type) {
      continue;
    }

    const schemaName = rosTypenameToProtoPath(connection.type);

    const root = rosMsgDefinitionToProto(connection.type, connection.messageDefinition);
    const MsgRoot = root.lookupType(schemaName);

    // create a descriptor message for the root
    // Strip leading `.` from the package names to make them relative to the descriptor
    const descriptorMsg = root.toDescriptor("proto3");
    for (const desc of descriptorMsg.file) {
      desc.package = desc.package?.substring(1);
    }

    const descriptorMsgEncoded = descriptor.FileDescriptorSet.encode(descriptorMsg).finish();

    const schemaId = await mcapFile.registerSchema({
      name: schemaName,
      encoding: "protobuf",
      data: descriptorMsgEncoded,
    });

    const channelInfo: Omit<Mcap0Types.Channel, "id"> = {
      schemaId,
      topic: connection.topic,
      messageEncoding: "protobuf",
      metadata: new Map(),
    };

    const channelId = await mcapFile.registerChannel(channelInfo);

    topicToDetailMap.set(connection.topic, {
      channelId,
      MsgRoot,
    });
  }

  const readResults: Array<{ topic: string; message: unknown; timestamp: Time }> = [];
  await bag.readMessages(
    {
      decompress: {
        lz4: (buffer: Uint8Array, size: number) => decompressLZ4(buffer, size),
        bz2: (buffer: Uint8Array, size: number) => bzip2.decompress(buffer, size, { small: false }),
      },
    },
    (result) => {
      readResults.push(result);
    },
  );

  for (const result of readResults) {
    const detail = topicToDetailMap.get(result.topic);
    if (!detail) {
      return;
    }

    const { channelId, MsgRoot } = detail;
    try {
      const rosMsg = convertTypedArrays(result.message as Record<string, unknown>);
      const protoMsg = MsgRoot.fromObject(rosMsg);
      const protoMsgBuffer = MsgRoot.encode(protoMsg).finish();

      const timestamp = BigInt(result.timestamp.sec) * 1000000000n + BigInt(result.timestamp.nsec);
      await mcapFile.addMessage({
        channelId,
        sequence: 0,
        publishTime: timestamp,
        logTime: timestamp,
        data: protoMsgBuffer,
      });
    } catch (err) {
      console.error(err);
      console.log(result.message);
      throw err;
    }
  }

  await mcapFile.end();
}

program
  .argument("<file...>", "path to .bag file(s)")
  .description("Convert a ROS1 .bag file to a mcap file with protobuf messages")
  .action(async (files: string[]) => {
    for (const file of files) {
      await convert(file, { indexed: true }).catch(console.error);
    }
  })
  .parse();
