// convert a ROS1 .bag file to an mcap file with protobuf schema and message encoding

import { Bag } from "@foxglove/rosbag";
import { FileReader } from "@foxglove/rosbag/node.js";
import { parse as parseMessageDefinition } from "@foxglove/rosmsg";
import type { Time } from "@foxglove/rosmsg-serialization";
import { toNanoSec } from "@foxglove/rostime";
import Bzip2 from "@foxglove/wasm-bz2";
import decompressLZ4 from "@foxglove/wasm-lz4";
import zstd from "@foxglove/wasm-zstd";
import { McapWriter } from "@mcap/core";
import type { Channel } from "@mcap/core";
import { FileHandleWritable } from "@mcap/nodejs";
import { protobufToDescriptor } from "@mcap/support";
import type { ProtobufDescriptor } from "@mcap/support";
import { program } from "commander";
import { open } from "node:fs/promises";
import * as protobufjs from "protobufjs";
import { FileDescriptorSet } from "protobufjs/ext/descriptor/index.js";

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
function rosMsgDefinitionToProto(
  typeName: string,
  msgDef: string,
): {
  rootType: protobufjs.Type;
  descriptorSet: ProtobufDescriptor;
  schemaName: string;
} {
  const definitionArr = parseMessageDefinition(msgDef);
  const root = new protobufjs.Root();

  const BuiltinSrcParse = protobufjs.parse(builtinSrc, { keepCase: true });
  BuiltinSrcParse.root.nested!["ros"]!.filename = "ros/builtin.proto";
  root.add(BuiltinSrcParse.root);

  const dependenciesByFilename = new Map<string, Set<string>>();
  dependenciesByFilename.set("ros/builtin.proto", new Set());

  for (const def of definitionArr) {
    const rosDatatypeName = def.name ?? typeName;
    const nameParts = rosDatatypeName.split("/");
    if (nameParts.length !== 2) {
      throw new Error(`Invalid name ${typeName}`);
    }
    const packageName = nameParts[0]!;
    const msgName = nameParts[1]!;

    const filename = `ros/${packageName}.proto`;
    let dependencies = dependenciesByFilename.get(filename);
    if (!dependencies) {
      dependencies = new Set();
      dependenciesByFilename.set(filename, dependencies);
    }

    const fields: string[] = [];
    let fieldNumber = 1;
    for (const field of def.definitions) {
      if (field.isConstant === true) {
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
          const fieldNameParts = field.type.split("/");
          if (fieldNameParts.length !== 2) {
            throw new Error(`Invalid complex type name: ${field.type}`);
          }
          if (packageName !== fieldNameParts[0]!) {
            dependencies.add(`ros/${fieldNameParts[0]!}.proto`);
          }
        } else if (BUILTIN_TYPE_MAP.has(field.type)) {
          const protoType = BUILTIN_TYPE_MAP.get(field.type)!;
          if (protoType.includes("int")) {
            lineComments.push(`originally ${field.type}`);
          }
          qualifiers.push(BUILTIN_TYPE_MAP.get(field.type)!);
          if (field.type === "time" || field.type === "duration") {
            dependencies.add("ros/builtin.proto");
          }
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

    const ProtoSrcParse = protobufjs.parse(outputSections.join("\n"), { keepCase: true });
    // HACK: set the filename on the nested namespace object so that `root.toDescriptor` generates
    // file descriptors with the correct filename.
    (ProtoSrcParse.root.nested!["ros"] as protobufjs.Namespace).nested![packageName]!.filename =
      filename;
    root.add(ProtoSrcParse.root);
  }

  const schemaName = rosTypenameToProtoPath(typeName);
  const rootType = root.lookupType(schemaName);

  // create a descriptor message for the root
  const descriptorSet = protobufToDescriptor(root);
  for (const file of descriptorSet.file) {
    // Strip leading `.` from the package names to make them relative to the descriptor
    file.package = file.package?.substring(1);
    // protobufjs does not generate dependency fields, so fix them up manually
    if (file.name == undefined || file.name.length === 0) {
      throw new Error(`Missing filename for ${file.package ?? "(unknown package)"}`);
    }
    const deps = dependenciesByFilename.get(file.name);
    if (deps == undefined) {
      throw new Error(`Unknown dependencies for ${file.name}`);
    }
    file.dependency = Array.from(deps);
  }

  return { rootType, descriptorSet, schemaName };
}

type TopicDetail = {
  channelId: number;
  rootType: protobufjs.Type;
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

async function convert(filePath: string, options: { indexed: boolean }) {
  await decompressLZ4.isLoaded;
  await zstd.isLoaded;
  const bzip2 = await Bzip2.default.init();

  const bag = new Bag(new FileReader(filePath));
  await bag.open();

  const mcapFilePath = filePath.replace(".bag", ".mcap");
  console.debug(`Writing to ${mcapFilePath}`);

  const fileHandle = await open(mcapFilePath, "w");
  const fileHandleWritable = new FileHandleWritable(fileHandle);

  const mcapFile = new McapWriter({
    writable: fileHandleWritable,
    useStatistics: true,
    useChunks: options.indexed,
    useChunkIndex: options.indexed,
    compressChunk: (data) => ({
      compression: "zstd",
      compressedData: new Uint8Array(zstd.compress(data, 19)),
    }),
  });

  await mcapFile.start({
    profile: "",
    library: "mcap typescript bag2mcap",
  });

  const topicToDetailMap = new Map<string, TopicDetail>();

  for (const [, connection] of bag.connections) {
    if (!connection.type) {
      continue;
    }

    const { rootType, descriptorSet, schemaName } = rosMsgDefinitionToProto(
      connection.type,
      connection.messageDefinition,
    );
    const descriptorMsgEncoded = FileDescriptorSet.encode(descriptorSet).finish();

    const schemaId = await mcapFile.registerSchema({
      name: schemaName,
      encoding: "protobuf",
      data: descriptorMsgEncoded,
    });

    const channelInfo: Omit<Channel, "id"> = {
      schemaId,
      topic: connection.topic,
      messageEncoding: "protobuf",
      metadata: new Map(),
    };

    const channelId = await mcapFile.registerChannel(channelInfo);

    topicToDetailMap.set(connection.topic, {
      channelId,
      rootType,
    });
  }

  const readResults: Array<{ topic: string; message: unknown; timestamp: Time }> = [];
  await bag.readMessages(
    {
      decompress: {
        lz4: (buffer: Uint8Array, size: number) => new Uint8Array(decompressLZ4(buffer, size)),
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

    const { channelId, rootType } = detail;
    try {
      const rosMsg = convertTypedArrays(result.message as Record<string, unknown>);
      const protoMsg = rootType.fromObject(rosMsg);
      const protoMsgBuffer = rootType.encode(protoMsg).finish();

      const timestamp = toNanoSec(result.timestamp);
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
