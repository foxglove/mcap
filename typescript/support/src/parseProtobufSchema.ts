import protobufjs from "@foxglove/protobufjs";
import { FileDescriptorSet } from "@foxglove/protobufjs/ext/descriptor";

import { protobufDefinitionsToDatatypes, stripLeadingDot } from "./protobufDefinitionsToDatatypes";
import { MessageDefinitionMap } from "./types";

export type ParseProtobufSchemaOptions = {
  /**
   * A function that will be called with the root type after parsing the FileDescriptorSet. Used by
   * Foxglove Studio to modify the deserialization behavior of google.protobuf.Timestamp &
   * google.protobuf.Duration.
   */
  processRootType?: (rootType: protobufjs.Type) => protobufjs.Type;

  /**
   * A function that will be called after producing message definitions from the schema. Used by
   * Foxglove Studio to modify the field name of google.protobuf.Timestamp &
   * google.protobuf.Duration.
   */
  processMessageDefinitions?: (definitions: MessageDefinitionMap) => MessageDefinitionMap;
};

/**
 * Parse a Protobuf binary schema (FileDescriptorSet) and produce datatypes and a deserializer
 * function.
 */
export function parseProtobufSchema(
  schemaName: string,
  schemaData: Uint8Array,
  options?: ParseProtobufSchemaOptions,
): {
  datatypes: MessageDefinitionMap;
  deserialize: (buffer: ArrayBufferView) => unknown;
} {
  const descriptorSet = FileDescriptorSet.decode(schemaData);

  const root = protobufjs.Root.fromDescriptor(descriptorSet);
  root.resolveAll();
  let rootType = root.lookupType(schemaName);
  if (options?.processRootType) {
    rootType = options.processRootType(rootType);
  }

  const deserialize = (data: ArrayBufferView) => {
    return rootType.toObject(
      rootType.decode(new Uint8Array(data.buffer, data.byteOffset, data.byteLength)),
      { defaults: true, longs: BigInt },
    );
  };

  let datatypes: MessageDefinitionMap = new Map();
  protobufDefinitionsToDatatypes(datatypes, rootType);
  if (options?.processMessageDefinitions) {
    datatypes = options.processMessageDefinitions(datatypes);
  }

  if (!datatypes.has(schemaName)) {
    throw new Error(
      `Protobuf schema does not contain an entry for '${schemaName}'. The schema name should be fully-qualified, e.g. '${stripLeadingDot(
        rootType.fullName,
      )}'.`,
    );
  }

  return { deserialize, datatypes };
}
