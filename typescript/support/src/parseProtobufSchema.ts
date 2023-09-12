import protobufjs from "@foxglove/protobufjs";
import { FileDescriptorSet } from "@foxglove/protobufjs/ext/descriptor";

import { protobufDefinitionsToDatatypes, stripLeadingDot } from "./protobufDefinitionsToDatatypes";
import { MessageDefinitionMap } from "./types";

/**
 * Parse a Protobuf binary schema (FileDescriptorSet) and produce datatypes and a deserializer
 * function.
 */
export function parseProtobufSchema(
  schemaName: string,
  schemaData: Uint8Array,
): {
  datatypes: MessageDefinitionMap;
  deserialize: (buffer: ArrayBufferView) => unknown;
} {
  const descriptorSet = FileDescriptorSet.decode(schemaData);

  const root = protobufjs.Root.fromDescriptor(descriptorSet);
  root.resolveAll();
  const rootType = root.lookupType(schemaName);

  // Modify the definition of google.protobuf.Timestamp and Duration so they are interpreted as
  // {sec: number, nsec: number}, compatible with the rest of Studio. The standard Protobuf types
  // use different names (`seconds` and `nanos`), and `seconds` is an `int64`, which would be
  // deserialized as a bigint by default.
  //
  // protobufDefinitionsToDatatypes also has matching logic to rename the fields.
  const fixTimeType = (type: protobufjs.ReflectionObject | null) => {
    if (!type || !(type instanceof protobufjs.Type)) {
      return;
    }
    type.setup(); // ensure the original optimized toObject has been created
    const prevToObject = type.toObject; // eslint-disable-line @typescript-eslint/unbound-method
    const newToObject: typeof prevToObject = (message, options) => {
      const result = prevToObject.call(type, message, options);
      const { seconds, nanos } = result as { seconds: bigint; nanos: number };
      if (typeof seconds !== "bigint" || typeof nanos !== "number") {
        return result;
      }
      if (seconds > BigInt(Number.MAX_SAFE_INTEGER)) {
        throw new Error(
          `Timestamps with seconds greater than 2^53-1 are not supported (found seconds=${seconds}, nanos=${nanos})`,
        );
      }
      return { sec: Number(seconds), nsec: nanos };
    };
    type.toObject = newToObject;
  };

  fixTimeType(root.lookup(".google.protobuf.Timestamp"));
  fixTimeType(root.lookup(".google.protobuf.Duration"));

  const deserialize = (data: ArrayBufferView) => {
    return rootType.toObject(
      rootType.decode(new Uint8Array(data.buffer, data.byteOffset, data.byteLength)),
      { defaults: true, longs: BigInt },
    );
  };

  const datatypes: MessageDefinitionMap = new Map();
  protobufDefinitionsToDatatypes(datatypes, rootType);

  if (!datatypes.has(schemaName)) {
    throw new Error(
      `Protobuf schema does not contain an entry for '${schemaName}'. The schema name should be fully-qualified, e.g. '${stripLeadingDot(
        rootType.fullName,
      )}'.`,
    );
  }

  return { deserialize, datatypes };
}
