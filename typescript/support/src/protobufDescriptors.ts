// eslint-disable-next-line @typescript-eslint/triple-slash-reference
/// <reference path="../typings/protobufjs.d.ts" />

import protobufjs from "protobufjs";
import { FileDescriptorSet } from "protobufjs/ext/descriptor";

export type ProtobufDescriptor = ReturnType<protobufjs.Root["toDescriptor"]>;

export function protobufToDescriptor(root: protobufjs.Root): ProtobufDescriptor {
  return root.toDescriptor("proto3");
}

export function protobufFromDescriptor(descriptorSet: protobufjs.Message): protobufjs.Root {
  return protobufjs.Root.fromDescriptor(descriptorSet);
}

export function protobufFromBinaryDescriptor(schemaData: Uint8Array): protobufjs.Root {
  return protobufFromDescriptor(FileDescriptorSet.decode(schemaData));
}
