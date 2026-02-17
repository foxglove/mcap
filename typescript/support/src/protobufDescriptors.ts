import * as protobufjs from "protobufjs";
import * as descriptorModule from "protobufjs/ext/descriptor/index.js";
import type { IFileDescriptorSet } from "protobufjs/ext/descriptor/index.js";

import { unwrapDefaultExport } from "./esmInterop.ts";

type ProtobufJsModule = typeof import("protobufjs");
type ProtobufDescriptorModule = typeof import("protobufjs/ext/descriptor/index.js");

const protobufjsRuntime = unwrapDefaultExport<ProtobufJsModule>(protobufjs);
const descriptor = unwrapDefaultExport<ProtobufDescriptorModule>(descriptorModule);
const FileDescriptorSet = descriptor.FileDescriptorSet;

// https://github.com/protobufjs/protobuf.js/issues/1499
declare module "protobufjs" {
  interface ReflectionObject {
    toDescriptor(protoVersion: string): protobufjs.Message<IFileDescriptorSet> & IFileDescriptorSet;
  }
  // eslint-disable-next-line @typescript-eslint/no-namespace
  namespace ReflectionObject {
    const fromDescriptor: (desc: protobufjs.Message) => protobufjs.Root;
  }
}

export type ProtobufDescriptor = ReturnType<protobufjs.Root["toDescriptor"]>;

export function protobufToDescriptor(root: protobufjs.Root): ProtobufDescriptor {
  return root.toDescriptor("proto3");
}

export function protobufFromDescriptor(descriptorSet: protobufjs.Message): protobufjs.Root {
  return protobufjsRuntime.Root.fromDescriptor(descriptorSet);
}

export function protobufFromBinaryDescriptor(schemaData: Uint8Array): protobufjs.Root {
  return protobufFromDescriptor(FileDescriptorSet.decode(schemaData));
}
