import protobufjs from "protobufjs";
import { FileDescriptorSet, IFileDescriptorSet } from "protobufjs/ext/descriptor";

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
  return protobufjs.Root.fromDescriptor(descriptorSet);
}

export function protobufFromBinaryDescriptor(schemaData: Uint8Array): protobufjs.Root {
  return protobufFromDescriptor(FileDescriptorSet.decode(schemaData));
}
