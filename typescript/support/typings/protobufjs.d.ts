import protobufjs from "protobufjs";
import descriptor from "protobufjs/ext/descriptor";

// https://github.com/protobufjs/protobuf.js/issues/1499
declare module "protobufjs" {
  interface ReflectionObject {
    toDescriptor(
      protoVersion: string,
    ): protobufjs.Message<descriptor.IFileDescriptorSet> & descriptor.IFileDescriptorSet;
  }
  namespace ReflectionObject {
    export const fromDescriptor: (desc: protobufjs.Message) => protobufjs.Root;
  }
}
