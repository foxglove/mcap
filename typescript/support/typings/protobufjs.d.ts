import protobufjs from "@foxglove/protobufjs";
import descriptor from "@foxglove/protobufjs/ext/descriptor";

// https://github.com/protobufjs/protobuf.js/issues/1499
declare module "@foxglove/protobufjs" {
  interface ReflectionObject {
    toDescriptor(
      protoVersion: string,
    ): protobufjs.Message<descriptor.IFileDescriptorSet> & descriptor.IFileDescriptorSet;
  }
  declare namespace ReflectionObject {
    export const fromDescriptor: (desc: protobufjs.Message) => protobufjs.Root;
  }
}
