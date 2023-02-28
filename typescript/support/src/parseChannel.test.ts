import fs from "fs";
import { FileDescriptorSet, IFileDescriptorSet } from "protobufjs/ext/descriptor";

import { parseChannel } from "./parseChannel";
import { protobufToDescriptor } from "./protobufDescriptors";

describe("parseChannel", () => {
  it("works with json/jsonschema", () => {
    const channel = parseChannel({
      messageEncoding: "json",
      schema: {
        name: "X",
        encoding: "jsonschema",
        data: new TextEncoder().encode(
          JSON.stringify({ type: "object", properties: { value: { type: "string" } } }),
        ),
      },
    });
    expect(channel.deserializer(new TextEncoder().encode(JSON.stringify({ value: "hi" })))).toEqual(
      { value: "hi" },
    );
  });

  it("works with flatbuffer", () => {
    const reflectionSchema = fs.readFileSync(`${__dirname}/fixtures/reflection.bfbs`);
    const channel = parseChannel({
      messageEncoding: "flatbuffer",
      schema: { name: "reflection.Schema", encoding: "flatbuffer", data: reflectionSchema },
    });
    const deserialized = channel.deserializer(reflectionSchema) as {
      objects: Record<string, unknown>[];
    };
    expect(deserialized.objects.length).toEqual(10);
    expect(deserialized.objects[0]!.name).toEqual("reflection.Enum");
  });

  it("works with protobuf", () => {
    const fds = FileDescriptorSet.encode(protobufToDescriptor(FileDescriptorSet.root)).finish();
    const channel = parseChannel({
      messageEncoding: "protobuf",
      schema: { name: "google.protobuf.FileDescriptorSet", encoding: "protobuf", data: fds },
    });
    const deserialized = channel.deserializer(fds) as IFileDescriptorSet;
    expect(deserialized.file[0]!.name).toEqual("google_protobuf.proto");
  });

  it("handles protobuf repeated enum having multiple default value aliases", () => {
    /*
    syntax = "proto3";

    enum ExampleEnum {
        option allow_alias = true;
        UNKNOWN = 0;
        WHATEVER = 0;
        FOO = 1;
        BAR = 2;
    }

    message ExampleMessage {
        repeated ExampleEnum data = 1;
    }
    */
    const channel = parseChannel({
      messageEncoding: "protobuf",
      schema: {
        name: "ExampleMessage",
        encoding: "protobuf",
        data: Buffer.from(
          "0A8D010A156578616D706C655F6D6573736167652E70726F746F222C0A0E4578616D706C654D657373616765121A0A046461746118012003280E320C2E4578616D706C65456E756D2A3E0A0B4578616D706C65456E756D120B0A07554E4B4E4F574E1000120C0A085748415445564552100012070A03464F4F100112070A0342415210021A021001620670726F746F33",
          "hex",
        ),
      },
    });
    expect(channel.deserializer(Buffer.from("0A0101", "hex"))).toEqual({ data: [1] });
  });

  it("works with ros1", () => {
    const channel = parseChannel({
      messageEncoding: "ros1",
      schema: {
        name: "foo_msgs/Bar",
        encoding: "ros1msg",
        data: new TextEncoder().encode("string data"),
      },
    });

    const obj = channel.deserializer(new Uint8Array([4, 0, 0, 0, 65, 66, 67, 68]));
    expect(obj).toEqual({ data: "ABCD" });
  });

  it("works with ros2", () => {
    const channel = parseChannel({
      messageEncoding: "cdr",
      schema: {
        name: "foo_msgs/Bar",
        encoding: "ros2msg",
        data: new TextEncoder().encode("string data"),
      },
    });

    const obj = channel.deserializer(new Uint8Array([0, 1, 0, 0, 5, 0, 0, 0, 65, 66, 67, 68, 0]));
    expect(obj).toEqual({ data: "ABCD" });
  });
});
