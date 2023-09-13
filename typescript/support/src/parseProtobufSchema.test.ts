import { parseProtobufSchema } from "./parseProtobufSchema";

describe("parseProtobufSchema", () => {
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
    const channel = parseProtobufSchema(
      "ExampleMessage",
      Buffer.from(
        // cspell:disable-next-line
        "0A8D010A156578616D706C655F6D6573736167652E70726F746F222C0A0E4578616D706C654D657373616765121A0A046461746118012003280E320C2E4578616D706C65456E756D2A3E0A0B4578616D706C65456E756D120B0A07554E4B4E4F574E1000120C0A085748415445564552100012070A03464F4F100112070A0342415210021A021001620670726F746F33",
        "hex",
      ),
    );
    expect(channel.deserialize(Buffer.from("0A0101", "hex"))).toEqual({ data: [1] });
    expect(channel.datatypes).toEqual(
      new Map([
        [
          "ExampleMessage",
          {
            definitions: [
              { isConstant: true, name: "UNKNOWN", type: "int32", value: 0 },
              { isConstant: true, name: "WHATEVER", type: "int32", value: 0 },
              { isConstant: true, name: "FOO", type: "int32", value: 1 },
              { isConstant: true, name: "BAR", type: "int32", value: 2 },
              { name: "data", type: "int32" },
            ],
          },
        ],
      ]),
    );
  });

  it("allows modifying deserialization and datatypes", () => {
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
    const channel = parseProtobufSchema(
      "ExampleMessage",
      Buffer.from(
        // cspell:disable-next-line
        "0A8D010A156578616D706C655F6D6573736167652E70726F746F222C0A0E4578616D706C654D657373616765121A0A046461746118012003280E320C2E4578616D706C65456E756D2A3E0A0B4578616D706C65456E756D120B0A07554E4B4E4F574E1000120C0A085748415445564552100012070A03464F4F100112070A0342415210021A021001620670726F746F33",
        "hex",
      ),
      {
        processRootType(rootType) {
          rootType.fieldsById[1]!.name = "renamed_data";
          return rootType;
        },
        processMessageDefinitions(definitions) {
          definitions
            .get("ExampleMessage")!
            .definitions.find((def) => def.name === "renamed_data")!.type = "float64";
          return definitions;
        },
      },
    );
    expect(channel.deserialize(Buffer.from("0A0101", "hex"))).toEqual({ renamed_data: [1] });
    expect(channel.datatypes).toEqual(
      new Map([
        [
          "ExampleMessage",
          {
            definitions: [
              { isConstant: true, name: "UNKNOWN", type: "int32", value: 0 },
              { isConstant: true, name: "WHATEVER", type: "int32", value: 0 },
              { isConstant: true, name: "FOO", type: "int32", value: 1 },
              { isConstant: true, name: "BAR", type: "int32", value: 2 },
              { name: "renamed_data", type: "float64" },
            ],
          },
        ],
      ]),
    );
  });

  it("handles protobuf int64 values", () => {
    /*
    syntax = "proto3";

    message Int64Test {
      int64 int64 = 1;
      uint64 uint64 = 2;
      sint64 sint64 = 3;
      fixed64 fixed64 = 4;
      sfixed64 sfixed64 = 5;
      map<int64, int64> int64map = 6;
      map<uint64, uint64> uint64map = 7;
      map<sint64, sint64> sint64map = 8;
      map<fixed64, fixed64> fixed64map = 9;
      map<sfixed64, sfixed64> sfixed64map = 10;
      repeated Nested nested = 11;
    }

    message Nested {
      int64 int64 = 1;
      uint64 uint64 = 2;
      sint64 sint64 = 3;
      fixed64 fixed64 = 4;
      sfixed64 sfixed64 = 5;
      map<int64, int64> int64map = 6;
      map<uint64, uint64> uint64map = 7;
      map<sint64, sint64> sint64map = 8;
      map<fixed64, fixed64> fixed64map = 9;
      map<sfixed64, sfixed64> sfixed64map = 10;
    }
    */
    const channel = parseProtobufSchema(
      "Int64Test",
      Buffer.from(
        // cspell:disable-next-line
        "CvILCg9JbnQ2NFRlc3QucHJvdG8igwYKCUludDY0VGVzdBIUCgVpbnQ2NBgBIAEoA1IFaW50NjQSFgoGdWludDY0GAIgASgEUgZ1aW50NjQSFgoGc2ludDY0GAMgASgSUgZzaW50NjQSGAoHZml4ZWQ2NBgEIAEoBlIHZml4ZWQ2NBIaCghzZml4ZWQ2NBgFIAEoEFIIc2ZpeGVkNjQSNAoIaW50NjRtYXAYBiADKAsyGC5JbnQ2NFRlc3QuSW50NjRtYXBFbnRyeVIIaW50NjRtYXASNwoJdWludDY0bWFwGAcgAygLMhkuSW50NjRUZXN0LlVpbnQ2NG1hcEVudHJ5Ugl1aW50NjRtYXASNwoJc2ludDY0bWFwGAggAygLMhkuSW50NjRUZXN0LlNpbnQ2NG1hcEVudHJ5UglzaW50NjRtYXASOgoKZml4ZWQ2NG1hcBgJIAMoCzIaLkludDY0VGVzdC5GaXhlZDY0bWFwRW50cnlSCmZpeGVkNjRtYXASPQoLc2ZpeGVkNjRtYXAYCiADKAsyGy5JbnQ2NFRlc3QuU2ZpeGVkNjRtYXBFbnRyeVILc2ZpeGVkNjRtYXASHwoGbmVzdGVkGAsgAygLMgcuTmVzdGVkUgZuZXN0ZWQaOwoNSW50NjRtYXBFbnRyeRIQCgNrZXkYASABKANSA2tleRIUCgV2YWx1ZRgCIAEoA1IFdmFsdWU6AjgBGjwKDlVpbnQ2NG1hcEVudHJ5EhAKA2tleRgBIAEoBFIDa2V5EhQKBXZhbHVlGAIgASgEUgV2YWx1ZToCOAEaPAoOU2ludDY0bWFwRW50cnkSEAoDa2V5GAEgASgSUgNrZXkSFAoFdmFsdWUYAiABKBJSBXZhbHVlOgI4ARo9Cg9GaXhlZDY0bWFwRW50cnkSEAoDa2V5GAEgASgGUgNrZXkSFAoFdmFsdWUYAiABKAZSBXZhbHVlOgI4ARo+ChBTZml4ZWQ2NG1hcEVudHJ5EhAKA2tleRgBIAEoEFIDa2V5EhQKBXZhbHVlGAIgASgQUgV2YWx1ZToCOAEi0AUKBk5lc3RlZBIUCgVpbnQ2NBgBIAEoA1IFaW50NjQSFgoGdWludDY0GAIgASgEUgZ1aW50NjQSFgoGc2ludDY0GAMgASgSUgZzaW50NjQSGAoHZml4ZWQ2NBgEIAEoBlIHZml4ZWQ2NBIaCghzZml4ZWQ2NBgFIAEoEFIIc2ZpeGVkNjQSMQoIaW50NjRtYXAYBiADKAsyFS5OZXN0ZWQuSW50NjRtYXBFbnRyeVIIaW50NjRtYXASNAoJdWludDY0bWFwGAcgAygLMhYuTmVzdGVkLlVpbnQ2NG1hcEVudHJ5Ugl1aW50NjRtYXASNAoJc2ludDY0bWFwGAggAygLMhYuTmVzdGVkLlNpbnQ2NG1hcEVudHJ5UglzaW50NjRtYXASNwoKZml4ZWQ2NG1hcBgJIAMoCzIXLk5lc3RlZC5GaXhlZDY0bWFwRW50cnlSCmZpeGVkNjRtYXASOgoLc2ZpeGVkNjRtYXAYCiADKAsyGC5OZXN0ZWQuU2ZpeGVkNjRtYXBFbnRyeVILc2ZpeGVkNjRtYXAaOwoNSW50NjRtYXBFbnRyeRIQCgNrZXkYASABKANSA2tleRIUCgV2YWx1ZRgCIAEoA1IFdmFsdWU6AjgBGjwKDlVpbnQ2NG1hcEVudHJ5EhAKA2tleRgBIAEoBFIDa2V5EhQKBXZhbHVlGAIgASgEUgV2YWx1ZToCOAEaPAoOU2ludDY0bWFwRW50cnkSEAoDa2V5GAEgASgSUgNrZXkSFAoFdmFsdWUYAiABKBJSBXZhbHVlOgI4ARo9Cg9GaXhlZDY0bWFwRW50cnkSEAoDa2V5GAEgASgGUgNrZXkSFAoFdmFsdWUYAiABKAZSBXZhbHVlOgI4ARo+ChBTZml4ZWQ2NG1hcEVudHJ5EhAKA2tleRgBIAEoEFIDa2V5EhQKBXZhbHVlGAIgASgQUgV2YWx1ZToCOAFiBnByb3RvMw==",
        "base64",
      ),
    );
    expect(
      channel.deserialize(
        Buffer.from(
          // cspell:disable-next-line
          "088c95f0c4c5a9d28ff20110bc85a0cfc8e0c8e38a0118e7d59ff6f4acdbe01b21bc02e8890423c78a298c0a9c584c491ff23216088c95f0c4c5a9d28ff201108c95f0c4c5a9d28ff2013a1608bc85a0cfc8e0c8e38a0110bc85a0cfc8e0c8e38a01421408e7d59ff6f4acdbe01b10e7d59ff6f4acdbe01b4a1209bc02e8890423c78a11bc02e8890423c78a5212098c0a9c584c491ff2118c0a9c584c491ff25aa001088c95f0c4c5a9d28ff20110bc85a0cfc8e0c8e38a0118e7d59ff6f4acdbe01b21bc02e8890423c78a298c0a9c584c491ff23216088c95f0c4c5a9d28ff201108c95f0c4c5a9d28ff2013a1608bc85a0cfc8e0c8e38a0110bc85a0cfc8e0c8e38a01421408e7d59ff6f4acdbe01b10e7d59ff6f4acdbe01b4a1209bc02e8890423c78a11bc02e8890423c78a5212098c0a9c584c491ff2118c0a9c584c491ff2",
          "hex",
        ),
      ),
    ).toEqual({
      int64: -999999999999997300n,
      uint64: 10000000000000000700n,
      sint64: -999999999999997300n,
      fixed64: 10000000000000000700n,
      sfixed64: -999999999999997300n,
      int64map: [{ key: -999999999999997300n, value: -999999999999997300n }],
      uint64map: [{ key: 10000000000000000700n, value: 10000000000000000700n }],
      sint64map: [{ key: -999999999999997300n, value: -999999999999997300n }],
      fixed64map: [{ key: 10000000000000000700n, value: 10000000000000000700n }],
      sfixed64map: [{ key: -999999999999997300n, value: -999999999999997300n }],
      nested: [
        {
          int64: -999999999999997300n,
          uint64: 10000000000000000700n,
          sint64: -999999999999997300n,
          fixed64: 10000000000000000700n,
          sfixed64: -999999999999997300n,
          int64map: [{ key: -999999999999997300n, value: -999999999999997300n }],
          uint64map: [{ key: 10000000000000000700n, value: 10000000000000000700n }],
          sint64map: [{ key: -999999999999997300n, value: -999999999999997300n }],
          fixed64map: [{ key: 10000000000000000700n, value: 10000000000000000700n }],
          sfixed64map: [{ key: -999999999999997300n, value: -999999999999997300n }],
        },
      ],
    });
  });
});
