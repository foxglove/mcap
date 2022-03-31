declare module "kaitai-struct" {
  export class KaitaiStream {
    constructor(arrayBuffer: ArrayBuffer, byteOffset?: number);

    readU4le(): number;
    readU8le(): number | bigint;
  }
}

declare module "kaitai-struct-compiler" {
  export default class KaitaiStructCompiler {
    compile(
      langStr: string,
      yaml: unknown,
      importer?: { importYaml(name: string, mode: string): Promise<unknown> },
      debug?: boolean, // eslint-disable-line @foxglove/no-boolean-parameters
    ): Promise<Record<string, string>>;
  }
}
