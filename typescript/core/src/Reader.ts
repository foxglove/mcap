import { getBigUint64 } from "./common/getBigUint64";

export default class Reader {
  private view: DataView;
  offset: number;
  private textDecoder = new TextDecoder();

  constructor(view: DataView, offset = 0) {
    this.view = view;
    this.offset = offset;
  }

  uint8(): number {
    const value = this.view.getUint8(this.offset);
    this.offset += 1;
    return value;
  }

  uint16(): number {
    const value = this.view.getUint16(this.offset, true);
    this.offset += 2;
    return value;
  }

  uint32(): number {
    const value = this.view.getUint32(this.offset, true);
    this.offset += 4;
    return value;
  }

  uint64(): bigint {
    const value = getBigUint64.call(this.view, this.offset, true);
    this.offset += 8;
    return value;
  }

  string(): string {
    const length = this.uint32();
    if (this.offset + length > this.view.byteLength) {
      throw new Error(`String length ${length} exceeds bounds of buffer`);
    }
    const value = this.textDecoder.decode(
      new Uint8Array(this.view.buffer, this.view.byteOffset + this.offset, length),
    );
    this.offset += length;
    return value;
  }

  keyValuePairs<K, V>(readKey: (reader: Reader) => K, readValue: (reader: Reader) => V): [K, V][] {
    const length = this.uint32();
    if (this.offset + length > this.view.byteLength) {
      throw new Error(`Key-value pairs length ${length} exceeds bounds of buffer`);
    }
    const result: [K, V][] = [];
    const endOffset = this.offset + length;
    try {
      while (this.offset < endOffset) {
        result.push([readKey(this), readValue(this)]);
      }
    } catch (err) {
      throw new Error(`Error reading key-value pairs: ${(err as Error).message}`);
    }
    if (this.offset !== endOffset) {
      throw new Error(
        `Key-value pairs length (${
          this.offset - endOffset + length
        }) greater than expected (${length})`,
      );
    }
    return result;
  }

  map<K, V>(readKey: (reader: Reader) => K, readValue: (reader: Reader) => V): Map<K, V> {
    const length = this.uint32();
    if (this.offset + length > this.view.byteLength) {
      throw new Error(`Map length ${length} exceeds bounds of buffer`);
    }
    const result = new Map<K, V>();
    const endOffset = this.offset + length;
    try {
      while (this.offset < endOffset) {
        const key = readKey(this);
        const value = readValue(this);
        const existingValue = result.get(key);
        if (existingValue != undefined) {
          throw new Error(
            `Duplicate key ${String(key)} (${String(existingValue)} vs ${String(value)})`,
          );
        }
        result.set(key, value);
      }
    } catch (err) {
      throw new Error(`Error reading map: ${(err as Error).message}`);
    }
    if (this.offset !== endOffset) {
      throw new Error(
        `Map length (${this.offset - endOffset + length}) greater than expected (${length})`,
      );
    }
    return result;
  }
}
