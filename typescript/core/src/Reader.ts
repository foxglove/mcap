import { getBigUint64 } from "./getBigUint64.ts";

// For performance reasons we use a single TextDecoder instance whose internal state is merely
// the encoding (defaults to UTF-8). This means that a TextDecoder.decode() call is not affected
// be previous calls.
const textDecoder = new TextDecoder();

export default class Reader {
  #view: DataView;
  #viewU8: Uint8Array;
  offset: number;

  constructor(view: DataView, offset = 0) {
    this.#view = view;
    this.#viewU8 = new Uint8Array(view.buffer, view.byteOffset, view.byteLength);
    this.offset = offset;
  }

  // Should be ~identical to the constructor, it allows us to reinitialize the reader when
  // the view changes,  without creating a new instance, avoiding allocation / GC overhead
  reset(view: DataView, offset = 0): void {
    this.#view = view;
    this.#viewU8 = new Uint8Array(view.buffer, view.byteOffset, view.byteLength);
    this.offset = offset;
  }

  bytesRemaining(): number {
    return this.#viewU8.length - this.offset;
  }

  uint8(): number {
    const value = this.#view.getUint8(this.offset);
    this.offset += 1;
    return value;
  }

  uint16(): number {
    const value = this.#view.getUint16(this.offset, true);
    this.offset += 2;
    return value;
  }

  uint32(): number {
    const value = this.#view.getUint32(this.offset, true);
    this.offset += 4;
    return value;
  }

  uint64(): bigint {
    const value = getBigUint64.call(this.#view, this.offset, true);
    this.offset += 8;
    return value;
  }

  string(): string {
    const length = this.uint32();
    if (length === 0) {
      return "";
    } else if (length > this.bytesRemaining()) {
      throw new Error(`String length ${length} exceeds bounds of buffer`);
    }
    return textDecoder.decode(this.u8ArrayBorrow(length));
  }

  keyValuePairs<K, V>(readKey: (reader: Reader) => K, readValue: (reader: Reader) => V): [K, V][] {
    const length = this.uint32();
    if (this.offset + length > this.#view.byteLength) {
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
    if (this.offset + length > this.#view.byteLength) {
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

  // Read a borrowed Uint8Array, useful temp references or borrow semantics
  u8ArrayBorrow(length: number): Uint8Array {
    const result = this.#viewU8.subarray(this.offset, this.offset + length);
    this.offset += length;
    return result;
  }

  // Read a copied Uint8Array from the underlying buffer, use when you need to keep the data around
  u8ArrayCopy(length: number): Uint8Array {
    const result = this.#viewU8.slice(this.offset, this.offset + length);
    this.offset += length;
    return result;
  }
}
