/**
 * Compute CRC32 lookup tables as described at:
 * https://github.com/komrad36/CRC#option-6-1-byte-tabular
 *
 * An iteration of CRC computation can be performed on 8 bits of input at once. By pre-computing a
 * table of the values of CRC(?) for all 2^8 = 256 possible byte values, during the final
 * computation we can replace a loop over 8 bits with a single lookup in the table.
 *
 * For further speedup, we can also pre-compute the values of CRC(?0) for all possible bytes when a
 * zero byte is appended. Then we can process two bytes of input at once by computing CRC(AB) =
 * CRC(A0) ^ CRC(B), using one lookup in the CRC(?0) table and one lookup in the CRC(?) table.
 *
 * The same technique applies for any number of bytes to be processed at once, although the speed
 * improvements diminish.
 *
 * @param polynomial The binary representation of the polynomial to use (reversed, i.e. most
 * significant bit represents x^0).
 * @param numTables The number of bytes of input that will be processed at once.
 */
export function crc32GenerateTables({ polynomial, numTables }: { polynomial: number; numTables: number }): Uint32Array {
    const table = new Uint32Array(256 * numTables);
    for (let i = 0; i < 256; i++) {
        let r = i;
        r = ((r & 1) * polynomial) ^ (r >>> 1);
        r = ((r & 1) * polynomial) ^ (r >>> 1);
        r = ((r & 1) * polynomial) ^ (r >>> 1);
        r = ((r & 1) * polynomial) ^ (r >>> 1);
        r = ((r & 1) * polynomial) ^ (r >>> 1);
        r = ((r & 1) * polynomial) ^ (r >>> 1);
        r = ((r & 1) * polynomial) ^ (r >>> 1);
        r = ((r & 1) * polynomial) ^ (r >>> 1);
        table[i] = r;
    }
    for (let i = 256; i < table.length; i++) {
        const value = table[i - 256]!;
        table[i] = table[value & 0xff]! ^ (value >>> 8);
    }
    return table;
}

const CRC32_TABLE = crc32GenerateTables({ polynomial: 0xedb88320, numTables: 8 });

/**
 * Initialize a CRC32 to all 1 bits.
 */
export function crc32Init(): number {
    return ~0;
}

/**
 * Update a streaming CRC32 calculation.
 *
 * For performance, this implementation processes the data 8 bytes at a time, using the algorithm
 * presented at: https://github.com/komrad36/CRC#option-9-8-byte-tabular
 */
export function crc32Update(prev: number, data: ArrayBufferView): number {
    const byteLength = data.byteLength;
    const view = new DataView(data.buffer, data.byteOffset, byteLength);
    let r = prev;
    // Process 8 bytes (2 uint32s) at a time.
    const endOffset = byteLength - 8;
    let offset = 0;
    while (offset < endOffset) {
        r ^= view.getUint32(offset, true);
        offset += 4;
        const r2 = view.getUint32(offset, true);
        offset += 4;
        r =
            (CRC32_TABLE[((r2 >>> 24) & 0xff)]! | 0) ^
            (CRC32_TABLE[256 + ((r2 >>> 16) & 0xff)]! | 0) ^
            (CRC32_TABLE[512 + ((r2 >>> 8) & 0xff)]! | 0) ^
            (CRC32_TABLE[768 + (r2 & 0xff)]! | 0) ^
            (CRC32_TABLE[1024 + ((r >>> 24) & 0xff)]! | 0) ^
            (CRC32_TABLE[1280 + ((r >>> 16) & 0xff)]! | 0) ^
            (CRC32_TABLE[1536 + ((r >>> 8) & 0xff)]! | 0) ^
            (CRC32_TABLE[1792 + (r & 0xff)]! | 0);
    }
    // Process any remaining bytes one by one. (Perf note: inexplicably, using a temporary variable
    // `i` rather than reusing `offset` here is faster in V8.)
    for (let i = offset; i < byteLength; i++) {
        r = CRC32_TABLE[(r ^ view.getUint8(i)) & 0xff]! ^ (r >>> 8);
    }
    return r;
}

/**
 * Finalize a CRC32 by inverting the output value. An unsigned right-shift of 0 is used to ensure the result is a positive number.
 */
export function crc32Final(prev: number): number {
    return (prev ^ ~0) >>> 0;
}

/**
 * Calculate a one-shot CRC32. If the data is being accumulated incrementally, use the functions
 * `crc32Init`, `crc32Update`, and `crc32Final` instead.
 */
export function crc32(data: ArrayBufferView): number {
    return crc32Final(crc32Update(crc32Init(), data));
}
