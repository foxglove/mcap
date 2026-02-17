import { bytesToHex } from "./bytesToHex.ts";

export function splitMcapRecords(data: Uint8Array): string[] {
  const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
  const result: string[] = [];
  let offset = 0;
  try {
    while (offset < data.length) {
      const opcode = view.getUint8(offset);
      if (opcode === 0x89) {
        result.push(bytesToHex(data.slice(offset, offset + 8)));
        offset += 8;
      } else {
        const length = view.getBigUint64(offset + 1, true);
        const bytes = data.slice(offset, offset + 9 + Number(length));
        const hex = bytesToHex(bytes);
        offset += Number(length) + 9;
        result.push(hex);
      }
    }
  } catch (err) {
    result.push((err as Error).message);
  }
  if (offset > data.length) {
    result.push("read beyond bounds");
  }
  return result;
}
