/**
 * IReadable describes a random-access reader interface.
 */
export interface IReadable {
  size(): Promise<bigint>;
  read(offset: bigint, size: bigint): Promise<Uint8Array>;
}
