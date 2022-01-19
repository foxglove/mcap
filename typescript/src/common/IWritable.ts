/**
 * IWritable describes a writer interface.
 */
export interface IWritable {
  write(buffer: Uint8Array): Promise<unknown>;
}
