/**
 * IWritable describes a writer interface.
 */
export interface IWritable {
  // Write buffer to the output
  write(buffer: Uint8Array): Promise<unknown>;

  // The current position in bytes from the start of the output
  position(): number;
}
