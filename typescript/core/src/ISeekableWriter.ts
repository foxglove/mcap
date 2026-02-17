import type { IWritable } from "./IWritable.ts";

/**
 * ISeekableWriter describes a writer interface with seek abilities.
 */
export interface ISeekableWriter extends IWritable {
  /** Move the cursor to the given position */
  seek(position: bigint): Promise<void>;
  /** Remove data after the current write position */
  truncate(): Promise<void>;
}
