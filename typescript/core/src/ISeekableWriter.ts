import { IWritable } from "./IWritable";

/**
 * ISeekableWriter describes a writer interface with seek abilities.
 */
export interface ISeekableWriter extends IWritable {
  // Seek the cursor to the given position
  seek(position: bigint): Promise<void>;
}
