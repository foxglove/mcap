type CompressedAudioFormat = "opus";
type CompressedAudioType = "key" | "delta";
export type CompressedAudioData = {
  format: CompressedAudioFormat;
  type: CompressedAudioType;
  timestamp: number;
  data: Uint8Array;
  sampleRate: number;
  numberOfChannels: number;

  /** Call this function to release the buffer so it can be reused for new frames */
  release: () => void;
};
