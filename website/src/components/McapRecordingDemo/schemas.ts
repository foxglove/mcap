import { Time } from "@foxglove/rostime";
import { FoxgloveMessageSchema } from "@foxglove/schemas/internal";

export type RawAudioMessage = {
  /** Timestamp of the audio frame */
  timestamp: Time;

  /** Audio frame data. The samples in the data must be interleaved and little-endian */
  data: Uint8Array;

  /** Audio format. Only 'pcm-s16' is currently supported */
  format: string;

  /** Sample rate in Hz */
  sample_rate: number;

  /** Number of channels in the audio frame */
  number_of_channels: number;
};

export const RawAudioSchema: FoxgloveMessageSchema = {
  type: "message",
  name: "RawAudio",
  description: "A chunk of raw audio bit stream",
  fields: [
    {
      name: "timestamp",
      type: { type: "primitive", name: "time" },
      description: "Timestamp of audio frame",
    },
    {
      name: "data",
      type: { type: "primitive", name: "bytes" },
      description: `Raw audio frame data.`,
    },
    {
      name: "format",
      type: { type: "primitive", name: "string" },
      description: "Audio format.\n\nSupported values: `pcm-s16`.",
    },
    {
      name: "sample_rate",
      type: { type: "primitive", name: "uint32" },
      description: "Number of audio samples per second.",
    },
    {
      name: "number_of_channels",
      type: { type: "primitive", name: "uint32" },
      description: "Number of audio channels in the input.",
    },
  ],
};
