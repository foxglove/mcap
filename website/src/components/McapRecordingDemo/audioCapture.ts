type AudioStreamParams = {
  /** Progress element to display the volume level */
  progress: HTMLProgressElement;
  /** Called when the audio stream is available */
  onAudioStream: (stream: MediaStream) => void;
  /** Called when an error is encountered */
  onError: (error: Error) => void;
};

/**
 * Prompts the user for microphone permission and displays audio volume in the provided <progress> element
 * @returns A function to stop the stream and clean up resources
 */
export function startAudioStream({
  progress,
  onAudioStream,
  onError,
}: AudioStreamParams): () => void {
  let canceled = false;
  let stream: MediaStream | undefined;
  let animationID = 0;

  // Although TypeScript does not believe mediaDevices is ever undefined, it may be in practice
  // (e.g. in Safari)
  if (typeof navigator.mediaDevices !== "object") {
    onError(new Error("navigator.mediaDevices is not defined"));
  } else {
    const update = (analyzer: AnalyserNode) => {
      if (canceled) {
        return;
      }

      animationID = requestAnimationFrame(() => {
        // Update the progress bar to show the audio level
        const fbcArray = new Uint8Array(analyzer.frequencyBinCount);
        analyzer.getByteFrequencyData(fbcArray);
        const level =
          fbcArray.reduce((acc, val) => acc + val, 0) / fbcArray.length;
        progress.value = level / 100;

        update(analyzer);
      });
    };

    const context = new AudioContext();
    void context.resume();
    navigator.mediaDevices
      .getUserMedia({ audio: true })
      .then((mediaStream) => {
        if (canceled) {
          return;
        }
        stream = mediaStream;
        onAudioStream(stream);

        // For displaying volume level
        const source = context.createMediaStreamSource(stream);
        const analyzer = context.createAnalyser();
        source.connect(analyzer);

        update(analyzer);
      })
      .catch((err) => {
        if (canceled) {
          return;
        }

        onError(
          new Error(
            `${(
              err as Error
            ).toString()}. Ensure microphone permissions are enabled.`,
          ),
        );
      });
  }

  return () => {
    canceled = true;
    cancelAnimationFrame(animationID);
    for (const track of stream?.getTracks() ?? []) {
      track.stop();
    }
  };
}

/**
 * Determine whether required Web Audio APIs are supported.
 *
 * Capture uses MediaStreamTrackProcessor and AudioEncoder to
 * read and encode audio frames.
 *
 * MediaStreamTrackProcessor: https://developer.mozilla.org/en-US/docs/Web/API/MediaStreamTrackProcessor#browser_compatibility
 * AudioEncoder: https://developer.mozilla.org/en-US/docs/Web/API/AudioEncoder#browser_compatibility
 *
 * As of 2024-12-15, Chrome and Edge have support
 */
const supportsMediaCaptureTransformAndWebCodecs = (): boolean => {
  return "MediaStreamTrackProcessor" in window && "AudioEncoder" in window;
};

const DEFAULT_OPUS_CONFIG: AudioEncoderConfig = {
  codec: "opus",
  sampleRate: 48000,
  numberOfChannels: 1,
};

/**
 * Determine whether AudioEncoder can be used to encode audio with Opus.
 */
export const supportsOpusEncoding = async (): Promise<boolean> => {
  if (!supportsMediaCaptureTransformAndWebCodecs()) {
    return false;
  }

  const support = await AudioEncoder.isConfigSupported(DEFAULT_OPUS_CONFIG);
  return support.supported === true;
};

const DEFAULT_MP4A_CONFIG: AudioEncoderConfig = {
  codec: "mp4a.40.2",
  numberOfChannels: 1,
  sampleRate: 48000,
};

/**
 * Determine whether AudioEncoder can be used to encode audio with mp4a.40.2.
 */
export const supportsMP4AEncoding = async (): Promise<boolean> => {
  if (!supportsMediaCaptureTransformAndWebCodecs()) {
    return false;
  }

  const support = await AudioEncoder.isConfigSupported(DEFAULT_MP4A_CONFIG);
  return support.supported === true;
};

const configureEncoder = ({
  config,
  framePool,
  onAudioData,
  onError,
}: {
  config: AudioEncoderConfig;
  framePool: ArrayBuffer[];
  onAudioData: (data: CompressedAudioData) => void;
  onError: (error: Error) => void;
}): AudioEncoder => {
  const encoder = new AudioEncoder({
    output: (chunk) => {
      let buffer = framePool.pop();
      if (!buffer || buffer.byteLength < chunk.byteLength) {
        buffer = new ArrayBuffer(chunk.byteLength);
      }
      chunk.copyTo(buffer);
      onAudioData({
        format: config.codec as CompressedAudioFormat,
        type: chunk.type as CompressedAudioType,
        timestamp: chunk.timestamp,
        data: new Uint8Array(buffer, 0, chunk.byteLength),
        sampleRate: config.sampleRate,
        numberOfChannels: config.numberOfChannels,
        release() {
          if (buffer) {
            framePool.push(buffer);
          }
        },
      });
    },
    error: (error) => {
      onError(error);
    },
  });
  encoder.configure(config);

  return encoder;
};

type CompressedAudioFormat = "opus" | "mp4a.40.2";
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

interface AudioCaptureParams {
  enableMP4A: boolean;
  enableOpus: boolean;
  /** MediaStream from startAudioStream */
  stream: MediaStream;
  /** Called when an audio frame has been encoded */
  onAudioData: (data: CompressedAudioData) => void;
  onError: (error: Error) => void;
}

export function startAudioCapture({
  enableMP4A,
  enableOpus,
  stream,
  onAudioData,
  onError,
}: AudioCaptureParams): (() => void) | undefined {
  if (!enableMP4A && !enableOpus) {
    onError(new Error("Invariant: expected Opus encoding to be enabled"));
    return undefined;
  }

  if (!supportsMediaCaptureTransformAndWebCodecs()) {
    onError(
      new Error(
        "Audio capture not supported: MediaStreamTrackProcessor and AudioEncoder not supported in browser",
      ),
    );
    return undefined;
  }

  const track = stream.getAudioTracks()[0];
  if (!track) {
    onError(new Error("Invariant: expected audio track from stream"));
    return undefined;
  }

  const trackProcessor = new MediaStreamTrackProcessor({
    track,
  });

  const framePool: ArrayBuffer[] = [];

  const encoders = [
    ...(enableMP4A
      ? [
          configureEncoder({
            config: DEFAULT_MP4A_CONFIG,
            framePool,
            onAudioData,
            onError,
          }),
        ]
      : []),
    ...(enableOpus
      ? [
          configureEncoder({
            config: DEFAULT_OPUS_CONFIG,
            framePool,
            onAudioData,
            onError,
          }),
        ]
      : []),
  ];

  const reader = trackProcessor.readable.getReader();
  let canceled = false;

  const readAndEncode = () => {
    if (canceled) {
      return;
    }

    reader
      .read()
      .then((result) => {
        if (result.done || canceled) {
          return;
        }

        for (const encoder of encoders) {
          encoder.encode(result.value);
        }

        readAndEncode();
      })
      .catch((error) => {
        onError(error as Error);
      });
  };

  readAndEncode();

  return () => {
    canceled = true;
    for (const encoder of encoders) {
      encoder.close();
    }
  };
}
