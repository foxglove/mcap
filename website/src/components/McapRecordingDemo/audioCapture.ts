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

interface AudioCaptureParams {
  enableOpus: boolean;
  /** MediaStream from startAudioStream */
  stream: MediaStream;
  /** Called when an audio frame has been encoded */
  onAudioData: (data: CompressedAudioData) => void;
  onError: (error: Error) => void;
}

export function startAudioCapture({
  enableOpus,
  stream,
  onAudioData,
  onError,
}: AudioCaptureParams): () => void {
  const framePool: ArrayBuffer[] = [];

  const track = stream.getAudioTracks()[0];
  if (!track) {
    onError(new Error("Invariant: expected audio track"));
    return () => {
      // no op
    };
  }

  const settings = track.getSettings();

  const trackProcessor = new MediaStreamTrackProcessor({
    // TODO: Don't assert
    track: stream.getAudioTracks()[0]!,
  });

  const encoder = new AudioEncoder({
    output: (chunk) => {
      console.log(chunk);
      let buffer = framePool.pop();
      if (!buffer || buffer.byteLength < chunk.byteLength) {
        buffer = new ArrayBuffer(chunk.byteLength);
      }
      chunk.copyTo(buffer);
      onAudioData({
        format: "opus",
        type: chunk.type as CompressedAudioType,
        timestamp: chunk.timestamp,
        data: new Uint8Array(buffer, 0, chunk.byteLength),
        sampleRate: settings.sampleRate ?? 0,
        numberOfChannels: settings.channelCount ?? 0,
        release() {
          // TODO: Don't assert
          framePool.push(buffer!);
        },
      });
    },
    error: (error) => {
      onError(error);
    },
  });
  encoder.configure({
    codec: "opus",
    sampleRate: settings.sampleRate ?? 0,
    numberOfChannels: settings.channelCount ?? 0,
  });

  const reader = trackProcessor.readable.getReader();
  let canceled = false;

  const readAndEncode = () => {
    reader
      .read()
      .then((result) => {
        if (result.done || canceled) {
          return;
        }

        encoder.encode(result.value);

        readAndEncode();
      })
      .catch((error) => {
        onError(error as Error);
      });
  };

  readAndEncode();

  return () => {
    canceled = true;
    encoder.close();
  };
}
