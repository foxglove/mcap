// cspell:ignore Millis

import { fromMillis } from "@foxglove/rostime";
import { RawAudio } from "@foxglove/schemas";

type AudioStreamParams = {
  /** Canvas element to display the audio visualization */
  canvas: HTMLCanvasElement;
  /** Called when the audio stream is available */
  onAudioStream: (stream: MediaStream) => void;
  /** Called when an error is encountered */
  onError: (error: Error) => void;
  /** Optional device ID for specific microphone selection */
  deviceId?: string;
};

/**
 * Prompts the user for microphone permission and displays audio visualization in the provided canvas
 * @returns A function to stop the stream and clean up resources
 */
export function startAudioStream({
  canvas,
  onAudioStream,
  onError,
  deviceId,
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
        // Get time domain data for waveform visualization
        const timeData = new Uint8Array(analyzer.frequencyBinCount);
        analyzer.getByteTimeDomainData(timeData);
        const ctx = canvas.getContext("2d");
        if (ctx) {
          const width = canvas.width;
          const height = canvas.height;

          // Clear with fade effect
          ctx.fillStyle = "rgba(0, 0, 0, 0.3)";
          ctx.fillRect(0, 0, width, height);

          const centerY = height / 2;
          const step = width / timeData.length;
          const scale = height / 256; // Scale factor for 8-bit audio data

          ctx.beginPath();
          ctx.moveTo(0, centerY);

          for (let i = 0; i < timeData.length; i++) {
            const x = i * step;
            // Convert from [0, 255] to [-1, 1] range
            const sample = (timeData[i] ?? 128) - 128;
            const y = centerY + sample * scale;
            ctx.lineTo(x, y);
          }

          ctx.strokeStyle = "#00ffff";
          ctx.lineWidth = 2;
          ctx.stroke();
        }

        update(analyzer);
      });
    };

    const context = new AudioContext();
    void context.resume();

    const constraints: MediaStreamConstraints = {
      audio: deviceId ? { deviceId: { exact: deviceId } } : true,
    };

    navigator.mediaDevices
      .getUserMedia(constraints)
      .then((mediaStream) => {
        if (canceled) {
          return;
        }
        stream = mediaStream;
        onAudioStream(stream);

        // For displaying waveform
        const source = context.createMediaStreamSource(stream);
        const analyzer = context.createAnalyser();
        analyzer.fftSize = 2048;
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
 * Capture uses AudioWorkletNode to get raw PCM data from the audio stream.
 *
 * AudioWorkletNode: https://developer.mozilla.org/en-US/docs/Web/API/AudioWorkletNode
 *
 * This is the modern replacement for the deprecated ScriptProcessorNode.
 */
const supportsWebAudio = (): boolean => {
  return "AudioContext" in window || "webkitAudioContext" in window;
};

/**
 * Determine whether we can capture raw PCM audio.
 */
export const supportsPCMEncoding = async (): Promise<boolean> => {
  if (!supportsWebAudio()) {
    return false;
  }
  return true;
};

type AudioCaptureParams = {
  enablePCM: boolean;
  /** MediaStream from startAudioStream */
  stream: MediaStream;
  /** Called when an audio frame has been encoded */
  onAudioData: (data: RawAudio) => void;
  onError: (error: Error) => void;
};

export function startAudioCapture({
  enablePCM,
  stream,
  onAudioData,
  onError,
}: AudioCaptureParams): (() => void) | undefined {
  if (!enablePCM) {
    onError(new Error("Invariant: expected PCM encoding to be enabled"));
    return undefined;
  }

  if (!supportsWebAudio()) {
    onError(
      new Error(
        "Audio capture not supported: Web Audio API not supported in browser",
      ),
    );
    return undefined;
  }

  const track = stream.getAudioTracks()[0];
  if (!track) {
    onError(new Error("Invariant: expected audio track from stream"));
    return undefined;
  }

  let canceled = false;
  const audioContext = new AudioContext({
    sampleRate: 44100, // Fixed sample rate
  });

  // Create and register the audio worklet processor
  const workletCode = `
    class PCMProcessor extends AudioWorkletProcessor {
      constructor() {
        super();
        this.buffer = new Float32Array(1024);
        this.bufferIndex = 0;
      }

      process(inputs, outputs, parameters) {
        const input = inputs[0];
        if (input.length > 0) {
          const channelData = input[0];

          // Copy samples into our buffer
          for (let i = 0; i < channelData.length; i++) {
            this.buffer[this.bufferIndex++] = channelData[i];

            // If we've filled our buffer, send it and reset
            if (this.bufferIndex >= 1024) {
              this.port.postMessage(this.buffer);
              this.buffer = new Float32Array(1024);
              this.bufferIndex = 0;
            }
          }
        }
        return true;
      }
    }
    registerProcessor('pcm-processor', PCMProcessor);
  `;

  const blob = new Blob([workletCode], { type: "application/javascript" });
  const workletUrl = URL.createObjectURL(blob);

  const source = audioContext.createMediaStreamSource(stream);
  let workletNode: AudioWorkletNode | undefined;

  void audioContext.audioWorklet
    .addModule(workletUrl)
    .then(() => {
      if (canceled) {
        return;
      }

      workletNode = new AudioWorkletNode(audioContext, "pcm-processor", {
        numberOfInputs: 1,
        numberOfOutputs: 1,
        outputChannelCount: [1],
      });

      workletNode.port.onmessage = (event) => {
        if (canceled) {
          return;
        }

        const inputData = event.data as Float32Array;

        // Convert Float32Array to Int16Array (PCM S16)
        const pcmData = new Int16Array(inputData.length);
        for (let i = 0; i < inputData.length; i++) {
          // Convert from float [-1, 1] to int16 [-32768, 32767]
          pcmData[i] = Math.max(-1, Math.min(1, inputData[i] ?? 0)) * 0x7fff;
        }

        onAudioData({
          format: "pcm-s16",
          timestamp: fromMillis(Date.now()),
          data: new Uint8Array(pcmData.buffer),
          sample_rate: 44100,
          number_of_channels: 1,
        });
      };

      // Connect the audio processing chain
      source.connect(workletNode);
      workletNode.connect(audioContext.destination);
    })
    .catch(onError);

  return () => {
    canceled = true;
    if (workletNode) {
      workletNode.disconnect();
    }
    source.disconnect();
    void audioContext.close();
    URL.revokeObjectURL(workletUrl);
    for (const audioTrack of stream.getTracks()) {
      audioTrack.stop();
    }
  };
}
