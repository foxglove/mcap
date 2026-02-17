import { RawAudio } from "@foxglove/schemas";
import { useEffect } from "react";

import { startAudioCapture, startAudioStream } from "../audioCapture.ts";
import { useStore } from "../state.ts";

/**
 * Hook to handle audio recording functionality including stream setup and waveform visualization
 * @param audioWaveformRef - Reference to the canvas element for waveform visualization
 */
export function useAudioRecording(
  audioWaveformRef: React.RefObject<HTMLCanvasElement>,
): void {
  const {
    actions,
    recording,
    recordAudio,
    audioStream,
    selectedAudioDeviceId,
  } = useStore();

  const enableMicrophone = recordAudio;

  useEffect(() => {
    if (!enableMicrophone) {
      return;
    }

    const canvasElement = audioWaveformRef.current;
    if (!canvasElement) {
      return;
    }

    // Set canvas size to match its display size
    const rect = canvasElement.getBoundingClientRect();
    canvasElement.width = rect.width * window.devicePixelRatio;
    canvasElement.height = rect.height * window.devicePixelRatio;
    canvasElement.style.width = `${rect.width}px`;
    canvasElement.style.height = `${rect.height}px`;

    const cleanup = startAudioStream({
      canvas: canvasElement,
      deviceId: selectedAudioDeviceId,
      onAudioStream: (stream: MediaStream) => {
        actions.setAudioStream(stream);
      },
      onError: (err: Error) => {
        actions.setAudioError(err);
        console.error(err);
      },
    });

    return () => {
      cleanup();
      actions.setAudioStream(undefined);
      actions.setAudioError(undefined);
    };
  }, [enableMicrophone, selectedAudioDeviceId, actions, audioWaveformRef]);

  useEffect(() => {
    if (!enableMicrophone || !recording || !audioStream) {
      return;
    }

    const canvasElement = audioWaveformRef.current;
    if (canvasElement) {
      // Set canvas size to match its display size
      const rect = canvasElement.getBoundingClientRect();
      canvasElement.width = rect.width * window.devicePixelRatio;
      canvasElement.height = rect.height * window.devicePixelRatio;
      canvasElement.style.width = `${rect.width}px`;
      canvasElement.style.height = `${rect.height}px`;
    }

    const cleanup = startAudioCapture({
      enablePCM: recordAudio,
      stream: audioStream,
      onAudioData: (data: RawAudio) => {
        actions.addAudioData(data);
      },
      onError: (error: Error) => {
        actions.setAudioError(error);
      },
    });

    const currentAudioWaveform = audioWaveformRef.current;

    return () => {
      cleanup?.();
      // Clear canvas on cleanup
      if (currentAudioWaveform) {
        const ctx = currentAudioWaveform.getContext("2d");
        if (ctx) {
          ctx.clearRect(
            0,
            0,
            currentAudioWaveform.width,
            currentAudioWaveform.height,
          );
        }
      }
    };
  }, [
    enableMicrophone,
    audioStream,
    recording,
    recordAudio,
    actions,
    audioWaveformRef,
  ]);
}
