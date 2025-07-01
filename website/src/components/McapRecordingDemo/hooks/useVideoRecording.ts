import { useEffect } from "react";

import { useStore } from "../state";
import {
  startVideoCapture,
  startVideoStream,
  CompressedVideoFrame,
} from "../videoCapture";

/**
 * Hook to handle video recording functionality including stream setup and frame capture
 * @param videoContainerRef - Reference to the video container element
 */
export function useVideoRecording(
  videoContainerRef: React.RefObject<HTMLDivElement>,
): void {
  const {
    actions,
    recording,
    videoStarted,
    enabledVideoFormats,
    selectedCameraDeviceId,
  } = useStore();

  const enableCamera = enabledVideoFormats.size > 0;

  // Automatically pause recording after 30 seconds to avoid unbounded growth
  useEffect(() => {
    if (!recording) {
      return;
    }
    const timeout = setTimeout(() => {
      actions.setRecording({ isRecording: false });
    }, 30000);
    return () => {
      clearTimeout(timeout);
    };
  }, [recording, actions]);

  useEffect(() => {
    const videoContainer = videoContainerRef.current;
    if (!videoContainer || !enableCamera) {
      return;
    }

    // Remove existing video element if any
    const existingVideo = videoContainer.querySelector("video");
    if (existingVideo) {
      existingVideo.remove();
    }

    // Create new video element
    const video = document.createElement("video");
    video.muted = true;
    video.playsInline = true;
    videoContainer.appendChild(video);

    const cleanup = startVideoStream({
      video,
      deviceId: selectedCameraDeviceId,
      onStart: () => {
        actions.setVideoStarted({ isStarted: true });
      },
      onError: (err: Error) => {
        actions.setVideoError(err);
        console.error(err);
      },
    });

    return () => {
      cleanup();
      video.remove();
      actions.setVideoStarted({ isStarted: false });
      actions.setVideoError(undefined);
    };
  }, [enableCamera, selectedCameraDeviceId, actions, videoContainerRef]);

  useEffect(() => {
    const videoContainer = videoContainerRef.current;
    if (!recording || !videoContainer || !videoStarted) {
      return;
    }
    if (!enableCamera) {
      return;
    }

    const video = videoContainer.querySelector("video");
    if (!video) {
      return;
    }

    const stopCapture = startVideoCapture({
      video,
      enableAV1: enabledVideoFormats.has("av1"),
      enableVP9: enabledVideoFormats.has("vp9"),
      enableH265: enabledVideoFormats.has("h265"),
      enableH264: enabledVideoFormats.has("h264"),
      enableJpeg: enabledVideoFormats.has("jpeg"),
      frameDurationSec: 1 / 30,
      onJpegFrame: (blob: Blob) => {
        actions.addJpegFrame(blob);
      },
      onVideoFrame: (frame: CompressedVideoFrame) => {
        actions.addVideoFrame(frame);
      },
      onError: (err: Error) => {
        actions.setVideoError(err);
        console.error(err);
      },
    });
    return () => {
      stopCapture();
    };
  }, [
    enableCamera,
    recording,
    videoStarted,
    enabledVideoFormats,
    actions,
    videoContainerRef,
  ]);
}
