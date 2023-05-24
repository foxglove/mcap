type VideoCaptureParams = {
  /** Video element to attach to the camera */
  video: HTMLVideoElement;
  /** Frame interval in seconds */
  frameDurationSec: number;
  /** Called when video capture has started */
  onStart: () => void;
  /** Called when an error is encountered */
  onError: (error: Error) => void;
  /** Called when each frame has been converted to an image */
  onFrame: (blob: Blob) => void;
};

/**
 * Prompts the user for camera permission and begins capturing frames
 * @returns A function to stop the capture and clean up resources
 */
export function startVideoCapture(params: VideoCaptureParams): () => void {
  const controller = new AbortController();
  void startVideoCaptureAsync(params, controller.signal);
  return () => {
    controller.abort();
  };
}

async function startVideoCaptureAsync(
  params: VideoCaptureParams,
  signal: AbortSignal
) {
  const { video, onStart, onFrame, onError, frameDurationSec } = params;
  let stream: MediaStream;
  try {
    stream = await navigator.mediaDevices.getUserMedia({ video: true });
  } catch (error) {
    onError(error as Error);
    return;
  }
  if (signal.aborted) {
    return;
  }
  video.srcObject = stream;
  try {
    await video.play();
  } catch (error) {
    // Interrupted: https://developer.chrome.com/blog/play-request-was-interrupted/
    console.error(error);
    return;
  }

  if (!signal.aborted) {
    onStart();
  }

  const canvas = document.createElement("canvas");
  canvas.width = video.videoWidth;
  canvas.height = video.videoHeight;
  const ctx = canvas.getContext("2d");

  let framePromise: Promise<void> | undefined;
  const interval = setInterval(() => {
    if (framePromise) {
      // last frame is not yet complete, skip frame
      return;
    }
    framePromise = new Promise((resolve) => {
      ctx?.drawImage(video, 0, 0);
      canvas.toBlob(
        (blob) => {
          if (blob && !signal.aborted) {
            onFrame(blob);
          }
          resolve();
          framePromise = undefined;
        },
        "image/jpeg",
        0.8
      );
    });
  }, frameDurationSec * 1000);

  const cleanup = () => {
    clearInterval(interval);
    for (const track of stream.getTracks()) {
      track.stop();
    }
  };
  if (signal.aborted) {
    cleanup();
  } else {
    signal.addEventListener("abort", cleanup);
  }
}
