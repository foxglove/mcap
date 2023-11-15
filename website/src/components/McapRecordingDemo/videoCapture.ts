type VideoStreamParams = {
  /** Video element to attach to the camera */
  video: HTMLVideoElement;
  /** Called when video stream has started */
  onStart: () => void;
  /** Called when an error is encountered */
  onError: (error: Error) => void;
};

/**
 * Prompts the user for camera permission and displays video in the provided <video> element
 * @returns A function to stop the stream and clean up resources
 */
export function startVideoStream(params: VideoStreamParams): () => void {
  let canceled = false;
  let stream: MediaStream | undefined;
  navigator.mediaDevices
    .getUserMedia({ video: true })
    .then(async (videoStream) => {
      if (canceled) {
        return;
      }
      stream = videoStream;
      params.video.srcObject = videoStream;
      await params.video.play();
      if (canceled as boolean) {
        return;
      }
      params.onStart();
    })
    .catch((err) => {
      if (canceled) {
        return;
      }
      params.onError(err as Error);
    });

  return () => {
    canceled = true;
    if (stream) {
      for (const track of stream.getTracks()) {
        track.stop();
      }
    }
  };
}

type VideoCaptureParams = {
  /** Video element to capture */
  video: HTMLVideoElement;
  /** Frame interval in seconds */
  frameDurationSec: number;
  /** Called when each frame has been converted to an image */
  onFrame: (blob: Blob) => void;
};

/**
 * Begins capturing frames from a <video> element
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
  signal: AbortSignal,
) {
  const { video, onFrame, frameDurationSec } = params;
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
        0.8,
      );
    });
  }, frameDurationSec * 1000);

  const cleanup = () => {
    clearInterval(interval);
  };
  if (signal.aborted) {
    cleanup();
  } else {
    signal.addEventListener("abort", cleanup);
  }
}
