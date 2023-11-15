// cspell:word annexb

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
  // Although TypeScript does not believe mediaDevices is ever undefined, it may be in practice
  // (e.g. in Safari)
  if (typeof navigator.mediaDevices !== "object") {
    params.onError(new Error("navigator.mediaDevices is not defined"));
  } else {
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
  }

  return () => {
    canceled = true;
    if (stream) {
      for (const track of stream.getTracks()) {
        track.stop();
      }
    }
  };
}

export type H264Frame = {
  /** Annex B formatted data */
  data: Uint8Array;
  /** Call this function to release the buffer so it can be reused for new frames */
  release: () => void;
};

type VideoCaptureParams = {
  /** Video element to capture */
  video: HTMLVideoElement;
  /** Frame interval in seconds */
  frameDurationSec: number;
  /** Called when a frame has been converted to an image */
  onJpegFrame: (blob: Blob) => void;
  /** Called when a video frame has been encoded */
  onH264Frame: (frame: H264Frame) => void;
  /** Called when an error is encountered */
  onError: (error: Error) => void;
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

/**
 * Sometimes `VideoEncoder.isConfigSupported` returns true but the encoder does not actually output
 * frames (looking at you, Safari). This function tries actually encoding a frame and making sure
 * that the encoder can output a chunk.
 */
async function isEncoderConfigActuallySupported(config: VideoEncoderConfig) {
  try {
    if ((await VideoEncoder.isConfigSupported(config)).supported !== true) {
      return false;
    }
    let outputAnyFrames = false as boolean;
    let hadErrors = false as boolean;
    const encoder = new VideoEncoder({
      output() {
        outputAnyFrames = true;
      },
      error(err) {
        hadErrors = true;
        console.log(
          "VideoEncoder error during compatibility detection:",
          config,
          err,
        );
      },
    });
    encoder.configure(config);
    const bitmap = await createImageBitmap(new ImageData(1, 1));
    const frame = new VideoFrame(bitmap, { timestamp: 0 });
    bitmap.close();
    encoder.encode(frame, { keyFrame: true });
    frame.close();
    await encoder.flush();
    encoder.close();

    return outputAnyFrames && !hadErrors;
  } catch (err) {
    console.log(
      "VideoEncoder error during compatibility detection:",
      config,
      err,
    );
    return false;
  }
}

/**
 * Start a periodic capture of camera frames and encode them as H264 video if supported by the
 * browser, otherwise fall back to JPEG encoding.
 */
async function startVideoCaptureAsync(
  params: VideoCaptureParams,
  signal: AbortSignal,
) {
  const { video, onJpegFrame, onH264Frame, onError, frameDurationSec } = params;
  const canvas = document.createElement("canvas");
  canvas.width = video.videoWidth;
  canvas.height = video.videoHeight;
  const ctx = canvas.getContext("2d");

  let encoder: VideoEncoder | undefined;
  const framePool: ArrayBuffer[] = [];
  setupEncoder: try {
    if (typeof VideoEncoder !== "function") {
      console.log(
        "VideoEncoder is not supported, falling back to JPEG encoding",
      );
      break setupEncoder;
    }

    const config: VideoEncoderConfig = {
      codec: "avc1.42001f", // Baseline profile (42 00) with level 3.1 (1f)
      width: video.videoWidth,
      height: video.videoHeight,
      displayWidth: video.videoWidth,
      displayHeight: video.videoHeight,
      latencyMode: "realtime",
      avc: { format: "annexb" },
      // Note that Safari 17 does not support latencyMode: "realtime", and in newer versions of the
      // Safari Technical Preview, realtime mode only works if framerate and bitrate are set.
      framerate: 1 / frameDurationSec,
      bitrate: 1000000,
    };
    if (!(await isEncoderConfigActuallySupported(config))) {
      // Safari 17 does not output any frames when latencyMode is "realtime"
      // (https://bugs.webkit.org/show_bug.cgi?id=264894). Try again with "quality".
      //
      // See also: https://bugs.webkit.org/show_bug.cgi?id=264893
      console.log(
        "latencyMode realtime encoding not supported, falling back to quality",
      );
      config.latencyMode = "quality";
      if (!(await isEncoderConfigActuallySupported(config))) {
        console.log(
          "Config is not supported, falling back to JPEG encoding",
          config,
        );
        break setupEncoder;
      }
    }
    encoder = new VideoEncoder({
      output: (chunk) => {
        if (signal.aborted) {
          return;
        }
        let buffer = framePool.pop();
        if (!buffer || buffer.byteLength < chunk.byteLength) {
          buffer = new ArrayBuffer(chunk.byteLength);
        }
        chunk.copyTo(buffer);
        onH264Frame({
          data: new Uint8Array(buffer, 0, chunk.byteLength),
          release() {
            framePool.push(this.data.buffer);
          },
        });
      },
      error: (err) => {
        if (signal.aborted) {
          return;
        }
        onError(err);
      },
    });

    encoder.configure(config); // may throw
  } catch (err) {
    onError(err as Error);
    encoder = undefined;
  }

  // add a keyframe every 2 seconds for h264 encoding
  const keyframeInterval = 2000;
  let lastKeyframeTime: number | undefined;

  let processingFrame = false;
  const start = performance.now();
  const interval = setInterval(() => {
    if (processingFrame) {
      // last frame is not yet complete, skip frame
      return;
    }
    processingFrame = true;
    if (encoder) {
      encoder.addEventListener(
        "dequeue",
        () => {
          processingFrame = false;
        },
        { once: true },
      );
      const now = performance.now();
      const frame = new VideoFrame(video, {
        timestamp: (now - start) * 1e3,
        duration: frameDurationSec * 1e6,
      });
      const encodeOptions: VideoEncoderEncodeOptions = { keyFrame: false };
      if (
        lastKeyframeTime == undefined ||
        now - lastKeyframeTime >= keyframeInterval
      ) {
        encodeOptions.keyFrame = true;
        lastKeyframeTime = now;
      }
      encoder.encode(frame, encodeOptions);
      frame.close();
    } else {
      ctx?.drawImage(video, 0, 0);
      canvas.toBlob(
        (blob) => {
          processingFrame = false;
          if (blob && !signal.aborted) {
            onJpegFrame(blob);
          }
        },
        "image/jpeg",
        0.8,
      );
    }
  }, frameDurationSec * 1000);

  const cleanup = () => {
    clearInterval(interval);
    encoder?.close();
  };
  if (signal.aborted) {
    cleanup();
  } else {
    signal.addEventListener("abort", cleanup);
  }
}
