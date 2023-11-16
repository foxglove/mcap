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
        params.onError(
          new Error(
            `${(
              err as Error
            ).toString()}. Ensure camera permissions are enabled.`,
          ),
        );
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
  compression: "h264" | "jpeg";
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
 * Determine whether VideoEncoder can (probably) be used to encode video with H.264.
 */
export async function supportsH264Encoding(): Promise<{
  supported: boolean;
  /** True if too many keyframes may be produced (e.g. https://bugs.webkit.org/show_bug.cgi?id=264893) */
  mayUseLotsOfKeyframes: boolean;
}> {
  const result = await selectSupportedVideoEncoderConfig({
    // Notes about fake width/height:
    // - Some platforms require them to be even numbers
    // - Too small or too large return false from isConfigSupported in Chrome
    width: 640,
    height: 480,
    frameDurationSec: 1 / 30,
  });
  return {
    supported: result != undefined,
    mayUseLotsOfKeyframes: result?.mayUseLotsOfKeyframes ?? false,
  };
}

/**
 * Find a suitable configuration for VideoEncoder that can be used to encode H.264. Returns
 * undefined if not supported.
 */
async function selectSupportedVideoEncoderConfig({
  width,
  height,
  frameDurationSec,
}: {
  width: number;
  height: number;
  frameDurationSec: number;
}) {
  const config: VideoEncoderConfig = {
    codec: "avc1.42001f", // Baseline profile (42 00) with level 3.1 (1f)
    width,
    height,
    latencyMode: "realtime",
    avc: { format: "annexb" },
    // Note that Safari 17 does not support latencyMode: "realtime", and in newer versions of the
    // Safari Technical Preview, realtime mode only works if framerate and bitrate are set.
    framerate: 1 / frameDurationSec,
    bitrate: 1000000, // chosen fairly arbitrarily but seems to work in Chrome and Safari
  };
  try {
    if (typeof VideoEncoder !== "function") {
      console.log(
        "VideoEncoder is not supported, falling back to JPEG encoding",
      );
      return undefined;
    }

    let status = await isEncoderConfigActuallySupported(config);
    if (status.supported) {
      return {
        config,
        mayUseLotsOfKeyframes: status.mayUseLotsOfKeyframes,
      };
    }

    // Safari 17 does not output any frames when latencyMode is "realtime"
    // (https://bugs.webkit.org/show_bug.cgi?id=264894). Try again with "quality".
    //
    // See also: https://bugs.webkit.org/show_bug.cgi?id=264893
    console.log(
      "latencyMode realtime encoding not supported, attempting fallback to quality",
    );
    config.latencyMode = "quality";
    status = await isEncoderConfigActuallySupported(config);
    if (status.supported) {
      return { config, mayUseLotsOfKeyframes: status.mayUseLotsOfKeyframes };
    }
  } catch (err) {
    console.log(
      "VideoEncoder error during compatibility detection:",
      config,
      err,
    );
  }
  return undefined;
}

/**
 * Sometimes `VideoEncoder.isConfigSupported` returns true but the encoder does not actually output
 * frames (looking at you, Safari). This function tries actually encoding a frame and making sure
 * that the encoder can output a chunk.
 */
async function isEncoderConfigActuallySupported(config: VideoEncoderConfig) {
  try {
    if ((await VideoEncoder.isConfigSupported(config)).supported !== true) {
      return { supported: false };
    }
    let keyFrameCount = 0;
    let totalFrameCount = 0;
    let hadErrors = false as boolean;
    const encoder = new VideoEncoder({
      output(chunk) {
        if (chunk.type === "key") {
          keyFrameCount++;
        }
        totalFrameCount++;
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
    const bitmap = await createImageBitmap(
      new ImageData(config.width, config.height),
    );
    const duration = (1 / (config.framerate ?? 30)) * 1e6;
    // Encode two frames to check if we get any delta frames or only keyframes
    for (let i = 0; i < 2; i++) {
      const frame = new VideoFrame(bitmap, {
        timestamp: i * duration,
        duration,
      });
      encoder.encode(frame, { keyFrame: i === 0 });
      frame.close();
    }
    bitmap.close();
    await encoder.flush();
    encoder.close();

    return {
      supported: totalFrameCount === 2 && !hadErrors,
      mayUseLotsOfKeyframes: keyFrameCount > 1,
    };
  } catch (err) {
    console.log(
      "VideoEncoder error during compatibility detection:",
      config,
      err,
    );
    return { supported: false };
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
  const {
    video,
    compression,
    onJpegFrame,
    onH264Frame,
    onError,
    frameDurationSec,
  } = params;
  const canvas = document.createElement("canvas");
  canvas.width = video.videoWidth;
  canvas.height = video.videoHeight;
  const ctx = canvas.getContext("2d");

  let encoder: VideoEncoder | undefined;
  const framePool: ArrayBuffer[] = [];
  if (compression === "h264") {
    try {
      const result = await selectSupportedVideoEncoderConfig({
        width: video.videoWidth,
        height: video.videoHeight,
        frameDurationSec,
      });
      if (!result) {
        onError(
          new Error(
            "Unable to find a supported configuration for H.264 encoding",
          ),
        );
        return;
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

      encoder.configure(result.config); // may throw
    } catch (err) {
      onError(err as Error);
      return;
    }
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
