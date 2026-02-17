// cspell:word annexb, bitstream

type VideoStreamParams = {
  /** Video element to attach to the camera */
  video: HTMLVideoElement;
  /** Called when video stream has started */
  onStart: () => void;
  /** Called when an error is encountered */
  onError: (error: Error) => void;
  /** Optional device ID for specific camera selection */
  deviceId?: string;
};

// https://www.w3.org/TR/webcodecs-hevc-codec-registration/#videoencoderconfig-extensions
declare global {
  type HevcBitstreamFormat = "hevc" | "annexb";

  interface HevcEncoderConfig {
    format?: HevcBitstreamFormat;
  }
  interface VideoEncoderConfig {
    hevc?: HevcEncoderConfig;
  }
}

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
    const constraints: MediaStreamConstraints = {
      video: params.deviceId ? { deviceId: { exact: params.deviceId } } : true,
    };

    navigator.mediaDevices
      .getUserMedia(constraints)
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

type CompressedVideoFormat = "h264" | "h265" | "vp9" | "av1";
export type CompressedVideoFrame = {
  format: CompressedVideoFormat;
  data: Uint8Array<ArrayBuffer>;
  /** Call this function to release the buffer so it can be reused for new frames */
  release: () => void;
};

type VideoCaptureParams = {
  enableH265: boolean;
  enableH264: boolean;
  enableVP9: boolean;
  enableAV1: boolean;
  enableJpeg: boolean;
  /** Video element to capture */
  video: HTMLVideoElement;
  /** Frame interval in seconds */
  frameDurationSec: number;
  /** Called when a frame has been converted to an image */
  onJpegFrame: (blob: Blob) => void;
  /** Called when a video frame has been encoded */
  onVideoFrame: (frame: CompressedVideoFrame) => void;
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

const BASE_CONFIG_H264: Omit<VideoEncoderConfig, "width" | "height"> = {
  codec: "avc1.42001f", // Baseline profile (42 00) with level 3.1 (1f)
  avc: { format: "annexb" },
};
const BASE_CONFIG_H265: Omit<VideoEncoderConfig, "width" | "height"> = {
  // https://chromium.googlesource.com/chromium/src/+/d3acf22f7d91ad262a07075848fad13b94d15226/media/base/video_codecs.cc#189
  // https://dvb.org/wp-content/uploads/2019/10/a168_DVB_MPEG-DASH_Nov_2017.pdf page 23 & 24
  // 1 = general_profile_idc (Main profile)
  // 6 = 0b110 = general_profile_compatibility_flag (Main profile)
  // L = general_tier_flag 0 (Main tier)
  // 93 = general_level_idc (level 3.1)
  // B0 = constraint flags (progressive_source, frame_only, non_packed)
  codec: "hvc1.1.6.L93.B0",
  avc: { format: "annexb" }, // https://bugs.webkit.org/show_bug.cgi?id=281945
  hevc: { format: "annexb" },
};
const BASE_CONFIG_VP9: Omit<VideoEncoderConfig, "width" | "height"> = {
  // https://github.com/webmproject/vp9-dash/blob/main/VPCodecISOMediaFileFormatBinding.md
  // Profile 0, level 3.1, 8 bits
  codec: "vp09.00.31.08",
};
const BASE_CONFIG_AV1: Omit<VideoEncoderConfig, "width" | "height"> = {
  // https://aomediacodec.github.io/av1-isobmff/
  // https://jakearchibald.com/2022/html-codecs-parameter-for-av1/
  // Main Profile, level 3.1, Main tier, 8 bits
  codec: "av01.0.05M.08",
};

/**
 * Determine whether VideoEncoder can (probably) be used to encode video with H.264.
 */
export async function supportsH264Encoding(): Promise<{
  supported: boolean;
  /** True if too many keyframes may be produced (e.g. https://bugs.webkit.org/show_bug.cgi?id=264893) */
  mayUseLotsOfKeyframes: boolean;
}> {
  const result = await selectSupportedVideoEncoderConfig({
    baseConfig: {
      ...BASE_CONFIG_H264,
      // Notes about fake width/height:
      // - Some platforms require them to be even numbers
      // - Too small or too large return false from isConfigSupported in Chrome
      width: 640,
      height: 480,
    },
    frameDurationSec: 1 / 30,
  });
  return {
    supported: result != undefined,
    mayUseLotsOfKeyframes: result?.mayUseLotsOfKeyframes ?? false,
  };
}

/**
 * Determine whether VideoEncoder can (probably) be used to encode video with H.265.
 */
export async function supportsH265Encoding(): Promise<{
  supported: boolean;
  /** True if too many keyframes may be produced (e.g. https://bugs.webkit.org/show_bug.cgi?id=264893) */
  mayUseLotsOfKeyframes: boolean;
}> {
  const result = await selectSupportedVideoEncoderConfig({
    baseConfig: {
      ...BASE_CONFIG_H265,
      // Notes about fake width/height:
      // - Some platforms require them to be even numbers
      // - Too small or too large return false from isConfigSupported in Chrome
      width: 640,
      height: 480,
    },
    frameDurationSec: 1 / 30,
  });
  return {
    supported: result != undefined,
    mayUseLotsOfKeyframes: result?.mayUseLotsOfKeyframes ?? false,
  };
}

/**
 * Determine whether VideoEncoder can (probably) be used to encode video with VP9.
 */
export async function supportsVP9Encoding(): Promise<{
  supported: boolean;
  /** True if too many keyframes may be produced (e.g. https://bugs.webkit.org/show_bug.cgi?id=264893) */
  mayUseLotsOfKeyframes: boolean;
}> {
  const result = await selectSupportedVideoEncoderConfig({
    baseConfig: {
      ...BASE_CONFIG_VP9,
      // Notes about fake width/height:
      // - Some platforms require them to be even numbers
      // - Too small or too large return false from isConfigSupported in Chrome
      width: 640,
      height: 480,
    },
    frameDurationSec: 1 / 30,
  });
  return {
    supported: result != undefined,
    mayUseLotsOfKeyframes: result?.mayUseLotsOfKeyframes ?? false,
  };
}

/**
 * Determine whether VideoEncoder can (probably) be used to encode video with AV1.
 */
export async function supportsAV1Encoding(): Promise<{
  supported: boolean;
  /** True if too many keyframes may be produced (e.g. https://bugs.webkit.org/show_bug.cgi?id=264893) */
  mayUseLotsOfKeyframes: boolean;
}> {
  const result = await selectSupportedVideoEncoderConfig({
    baseConfig: {
      ...BASE_CONFIG_AV1,
      // Notes about fake width/height:
      // - Some platforms require them to be even numbers
      // - Too small or too large return false from isConfigSupported in Chrome
      width: 640,
      height: 480,
    },
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
  baseConfig,
  frameDurationSec,
}: {
  baseConfig: VideoEncoderConfig;
  frameDurationSec: number;
}) {
  const config: VideoEncoderConfig = {
    ...baseConfig,
    latencyMode: "realtime",
    // Note that Safari 17 does not support latencyMode: "realtime", and in newer versions of the
    // Safari Technical Preview, realtime mode only works if framerate and bitrate are set.
    framerate: 1 / frameDurationSec,
    bitrate: 1000000, // chosen fairly arbitrarily but seems to work in Chrome and Safari
  };
  try {
    if (typeof VideoEncoder !== "function") {
      console.error(
        "VideoEncoder is not supported, falling back to JPEG encoding",
      );
      return undefined;
    }

    let status = await isEncoderConfigActuallySupported(config);
    if (status.supported) {
      return { config, mayUseLotsOfKeyframes: status.mayUseLotsOfKeyframes };
    }

    // Safari 17 does not output any frames when latencyMode is "realtime"
    // (https://bugs.webkit.org/show_bug.cgi?id=264894). Try again with "quality".
    //
    // See also: https://bugs.webkit.org/show_bug.cgi?id=264893
    console.warn(
      `latencyMode realtime encoding not supported for ${baseConfig.codec}, attempting fallback to quality`,
    );
    config.latencyMode = "quality";
    status = await isEncoderConfigActuallySupported(config);
    if (status.supported) {
      return { config, mayUseLotsOfKeyframes: status.mayUseLotsOfKeyframes };
    }
  } catch (err) {
    console.warn(
      "VideoEncoder error during compatibility detection:",
      config,
      err,
    );
  }
  console.warn(`No supported config found for ${baseConfig.codec}`);
  return undefined;
}

/**
 * Sometimes `VideoEncoder.isConfigSupported` returns true but the encoder does not actually output
 * frames (looking at you, Safari). This function tries actually encoding a frame and making sure
 * that the encoder can output a chunk.
 */
async function isEncoderConfigActuallySupported(config: VideoEncoderConfig) {
  try {
    const supportedConfig = await VideoEncoder.isConfigSupported(config);
    if (supportedConfig.supported !== true) {
      return { supported: false };
    }
    console.info(
      `Found supported config for ${config.codec}:`,
      supportedConfig.config,
    );
    let keyFrameCount = 0;
    let totalFrameCount = 0;
    let hadErrors = false as boolean;
    let allFramesWereAnnexB = true;
    const encoder = new VideoEncoder({
      output(chunk) {
        if (chunk.type === "key") {
          keyFrameCount++;
        }
        totalFrameCount++;
        const buf = new Uint8Array(chunk.byteLength);
        chunk.copyTo(buf);

        // Double-check that we actually got AnnexB output -- only relevant for H.264 and H.265
        // https://bugs.webkit.org/show_bug.cgi?id=281945
        if (
          config.avc?.format === "annexb" ||
          config.hevc?.format === "annexb"
        ) {
          const isAnnexB =
            buf[0] === 0 &&
            buf[1] === 0 &&
            (buf[2] === 1 || (buf[2] === 0 && buf[3] === 1));
          if (!isAnnexB) {
            allFramesWereAnnexB = false;
          }
        }
      },
      error(err) {
        hadErrors = true;
        console.error(
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
      supported: totalFrameCount === 2 && !hadErrors && allFramesWereAnnexB,
      mayUseLotsOfKeyframes: keyFrameCount > 1,
    };
  } catch (err) {
    console.error(
      "VideoEncoder error during compatibility detection:",
      config,
      err,
    );
    return { supported: false };
  }
}

async function tryToConfigureEncoder(params: {
  baseConfig: Omit<VideoEncoderConfig, "width" | "height">;
  frameDurationSec: number;
  framePool: ArrayBuffer[];
  onError: (error: Error) => void;
  onVideoFrame: (frame: CompressedVideoFrame) => void;
  outputFormat: CompressedVideoFormat;
  signal: AbortSignal;
  video: HTMLVideoElement;
}): Promise<VideoEncoder | undefined> {
  const {
    baseConfig,
    frameDurationSec,
    framePool,
    onError,
    onVideoFrame,
    outputFormat,
    signal,
    video,
  } = params;

  let encoder;
  try {
    const result = await selectSupportedVideoEncoderConfig({
      baseConfig: {
        ...baseConfig,
        width: video.videoWidth,
        height: video.videoHeight,
      },
      frameDurationSec,
    });
    if (!result) {
      onError(
        new Error(
          `Unable to find a supported configuration for ${outputFormat} encoding`,
        ),
      );
      return undefined;
    }
    encoder = new VideoEncoder({
      output: (chunk) => {
        let buffer = framePool.pop();
        if (!buffer || buffer.byteLength < chunk.byteLength) {
          buffer = new ArrayBuffer(chunk.byteLength);
        }
        chunk.copyTo(buffer);
        onVideoFrame({
          format: outputFormat,
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
    return encoder;
  } catch (err) {
    onError(err as Error);
    return undefined;
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
    enableH264,
    enableH265,
    enableVP9,
    enableAV1,
    enableJpeg,
    onJpegFrame,
    onVideoFrame,
    onError,
    frameDurationSec,
  } = params;
  if (!enableAV1 && !enableVP9 && !enableH265 && !enableH264 && !enableJpeg) {
    throw new Error(
      "At least one of AV1, VP9, H.265, H.264, or JPEG encoding must be enabled",
    );
  }
  const canvas = document.createElement("canvas");
  canvas.width = video.videoWidth;
  canvas.height = video.videoHeight;
  const ctx = canvas.getContext("2d");

  const encoders: VideoEncoder[] = [];
  const framePool: ArrayBuffer[] = [];
  if (enableH264) {
    const encoder = await tryToConfigureEncoder({
      outputFormat: "h264",
      baseConfig: BASE_CONFIG_H264,
      frameDurationSec,
      framePool,
      onError,
      onVideoFrame,
      signal,
      video,
    });
    if (encoder) {
      encoders.push(encoder);
    }
  }
  if (enableH265) {
    const encoder = await tryToConfigureEncoder({
      outputFormat: "h265",
      baseConfig: BASE_CONFIG_H265,
      frameDurationSec,
      framePool,
      onError,
      onVideoFrame,
      signal,
      video,
    });
    if (encoder) {
      encoders.push(encoder);
    }
  }
  if (enableVP9) {
    const encoder = await tryToConfigureEncoder({
      outputFormat: "vp9",
      baseConfig: BASE_CONFIG_VP9,
      frameDurationSec,
      framePool,
      onError,
      onVideoFrame,
      signal,
      video,
    });
    if (encoder) {
      encoders.push(encoder);
    }
  }
  if (enableAV1) {
    const encoder = await tryToConfigureEncoder({
      outputFormat: "av1",
      baseConfig: BASE_CONFIG_AV1,
      frameDurationSec,
      framePool,
      onError,
      onVideoFrame,
      signal,
      video,
    });
    if (encoder) {
      encoders.push(encoder);
    }
  }

  // add a keyframe every 2 seconds for video encoding
  const keyframeInterval = 2000;
  let lastKeyframeTime: number | undefined;

  let processingCount = 0;
  const start = performance.now();
  const interval = setInterval(() => {
    if (processingCount > 0) {
      // last frame is not yet complete, skip frame
      return;
    }
    if (encoders.length > 0) {
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
      for (const encoder of encoders) {
        ++processingCount;
        encoder.addEventListener(
          "dequeue",
          () => {
            --processingCount;
          },
          { once: true },
        );
        encoder.encode(frame, encodeOptions);
      }
      frame.close();
    }

    if (enableJpeg) {
      ++processingCount;
      ctx?.drawImage(video, 0, 0);
      canvas.toBlob(
        (blob) => {
          --processingCount;
          if (blob) {
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
    for (const encoder of encoders) {
      encoder.close();
    }
  };
  if (signal.aborted) {
    cleanup();
  } else {
    signal.addEventListener("abort", cleanup);
  }
}
