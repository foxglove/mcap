// cspell:word millis

import Link from "@docusaurus/Link";
import cx from "classnames";
import React, { useCallback, useEffect, useRef } from "react";
import { useAsync } from "react-async";

import styles from "./McapRecordingDemo.module.css";
import { RecordingControls } from "./RecordingControls";
import { startAudioCapture, startAudioStream } from "./audioCapture";
import { useStore, formatBytes } from "./state";
import {
  startVideoCapture,
  startVideoStream,
  supportsAV1Encoding,
  supportsH264Encoding,
  supportsH265Encoding,
} from "./videoCapture";

const hasMouse = window.matchMedia("(hover: hover)").matches;

export function McapRecordingDemo(): JSX.Element {
  const state = useStore();

  const videoRef = useRef<HTMLVideoElement | undefined>();
  const videoContainerRef = useRef<HTMLDivElement>(null);
  const audioWaveformRef = useRef<HTMLCanvasElement>(null);

  const {
    actions,
    audioError,
    audioStream,
    orientationPermissionError,
    recordAudio,
    recording,
    recordMouse,
    recordOrientation,
    showDownloadInfo,
    videoError,
    enabledVideoFormats,
    videoStarted,
  } = state;

  const { data: h264Support } = useAsync(supportsH264Encoding);
  const { data: h265Support } = useAsync(supportsH265Encoding);
  const { data: av1Support } = useAsync(supportsAV1Encoding);

  const canStartRecording =
    recordMouse ||
    (!hasMouse && recordOrientation) ||
    (enabledVideoFormats.size > 0 && !videoError) ||
    (recordAudio && !audioError);

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
    if (!recording || !recordMouse) {
      return;
    }
    const handleMouseEvent = (event: PointerEvent) => {
      actions.addMouseEventMessage({
        clientX: event.clientX,
        clientY: event.clientY,
      });
    };
    window.addEventListener("pointerdown", handleMouseEvent);
    window.addEventListener("pointermove", handleMouseEvent);
    return () => {
      window.removeEventListener("pointerdown", handleMouseEvent);
      window.removeEventListener("pointermove", handleMouseEvent);
    };
  }, [actions, recording, recordMouse]);

  useEffect(() => {
    if (!recording || !recordOrientation) {
      return;
    }
    const handleDeviceOrientationEvent = (event: DeviceOrientationEvent) => {
      actions.addPoseMessage(event);
    };
    window.addEventListener("deviceorientation", handleDeviceOrientationEvent);
    return () => {
      window.removeEventListener(
        "deviceorientation",
        handleDeviceOrientationEvent,
      );
    };
  }, [actions, recording, recordOrientation]);

  const enableCamera = enabledVideoFormats.size > 0;
  useEffect(() => {
    const videoContainer = videoContainerRef.current;
    if (!videoContainer || !enableCamera) {
      return;
    }

    if (videoRef.current) {
      videoRef.current.remove();
    }
    const video = document.createElement("video");
    video.muted = true;
    video.playsInline = true;
    videoRef.current = video;
    videoContainer.appendChild(video);

    const cleanup = startVideoStream({
      video: videoRef.current,
      onStart: () => {
        actions.setVideoStarted({ isStarted: true });
      },
      onError: (err) => {
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
  }, [enableCamera, actions]);

  useEffect(() => {
    const video = videoRef.current;
    if (!recording || !video || !videoStarted) {
      return;
    }
    if (!enableCamera) {
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
      onJpegFrame: (blob) => {
        actions.addJpegFrame(blob);
      },
      onVideoFrame: (frame) => {
        actions.addVideoFrame(frame);
      },
      onError: (err) => {
        actions.setVideoError(err);
        console.error(err);
      },
    });
    return () => {
      stopCapture();
    };
  }, [enableCamera, recording, videoStarted, enabledVideoFormats, actions]);

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
      onAudioStream: (stream) => {
        actions.setAudioStream(stream);
      },
      onError: (err) => {
        actions.setAudioError(err);
        console.error(err);
      },
    });

    return () => {
      cleanup();
      actions.setAudioStream(undefined);
      actions.setAudioError(undefined);
    };
  }, [enableMicrophone, actions]);

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
      onAudioData: (data) => {
        actions.addAudioData(data);
      },
      onError: (error) => {
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
  }, [enableMicrophone, audioStream, recording, recordAudio, actions]);

  const onRecordClick = useCallback(
    (event: React.MouseEvent) => {
      event.preventDefault();
      if (recording) {
        actions.setRecording({ isRecording: false });
        return;
      }
      actions.setRecording({ isRecording: true });

      // Requesting orientation permission must be done as part of a user gesture
      actions.setOrientationPermissionError({ hasError: false });
      if (
        recordOrientation &&
        typeof DeviceOrientationEvent !== "undefined" &&
        "requestPermission" in DeviceOrientationEvent &&
        typeof DeviceOrientationEvent.requestPermission === "function"
      ) {
        void Promise.resolve(DeviceOrientationEvent.requestPermission())
          .then((result) => {
            if (result !== "granted") {
              actions.setOrientationPermissionError({ hasError: true });
            }
          })
          .catch(console.error);
      }
    },
    [recordOrientation, recording, actions],
  );

  const onDownloadClick = useCallback(
    (event: React.MouseEvent) => {
      event.preventDefault();
      void (async () => {
        const blob = await actions.closeAndRestart();
        const url = URL.createObjectURL(blob);
        const link = document.createElement("a");
        link.href = url;

        // Create a date+time string in the local timezone to use as the filename
        const date = new Date();
        const localTime = new Date(
          date.getTime() - date.getTimezoneOffset() * 60_000,
        )
          .toISOString()
          .replace(/\..+$/, "")
          .replace("T", "_")
          .replaceAll(":", "-");

        link.download = `demo_${localTime}.mcap`;
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
        URL.revokeObjectURL(url);
        actions.setShowDownloadInfo({ shouldShow: true });
      })();
    },
    [actions],
  );

  return (
    <section className={styles.container}>
      <div className={styles.column}>
        <header>
          <h2>Try it out now</h2>
          <p className={styles.subhead}>
            Select sensor data to record to an MCAP file. All data is recorded
            and saved locally.
          </p>
        </header>
        <RecordingControls />
        {orientationPermissionError && (
          <div className={styles.error}>
            Allow permission to use device orientation
          </div>
        )}

        <hr />

        {showDownloadInfo && (
          <div className={styles.downloadInfo}>
            <button
              aria-label="Close"
              className={cx("clean-btn", styles.downloadInfoCloseButton)}
              type="button"
              onClick={() => {
                actions.setShowDownloadInfo({ shouldShow: false });
              }}
            >
              <span aria-hidden="true">&times;</span>
            </button>
            Try inspecting the file with the{" "}
            <Link to="/guides/cli">MCAP CLI</Link>, or open it in{" "}
            <Link to="https://app.foxglove.dev/">Foxglove</Link>.
          </div>
        )}

        {videoError ? (
          <div className={cx(styles.error, styles.mediaErrorContainer)}>
            {videoError.toString()}
          </div>
        ) : enableCamera ? (
          <>
            {!videoStarted && (
              <progress className={styles.mediaLoadingIndicator} />
            )}
            {enabledVideoFormats.has("h264") &&
              h264Support?.mayUseLotsOfKeyframes === true && (
                <div className={styles.h264Warning}>
                  Note: This browser may have a bug that causes H.264 encoding
                  to be less efficient.
                </div>
              )}
          </>
        ) : (
          <span
            className={styles.mediaPlaceholderText}
            onClick={() => {
              if (av1Support?.supported === true) {
                actions.setVideoFormat({ format: "av1", enabled: true });
              } else if (h265Support?.supported === true) {
                actions.setVideoFormat({ format: "h265", enabled: true });
              } else if (h264Support?.supported === true) {
                actions.setVideoFormat({ format: "h264", enabled: true });
              } else {
                actions.setVideoFormat({ format: "jpeg", enabled: true });
              }
            }}
          >
            Choose a video format to enable video.
          </span>
        )}

        <div className={styles.statsCounters}>
          <div className={styles.statCounter}>
            <var>Messages</var> {state.messageCount.toString()}
          </div>
          <div className={styles.statCounter}>
            <var>Chunks</var> {state.chunkCount}
          </div>
        </div>

        <div className={styles.mediaRow}>
          <div className={styles.mediaContainer} ref={videoContainerRef}>
            {videoError ? (
              <div className={cx(styles.error, styles.mediaErrorContainer)}>
                {videoError.toString()}
              </div>
            ) : enableCamera ? (
              <>
                {!videoStarted && (
                  <progress className={styles.mediaLoadingIndicator} />
                )}
                {enabledVideoFormats.has("h264") &&
                  h264Support?.mayUseLotsOfKeyframes === true && (
                    <div className={styles.h264Warning}>
                      Note: This browser may have a bug that causes H.264
                      encoding to be less efficient.
                    </div>
                  )}
              </>
            ) : (
              <span
                className={styles.mediaPlaceholderText}
                onClick={() => {
                  if (av1Support?.supported === true) {
                    actions.setVideoFormat({ format: "av1", enabled: true });
                  } else if (h265Support?.supported === true) {
                    actions.setVideoFormat({ format: "h265", enabled: true });
                  } else if (h264Support?.supported === true) {
                    actions.setVideoFormat({ format: "h264", enabled: true });
                  } else {
                    actions.setVideoFormat({ format: "jpeg", enabled: true });
                  }
                }}
              >
                Choose a video format to enable video.
              </span>
            )}
          </div>
          <div className={styles.mediaContainer}>
            {audioError ? (
              <div className={cx(styles.error, styles.mediaErrorContainer)}>
                {audioError.toString()}
              </div>
            ) : enableMicrophone ? (
              <canvas ref={audioWaveformRef} className={styles.audioWaveform} />
            ) : (
              <span className={styles.mediaPlaceholderText}>
                Enable &ldquo;Microphone&rdquo; to record audio
              </span>
            )}
          </div>
        </div>

        <div className={styles.recordingControls}>
          <div className={styles.recordingControlsColumn}>
            <Link
              href="#"
              className={cx("button", "button--danger", {
                ["button--outline"]: !recording,
                disabled: !recording && !canStartRecording,
              })}
              onClick={onRecordClick}
            >
              <div
                className={cx(styles.recordingDot, {
                  [styles.recordingDotActive!]: recording,
                })}
              />
              {recording ? "Stop recording" : "Start recording"}
            </Link>
            {state.messageCount > 0 && (
              <Link
                href="#"
                className={cx(
                  "button",
                  "button--success",
                  styles.downloadButton,
                )}
                onClick={onDownloadClick}
              >
                Download recording ({formatBytes(Number(state.bytesWritten))})
              </Link>
            )}
          </div>

          <div className={styles.recordingControlsColumn}>
            {recordMouse && state.latestMouse && (
              <>
                <div className={styles.recordingStatsSection}>
                  <h4>Mouse position</h4>
                  <div>
                    <var>X</var>: {state.latestMouse.clientX.toFixed(1)}
                  </div>
                  <div>
                    <var>Y</var>: {state.latestMouse.clientY.toFixed(1)}
                  </div>
                </div>
                <hr />
              </>
            )}
            {recordOrientation && state.latestOrientation && (
              <>
                <div className={styles.recordingStatsSection}>
                  <h4>Device orientation</h4>
                  <div>
                    <var>Roll</var>:{" "}
                    {(state.latestOrientation.gamma ?? 0).toFixed()}°
                  </div>
                  <div>
                    <var>Pitch</var>:{" "}
                    {(state.latestOrientation.beta ?? 0).toFixed()}°
                  </div>
                  <div>
                    <var>Yaw</var>:{" "}
                    {(state.latestOrientation.alpha ?? 0).toFixed()}°
                  </div>
                </div>
                <hr />
              </>
            )}
          </div>
        </div>
      </div>
    </section>
  );
}
