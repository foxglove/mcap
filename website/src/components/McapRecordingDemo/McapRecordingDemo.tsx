// cspell:word millis

import Link from "@docusaurus/Link";
import cx from "classnames";
import React, { useCallback, useEffect, useRef } from "react";
import { useAsync } from "react-async";

import styles from "./McapRecordingDemo.module.css";
import {
  startAudioCapture,
  startAudioStream,
  supportsPCMEncoding,
} from "./audioCapture";
import { useStore, formatBytes } from "./state";
import {
  startVideoCapture,
  startVideoStream,
  supportsAV1Encoding,
  supportsH264Encoding,
  supportsH265Encoding,
  supportsVP9Encoding,
} from "./videoCapture";

const hasMouse = window.matchMedia("(hover: hover)").matches;

export function McapRecordingDemo(): JSX.Element {
  const state = useStore();

  const videoRef = useRef<HTMLVideoElement | undefined>();
  const videoContainerRef = useRef<HTMLDivElement>(null);
  const audioWaveformRef = useRef<HTMLCanvasElement>(null);

  const {
    addJpegFrame,
    addVideoFrame,
    addMouseEventMessage,
    addPoseMessage,
    addAudioData,
    recording,
    orientationPermissionError,
    showDownloadInfo,
    videoFormat,
    recordAudio,
    recordMouse,
    recordOrientation,
    videoStarted,
    videoError,
    audioError,
    audioStream,
    setRecording,
    setOrientationPermissionError,
    setShowDownloadInfo,
    setVideoFormat,
    setRecordAudio,
    setRecordMouse,
    setRecordOrientation,
    setVideoStarted,
    setVideoError,
    setAudioError,
    setAudioStream,
  } = state;

  const { data: h264Support } = useAsync(supportsH264Encoding);
  const { data: h265Support } = useAsync(supportsH265Encoding);
  const { data: vp9Support } = useAsync(supportsVP9Encoding);
  const { data: av1Support } = useAsync(supportsAV1Encoding);
  const { data: audioSupport } = useAsync(supportsPCMEncoding);

  const canStartRecording =
    recordMouse ||
    (!hasMouse && recordOrientation) ||
    (videoFormat !== "none" && !videoError) ||
    (recordAudio && !audioError);

  // Automatically pause recording after 30 seconds to avoid unbounded growth
  useEffect(() => {
    if (!recording) {
      return;
    }
    const timeout = setTimeout(() => {
      setRecording({ isRecording: false });
    }, 30000);
    return () => {
      clearTimeout(timeout);
    };
  }, [recording, setRecording]);

  useEffect(() => {
    if (!recording || !recordMouse) {
      return;
    }
    const handleMouseEvent = (event: PointerEvent) => {
      addMouseEventMessage({ clientX: event.clientX, clientY: event.clientY });
    };
    window.addEventListener("pointerdown", handleMouseEvent);
    window.addEventListener("pointermove", handleMouseEvent);
    return () => {
      window.removeEventListener("pointerdown", handleMouseEvent);
      window.removeEventListener("pointermove", handleMouseEvent);
    };
  }, [addMouseEventMessage, recording, recordMouse]);

  useEffect(() => {
    if (!recording || !recordOrientation) {
      return;
    }
    const handleDeviceOrientationEvent = (event: DeviceOrientationEvent) => {
      addPoseMessage(event);
    };
    window.addEventListener("deviceorientation", handleDeviceOrientationEvent);
    return () => {
      window.removeEventListener(
        "deviceorientation",
        handleDeviceOrientationEvent,
      );
    };
  }, [addPoseMessage, recording, recordOrientation]);

  const enableCamera = videoFormat !== "none";
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
        setVideoStarted({ isStarted: true });
      },
      onError: (err) => {
        setVideoError(err);
        console.error(err);
      },
    });

    return () => {
      cleanup();
      video.remove();
      setVideoStarted({ isStarted: false });
      setVideoError(undefined);
    };
  }, [enableCamera, setVideoStarted, setVideoError]);

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
      enableAV1: videoFormat === "av1",
      enableVP9: videoFormat === "vp9",
      enableH265: videoFormat === "h265",
      enableH264: videoFormat === "h264",
      enableJpeg: videoFormat === "jpeg",
      frameDurationSec: 1 / 30,
      onJpegFrame: (blob) => {
        addJpegFrame(blob);
      },
      onVideoFrame: (frame) => {
        addVideoFrame(frame);
      },
      onError: (err) => {
        setVideoError(err);
        console.error(err);
      },
    });
    return () => {
      stopCapture();
    };
  }, [
    addJpegFrame,
    addVideoFrame,
    enableCamera,
    recording,
    videoStarted,
    videoFormat,
    setVideoError,
  ]);

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
        setAudioStream(stream);
      },
      onError: (err) => {
        setAudioError(err);
        console.error(err);
      },
    });

    return () => {
      cleanup();
      setAudioStream(undefined);
      setAudioError(undefined);
    };
  }, [enableMicrophone, setAudioStream, setAudioError]);

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
        addAudioData(data);
        // Draw waveform
        const waveformCanvas = audioWaveformRef.current;
        if (waveformCanvas) {
          const ctx = waveformCanvas.getContext("2d", { alpha: true });
          if (ctx) {
            const width = waveformCanvas.width;
            const height = waveformCanvas.height;

            // Clear with slight fade effect
            ctx.fillStyle = "rgba(255, 255, 255, 0.1)";
            ctx.fillRect(0, 0, width, height);

            // Convert audio data to Int16Array
            const audioData = new Int16Array(data.data);
            const centerY = height / 2;
            const step = Math.max(1, width / audioData.length);

            ctx.beginPath();
            ctx.moveTo(0, centerY);

            for (let i = 0; i < audioData.length; i++) {
              const x = i * step;
              // Ensure we have a valid value from the array
              const sample = audioData[i] ?? 0;
              const y = centerY + (sample / 32768.0) * (height / 2);
              ctx.lineTo(x, y);
            }

            ctx.strokeStyle = "#6f3be8";
            ctx.lineWidth = 4 * window.devicePixelRatio;
            ctx.stroke();
          }
        }
      },
      onError: (error) => {
        setAudioError(error);
      },
    });

    return () => {
      cleanup?.();
      // Clear canvas on cleanup
      const cleanupCanvas = audioWaveformRef.current;
      if (cleanupCanvas) {
        const ctx = cleanupCanvas.getContext("2d");
        if (ctx) {
          ctx.clearRect(0, 0, cleanupCanvas.width, cleanupCanvas.height);
        }
      }
    };
  }, [
    addAudioData,
    enableMicrophone,
    audioStream,
    recording,
    recordAudio,
    setAudioError,
  ]);

  const onRecordClick = useCallback(
    (event: React.MouseEvent) => {
      event.preventDefault();
      if (recording) {
        setRecording({ isRecording: false });
        return;
      }
      setRecording({ isRecording: true });

      // Requesting orientation permission must be done as part of a user gesture
      setOrientationPermissionError({ hasError: false });
      if (
        recordOrientation &&
        typeof DeviceOrientationEvent !== "undefined" &&
        "requestPermission" in DeviceOrientationEvent &&
        typeof DeviceOrientationEvent.requestPermission === "function"
      ) {
        void Promise.resolve(DeviceOrientationEvent.requestPermission())
          .then((result) => {
            if (result !== "granted") {
              setOrientationPermissionError({ hasError: true });
            }
          })
          .catch(console.error);
      }
    },
    [recordOrientation, recording, setRecording, setOrientationPermissionError],
  );

  const onDownloadClick = useCallback(
    (event: React.MouseEvent) => {
      event.preventDefault();
      void (async () => {
        const blob = await state.closeAndRestart();
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
        setShowDownloadInfo({ shouldShow: true });
      })();
    },
    [state, setShowDownloadInfo],
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
        <div className={styles.sensors}>
          <div className={styles.sensorsGrid}>
            <div className={styles.sensorCategory}>Video</div>
            <div>
              <div className={styles.videoFormatGroup}>
                <select
                  value={videoFormat}
                  onChange={(e) => {
                    setVideoFormat({
                      format: e.target.value as typeof videoFormat,
                    });
                  }}
                  className={styles.videoFormatSelect}
                >
                  <option value="none">None</option>
                  {av1Support?.supported === true && (
                    <option value="av1">AV1</option>
                  )}
                  {vp9Support?.supported === true && (
                    <option value="vp9">VP9</option>
                  )}
                  {h265Support?.supported === true && (
                    <option value="h265">H.265</option>
                  )}
                  {h264Support?.supported === true && (
                    <option value="h264">H.264</option>
                  )}
                  <option value="jpeg">JPEG</option>
                </select>
              </div>
            </div>
            <div className={styles.sensorCategory}>Audio</div>
            <div>
              {audioSupport === true && (
                <label>
                  <input
                    type="checkbox"
                    checked={recordAudio}
                    onChange={(event) => {
                      setRecordAudio({
                        shouldRecord: event.target.checked,
                      });
                    }}
                  />
                  Microphone
                </label>
              )}
            </div>
            <div className={styles.sensorCategory}>Controls</div>
            <div>
              <label>
                <input
                  type="checkbox"
                  checked={recordMouse}
                  onChange={(event) => {
                    setRecordMouse({ shouldRecord: event.target.checked });
                  }}
                />
                Mouse position
              </label>
              {!hasMouse && (
                <label>
                  <input
                    type="checkbox"
                    checked={recordOrientation}
                    onChange={(event) => {
                      setRecordOrientation({
                        shouldRecord: event.target.checked,
                      });
                    }}
                  />
                  Orientation
                </label>
              )}
            </div>
          </div>
        </div>
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
                setShowDownloadInfo({ shouldShow: false });
              }}
            >
              <span aria-hidden="true">&times;</span>
            </button>
            Try inspecting the file with the{" "}
            <Link to="/guides/cli">MCAP CLI</Link>, or open it in{" "}
            <Link to="https://app.foxglove.dev/">Foxglove</Link>.
          </div>
        )}

        {videoFormat === "h264" &&
          h264Support?.mayUseLotsOfKeyframes === true && (
            <div className={styles.h264Warning}>
              Note: This browser may have a bug that causes H.264 encoding to be
              less efficient.
            </div>
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
              </>
            ) : (
              <span
                className={styles.mediaPlaceholderText}
                onClick={() => {
                  if (av1Support?.supported === true) {
                    setVideoFormat({ format: "av1" });
                  } else if (h265Support?.supported === true) {
                    setVideoFormat({ format: "h265" });
                  } else if (h264Support?.supported === true) {
                    setVideoFormat({ format: "h264" });
                  } else {
                    setVideoFormat({ format: "jpeg" });
                  }
                }}
              >
                Enable &ldquo;Camera&rdquo; to record video
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
