// cspell:word millis

import Link from "@docusaurus/Link";
import { fromMillis } from "@foxglove/rostime";
import { PoseInFrame } from "@foxglove/schemas";
import cx from "classnames";
import React, { useCallback, useEffect, useRef, useState } from "react";
import { useAsync } from "react-async";
import { create } from "zustand";

import styles from "./McapRecordingDemo.module.css";
import {
  MouseEventMessage,
  ProtobufObject,
  Recorder,
  toProtobufTime,
} from "./Recorder";
import {
  CompressedVideoFrame,
  startVideoCapture,
  startVideoStream,
  supportsH264Encoding,
  supportsH265Encoding,
} from "./videoCapture";

type State = {
  bytesWritten: bigint;
  messageCount: bigint;
  chunkCount: number;

  latestMouse: MouseEventMessage | undefined;
  latestOrientation: DeviceOrientationEvent | undefined;

  addMouseEventMessage: (msg: MouseEventMessage) => void;
  addPoseMessage: (msg: DeviceOrientationEvent) => void;
  addJpegFrame: (blob: Blob) => void;
  addVideoFrame: (frame: CompressedVideoFrame) => void;
  closeAndRestart: () => Promise<Blob>;
};

const useStore = create<State>((set) => {
  const recorder = new Recorder();
  recorder.addListener("update", () => {
    set({
      bytesWritten: recorder.bytesWritten,
      messageCount: recorder.messageCount,
      chunkCount: recorder.chunkCount,
    });
  });

  return {
    bytesWritten: recorder.bytesWritten,
    messageCount: recorder.messageCount,
    chunkCount: recorder.chunkCount,
    latestMouse: undefined,
    latestOrientation: undefined,
    addMouseEventMessage(msg: MouseEventMessage) {
      void recorder.addMouseEvent(msg);
      set({ latestMouse: msg });
    },
    addPoseMessage(msg: DeviceOrientationEvent) {
      void recorder.addPose(deviceOrientationToPose(msg));
      set({ latestOrientation: msg });
    },
    addJpegFrame(blob: Blob) {
      void recorder.addJpegFrame(blob);
    },
    addVideoFrame(frame: CompressedVideoFrame) {
      void recorder.addVideoFrame(frame);
    },
    async closeAndRestart() {
      return await recorder.closeAndRestart();
    },
  };
});

function formatBytes(totalBytes: number) {
  const units = ["B", "kiB", "MiB", "GiB", "TiB"];
  let bytes = totalBytes;
  let unit = 0;
  while (unit + 1 < units.length && bytes >= 1024) {
    bytes /= 1024;
    unit++;
  }
  return `${bytes.toFixed(unit === 0 ? 0 : 1)} ${units[unit]!}`;
}

const RADIANS_PER_DEGREE = Math.PI / 180;

// Adapted from https://github.com/mrdoob/three.js/blob/master/src/math/Quaternion.js
function deviceOrientationToPose(
  event: DeviceOrientationEvent,
): ProtobufObject<PoseInFrame> {
  const alpha = (event.alpha ?? 0) * RADIANS_PER_DEGREE; // z angle
  const beta = (event.beta ?? 0) * RADIANS_PER_DEGREE; // x angle
  const gamma = (event.gamma ?? 0) * RADIANS_PER_DEGREE; // y angle

  const c1 = Math.cos(beta / 2);
  const c2 = Math.cos(gamma / 2);
  const c3 = Math.cos(alpha / 2);

  const s1 = Math.sin(beta / 2);
  const s2 = Math.sin(gamma / 2);
  const s3 = Math.sin(alpha / 2);

  const x = s1 * c2 * c3 - c1 * s2 * s3;
  const y = c1 * s2 * c3 + s1 * c2 * s3;
  const z = c1 * c2 * s3 + s1 * s2 * c3;
  const w = c1 * c2 * c3 - s1 * s2 * s3;

  return {
    timestamp: toProtobufTime(fromMillis(event.timeStamp)),
    frame_id: "device",
    pose: { position: { x: 0, y: 0, z: 0 }, orientation: { x, y, z, w } },
  };
}

const hasMouse = window.matchMedia("(hover: hover)").matches;

export function McapRecordingDemo(): JSX.Element {
  const state = useStore();

  const [recording, setRecording] = useState(false);
  const [orientationPermissionError, setOrientationPermissionError] =
    useState(false);

  const videoRef = useRef<HTMLVideoElement | undefined>();
  const videoContainerRef = useRef<HTMLDivElement>(null);
  const [recordJpeg, setRecordJpeg] = useState(false);
  const [recordH264, setRecordH264] = useState(false);
  const [recordH265, setRecordH265] = useState(false);
  const [recordMouse, setRecordMouse] = useState(true);
  const [recordOrientation, setRecordOrientation] = useState(true);
  const [videoStarted, setVideoStarted] = useState(false);
  const [videoError, setVideoError] = useState<Error | undefined>();
  const [showDownloadInfo, setShowDownloadInfo] = useState(false);

  const { addJpegFrame, addVideoFrame, addMouseEventMessage, addPoseMessage } =
    state;

  const { data: h264Support } = useAsync(supportsH264Encoding);
  const { data: h265Support } = useAsync(supportsH265Encoding);

  const canStartRecording =
    recordMouse ||
    (!hasMouse && recordOrientation) ||
    (recordH265 && !videoError) ||
    (recordH264 && !videoError) ||
    (recordJpeg && !videoError);

  // Automatically pause recording after 30 seconds to avoid unbounded growth
  useEffect(() => {
    if (!recording) {
      return;
    }
    const timeout = setTimeout(() => {
      setRecording(false);
    }, 30000);
    return () => {
      clearTimeout(timeout);
    };
  }, [recording]);

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

  const enableCamera = recordH265 || recordH264 || recordJpeg;
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
        setVideoStarted(true);
      },
      onError: (err) => {
        setVideoError(err);
        console.error(err);
      },
    });

    return () => {
      cleanup();
      video.remove();
      setVideoStarted(false);
      setVideoError(undefined);
    };
  }, [enableCamera]);

  useEffect(() => {
    const video = videoRef.current;
    if (!recording || !video || !videoStarted) {
      return;
    }
    if (!recordH265 && !recordH264 && !recordJpeg) {
      return;
    }

    const stopCapture = startVideoCapture({
      video,
      enableH265: recordH265,
      enableH264: recordH264,
      enableJpeg: recordJpeg,
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
    recordH264,
    recordH265,
    recording,
    videoStarted,
    recordJpeg,
  ]);

  const onRecordClick = useCallback(
    (event: React.MouseEvent) => {
      event.preventDefault();
      if (recording) {
        setRecording(false);
        return;
      }
      setRecording((oldValue) => !oldValue);

      // Requesting orientation permission must be done as part of a user gesture
      setOrientationPermissionError(false);
      if (
        recordOrientation &&
        typeof DeviceOrientationEvent !== "undefined" &&
        "requestPermission" in DeviceOrientationEvent &&
        typeof DeviceOrientationEvent.requestPermission === "function"
      ) {
        void Promise.resolve(DeviceOrientationEvent.requestPermission())
          .then((result) => {
            if (result !== "granted") {
              setOrientationPermissionError(true);
            }
          })
          .catch(console.error);
      }
    },
    [recordOrientation, recording],
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
        setShowDownloadInfo(true);
      })();
    },
    [state],
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
          <label>
            <input
              type="checkbox"
              checked={recordMouse}
              onChange={(event) => {
                setRecordMouse(event.target.checked);
              }}
            />
            Mouse position
          </label>
          {h265Support?.supported === true && (
            <label>
              <input
                type="checkbox"
                checked={recordH265}
                onChange={(event) => {
                  setRecordH265(event.target.checked);
                }}
              />
              Camera (H.265)
            </label>
          )}
          {h264Support?.supported === true && (
            <label>
              <input
                type="checkbox"
                checked={recordH264}
                onChange={(event) => {
                  setRecordH264(event.target.checked);
                }}
              />
              Camera (H.264)
            </label>
          )}
          <label>
            <input
              type="checkbox"
              checked={recordJpeg}
              onChange={(event) => {
                setRecordJpeg(event.target.checked);
              }}
            />
            Camera (JPEG)
          </label>
          {!hasMouse && (
            <label>
              <input
                type="checkbox"
                checked={recordOrientation}
                onChange={(event) => {
                  setRecordOrientation(event.target.checked);
                }}
              />
              Orientation
            </label>
          )}
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
                setShowDownloadInfo(false);
              }}
            >
              <span aria-hidden="true">&times;</span>
            </button>
            Try inspecting the file with the{" "}
            <Link to="/guides/cli">MCAP CLI</Link>, or open it in{" "}
            <Link to="https://app.foxglove.dev/">Foxglove</Link>.
          </div>
        )}

        {recordH264 && h264Support?.mayUseLotsOfKeyframes === true && (
          <div className={styles.h264Warning}>
            Note: This browser may have a bug that causes H.264 encoding to be
            less efficient.
          </div>
        )}

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
            <div className={styles.recordingStatsSection}>
              <div>
                <Link href="/guides/concepts" target="_blank">
                  <var>Messages</var>
                </Link>
                : {state.messageCount.toString()}
              </div>
              <div>
                <Link href="/spec#use-of-chunk-records" target="_blank">
                  <var>Chunks</var>
                </Link>
                : {state.chunkCount}
              </div>
            </div>
          </div>

          <div className={styles.recordingControlsColumn}>
            <div className={styles.videoContainer} ref={videoContainerRef}>
              {videoError ? (
                <div className={cx(styles.error, styles.videoErrorContainer)}>
                  {videoError.toString()}
                </div>
              ) : recordH264 || recordJpeg ? (
                <>
                  {!videoStarted && (
                    <progress className={styles.videoLoadingIndicator} />
                  )}
                </>
              ) : (
                <span
                  className={styles.videoPlaceholderText}
                  onClick={() => {
                    if (h264Support?.supported === true) {
                      setRecordH264(true);
                    } else {
                      setRecordJpeg(true);
                    }
                  }}
                >
                  Enable “Camera” to record video
                </span>
              )}
            </div>
          </div>
        </div>
      </div>
    </section>
  );
}
