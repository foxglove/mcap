// cspell:word millis

import Link from "@docusaurus/Link";
import { fromMillis } from "@foxglove/rostime";
import { PoseInFrame } from "@foxglove/schemas";
import cx from "classnames";
import React, { useCallback, useEffect, useRef, useState } from "react";
import { create } from "zustand";

import styles from "./McapRecordingDemo.module.css";
import {
  MouseEventMessage,
  ProtobufObject,
  Recorder,
  toProtobufTime,
} from "./Recorder";
import { startVideoCapture } from "./videoCapture";

type State = {
  bytesWritten: bigint;
  messageCount: bigint;
  chunkCount: number;

  latestMouse: MouseEventMessage | undefined;
  latestOrientation: DeviceOrientationEvent | undefined;

  addMouseEventMessage: (msg: MouseEventMessage) => void;
  addPoseMessage: (msg: DeviceOrientationEvent) => void;
  addCameraImage: (blob: Blob) => void;
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
    addCameraImage(blob: Blob) {
      void recorder.addCameraImage(blob);
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
  return `${bytes.toFixed(2)} ${units[unit]!}`;
}

const RADIANS_PER_DEGREE = Math.PI / 180;

// Adapted from https://github.com/mrdoob/three.js/blob/master/src/math/Quaternion.js
function deviceOrientationToPose(
  event: DeviceOrientationEvent
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

  const videoRef = useRef<HTMLVideoElement>(null);
  const [recordVideo, setRecordVideo] = useState(false);
  const [recordMouse, setRecordMouse] = useState(true);
  const [recordOrientation, setRecordOrientation] = useState(true);
  const [videoStarted, setVideoStarted] = useState(false);
  const [videoPermissionError, setVideoPermissionError] = useState(false);
  const [downloadClicked, setDownloadClicked] = useState(false);

  const { addCameraImage, addMouseEventMessage, addPoseMessage } = state;

  useEffect(() => {
    if (!recording || !recordMouse) {
      return;
    }
    const handleMouseEvent = (event: MouseEvent) => {
      addMouseEventMessage({ clientX: event.clientX, clientY: event.clientY });
    };
    window.addEventListener("pointermove", handleMouseEvent);
    return () => {
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
        handleDeviceOrientationEvent
      );
    };
  }, [addPoseMessage, recording, recordOrientation]);

  useEffect(() => {
    const video = videoRef.current;
    if (!recording || !recordVideo || !video) {
      return;
    }

    setVideoStarted(false);
    setVideoPermissionError(false);

    const stopCapture = startVideoCapture({
      video,
      frameDurationSec: 1 / 30,
      onStart: () => setVideoStarted(true),
      onError: (err) => {
        console.error(err);
        setVideoPermissionError(true);
      },
      onFrame: (blob) => addCameraImage(blob),
    });
    return () => {
      stopCapture();
    };
  }, [addCameraImage, recordVideo, recording]);

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
    [recordOrientation, recording]
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
          date.getTime() - date.getTimezoneOffset() * 60_000
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
        setDownloadClicked(true);
      })();
    },
    [state]
  );

  return (
    <div className={styles.container}>
      <div className={styles.badge}>Try it out!</div>
      <div>Record an MCAP file right now in your browser:</div>
      <fieldset className={styles.sensors}>
        <legend>Sensors</legend>

        <label>
          <input
            type="checkbox"
            checked={recordVideo}
            onChange={(event) => setRecordVideo(event.target.checked)}
          />
          Camera
        </label>
        <label>
          <input
            type="checkbox"
            checked={recordMouse}
            onChange={(event) => setRecordMouse(event.target.checked)}
          />
          Mouse position
        </label>
        {!hasMouse && (
          <label>
            <input
              type="checkbox"
              checked={recordOrientation}
              onChange={(event) => setRecordOrientation(event.target.checked)}
            />
            Orientation
          </label>
        )}
      </fieldset>
      {orientationPermissionError && (
        <div style={{ color: "red" }}>
          Allow permission to use device orientation
        </div>
      )}
      {videoPermissionError && (
        <div style={{ color: "red" }}>
          Allow permission to record camera images
        </div>
      )}
      <div className={styles.recordingDetails}>
        {recording && recordVideo && !videoPermissionError && (
          <div style={{ width: 150, height: 100, position: "relative" }}>
            <video
              ref={videoRef}
              style={{ width: "100%", height: "100%" }}
              muted
              playsInline
            />
            {!videoStarted && (
              <div
                style={{
                  position: "absolute",
                  left: "50%",
                  top: "50%",
                  transform: `translate(-50%,-50%)`,
                }}
              >
                <progress />
              </div>
            )}
          </div>
        )}
        {recording && (
          <div className={styles.log}>
            {recordMouse && state.latestMouse && (
              <div>
                mouse x={state.latestMouse.clientX.toFixed()} y=
                {state.latestMouse.clientY.toFixed()}
              </div>
            )}
            {recordOrientation && state.latestOrientation && (
              <div>
                pose ɑ={(state.latestOrientation.alpha ?? 0).toFixed()}° β=
                {(state.latestOrientation.beta ?? 0).toFixed()}° γ=
                {(state.latestOrientation.gamma ?? 0).toFixed()}°
              </div>
            )}
            <div>messages: {state.messageCount.toString()}</div>
            <div>chunks: {state.chunkCount}</div>
          </div>
        )}
      </div>
      <Link
        href="#"
        className={cx("button", "button--danger", {
          ["button--outline"]: !recording,
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
      <div className={styles.downloadContainer}>
        {state.messageCount > 0 && (
          <Link
            href="#"
            className="button button--success"
            onClick={onDownloadClick}
          >
            Download MCAP file ({formatBytes(Number(state.bytesWritten))})
          </Link>
        )}
        {downloadClicked && (
          <div className={styles.downloadInfo}>
            ✨ Try inspecting the file with the{" "}
            <Link to="/guides/cli">MCAP CLI</Link>, or open it in{" "}
            <Link to="https://studio.foxglove.dev/">Foxglove Studio</Link>!
          </div>
        )}
      </div>
    </div>
  );
}
