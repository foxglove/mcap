// cspell:word millis

import React from "react";
import { fromMillis } from "@foxglove/rostime";
import { PoseInFrame } from "@foxglove/schemas";
import { useCallback, useEffect, useRef, useState } from "react";
import { create } from "zustand";
import {
  MouseEventMessage,
  ProtobufObject,
  Recorder,
  toProtobufTime,
} from "./Recorder";
import Link from "@docusaurus/Link";

type State = {
  bytesWritten: bigint;
  messageCount: number;
  chunkCount: number;

  latestMessages: Array<MouseEventMessage | DeviceOrientationEvent>;

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
    latestMessages: [],
    addMouseEventMessage(msg: MouseEventMessage) {
      void recorder.addMouseEvent(msg);
      set((state) => ({
        latestMessages: [...state.latestMessages, msg].slice(-3),
      }));
    },
    addPoseMessage(msg: DeviceOrientationEvent) {
      void recorder.addPose(deviceOrientationToPose(msg));
      set((state) => ({
        latestMessages: [...state.latestMessages, msg].slice(-3),
      }));
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

  // Automatically start recording if we believe the device has a mouse (which means it is likely
  // not to support orientation events)
  const [recording, setRecording] = useState(hasMouse);
  const [orientationPermissionError, setOrientationPermissionError] =
    useState(false);

  const videoRef = useRef<HTMLVideoElement>(null);
  const [recordingVideo, setRecordingVideo] = useState(false);
  const [videoStarted, setVideoStarted] = useState(false);
  const [videoPermissionError, setVideoPermissionError] = useState(false);
  const [downloadClicked, setDownloadClicked] = useState(false);

  const { addCameraImage, addMouseEventMessage, addPoseMessage } = state;

  useEffect(() => {
    if (!recording) {
      return;
    }

    const handleMouseEvent = (event: MouseEvent) => {
      addMouseEventMessage({ clientX: event.clientX, clientY: event.clientY });
    };
    const handleDeviceOrientationEvent = (event: DeviceOrientationEvent) => {
      addPoseMessage(event);
    };
    window.addEventListener("pointermove", handleMouseEvent);
    window.addEventListener("deviceorientation", handleDeviceOrientationEvent);
    return () => {
      window.removeEventListener("pointermove", handleMouseEvent);
      window.removeEventListener(
        "deviceorientation",
        handleDeviceOrientationEvent
      );
    };
  }, [addMouseEventMessage, addPoseMessage, recording]);

  useEffect(() => {
    const video = videoRef.current;
    if (!recordingVideo || !video) {
      return;
    }
    const controller = new AbortController();
    void (async (signal: AbortSignal) => {
      let stream: MediaStream;
      try {
        stream = await navigator.mediaDevices.getUserMedia({ video: true });
      } catch (error) {
        setVideoPermissionError(true);
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
        setVideoStarted(true);
      }

      const canvas = document.createElement("canvas");
      canvas.width = video.videoWidth;
      canvas.height = video.videoHeight;
      const ctx = canvas.getContext("2d");

      let framePromise: Promise<void> | undefined;
      const frameDurationSec = 1 / 30;
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
                addCameraImage(blob);
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
    })(controller.signal);

    return () => {
      controller.abort();
    };
  }, [addCameraImage, recordingVideo]);

  const onStartRecording = useCallback((event: React.MouseEvent) => {
    event.preventDefault();
    void (async () => {
      if (
        typeof DeviceOrientationEvent !== "undefined" &&
        "requestPermission" in DeviceOrientationEvent &&
        typeof DeviceOrientationEvent.requestPermission === "function"
      ) {
        const result: unknown =
          await DeviceOrientationEvent.requestPermission();
        if (result !== "granted") {
          setOrientationPermissionError(true);
        }
      }
      // Even if a permission error was encountered, we can record pointer events
      setRecording(true);
    })();
  }, []);

  const onDownloadClick = useCallback(
    (event: React.MouseEvent) => {
      event.preventDefault();
      void (async () => {
        const blob = await state.closeAndRestart();
        const url = URL.createObjectURL(blob);
        const link = document.createElement("a");
        link.href = url;
        link.download = "demo.mcap";
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
    <div
      style={{
        display: "flex",
        flexDirection: "column",
        alignItems: "center",
        justifyContent: "center",
      }}
    >
      <label>
        <input
          type="checkbox"
          checked={recordingVideo}
          disabled={!recording}
          onChange={(event) => {
            setVideoStarted(false);
            setRecordingVideo(event.target.checked);
          }}
        />
        Enable camera recording
      </label>
      <div
        style={{
          display: "flex",
          gap: 16,
          flexWrap: "wrap",
          justifyContent: "center",
        }}
      >
        {recordingVideo && !videoPermissionError && (
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
        <div
          style={{
            textAlign: "left",
            whiteSpace: "pre-line",
            font: "var(--ifm-code-font-size) / var(--ifm-pre-line-height) var(--ifm-font-family-monospace)",
          }}
        >
          {state.latestMessages
            .map((msg) => {
              if ("clientX" in msg) {
                return `mouse x=${msg.clientX.toFixed()} y=${msg.clientY.toFixed()}`;
              } else {
                const alpha = (msg.alpha ?? 0).toFixed();
                const beta = (msg.beta ?? 0).toFixed();
                const gamma = (msg.gamma ?? 0).toFixed();
                return `pose ɑ=${alpha}° β=${beta}° γ=${gamma}°`;
              }
            })
            .join("\n")}
        </div>
        <div
          style={{
            textAlign: "left",
            whiteSpace: "pre-line",
            font: "var(--ifm-code-font-size) / var(--ifm-pre-line-height) var(--ifm-font-family-monospace)",
          }}
        >
          {`\
Messages: ${state.messageCount}
Chunks: ${state.chunkCount}
`}
        </div>
        <div
          style={{
            display: "flex",
            flexDirection: "column",
            alignItems: "center",
            justifyContent: "center",
          }}
        >
          {recording ? (
            <Link
              href="#"
              style={{
                backgroundColor: "#81df8c",
                margin: "0 0.5rem",
                padding: "0.5rem 2rem",
                fontSize: "1.125rem",
                fontWeight: "bold",
                borderRadius: "var(--ifm-button-border-radius)",
              }}
              onClick={onDownloadClick}
            >
              Download MCAP file ({formatBytes(Number(state.bytesWritten))})
            </Link>
          ) : (
            <Link
              href="#"
              style={{
                backgroundColor: "#ff9797",
                margin: "0 0.5rem",
                padding: "0.5rem 2rem",
                fontSize: "1.125rem",
                fontWeight: "bold",
                borderRadius: "var(--ifm-button-border-radius)",
              }}
              onClick={onStartRecording}
            >
              <div
                style={{
                  display: "inline-block",
                  width: "1em",
                  height: "1em",
                  borderRadius: "50%",
                  backgroundColor: "currentcolor",
                  verticalAlign: "middle",
                }}
              ></div>{" "}
              Start recording MCAP file
            </Link>
          )}
          {downloadClicked && (
            <div style={{ fontSize: "0.75rem" }}>
              ✨ Try opening the file in{" "}
              <Link to="https://studio.foxglove.dev/">Foxglove Studio</Link>!
            </div>
          )}
        </div>
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
      </div>
    </div>
  );
}
