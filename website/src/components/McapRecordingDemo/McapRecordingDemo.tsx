// cspell:word millis

import Link from "@docusaurus/Link";
import cx from "classnames";
import React, { useRef } from "react";

import { AudioContainer } from "./AudioContainer.tsx";
import { DeviceSelector } from "./DeviceSelector.tsx";
import styles from "./McapRecordingDemo.module.css";
import { RecordingControls } from "./RecordingControls.tsx";
import { RecordingStats } from "./RecordingStats.tsx";
import { VideoContainer } from "./VideoContainer.tsx";
import {
  useRecordClick,
  useDownloadClick,
  useMouseEventRecording,
  useDeviceOrientationRecording,
  useVideoRecording,
  useAudioRecording,
} from "./hooks/index.ts";
import { useStore, formatBytes } from "./state.ts";

const hasMouse = window.matchMedia("(hover: hover)").matches;

export function McapRecordingDemo(): JSX.Element {
  const state = useStore();

  const videoContainerRef = useRef<HTMLDivElement>(null);
  const audioWaveformRef = useRef<HTMLCanvasElement>(null);

  const {
    actions,
    audioError,
    orientationPermissionError,
    recordAudio,
    recording,
    recordMouse,
    recordOrientation,
    showDownloadInfo,
    videoError,
    enabledVideoFormats,
  } = state;

  const canStartRecording =
    recordMouse ||
    (!hasMouse && recordOrientation) ||
    (enabledVideoFormats.size > 0 && !videoError) ||
    (recordAudio && !audioError);

  useMouseEventRecording();

  useDeviceOrientationRecording();

  useVideoRecording(videoContainerRef);

  useAudioRecording(audioWaveformRef);

  const onRecordClick = useRecordClick();

  const onDownloadClick = useDownloadClick();

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
        <DeviceSelector />
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
            <VideoContainer />
          </div>
          <div className={styles.mediaContainer}>
            <AudioContainer audioWaveformRef={audioWaveformRef} />
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

          <RecordingStats />
        </div>
      </div>
    </section>
  );
}
