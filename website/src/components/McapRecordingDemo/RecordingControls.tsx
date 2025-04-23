import React from "react";
import { useAsync } from "react-async";

import styles from "./McapRecordingDemo.module.css";
import { supportsPCMEncoding } from "./audioCapture";
import { useStore } from "./state";
import {
  supportsAV1Encoding,
  supportsH264Encoding,
  supportsH265Encoding,
  supportsVP9Encoding,
} from "./videoCapture";

const hasMouse = window.matchMedia("(hover: hover)").matches;

export function RecordingControls(): JSX.Element {
  const state = useStore();
  const { actions, videoFormat, recordAudio, recordMouse, recordOrientation } =
    state;

  const { data: h264Support } = useAsync(supportsH264Encoding);
  const { data: h265Support } = useAsync(supportsH265Encoding);
  const { data: vp9Support } = useAsync(supportsVP9Encoding);
  const { data: av1Support } = useAsync(supportsAV1Encoding);
  const { data: audioSupport } = useAsync(supportsPCMEncoding);

  return (
    <div className={styles.sensors}>
      <div className={styles.sensorsGrid}>
        <div className={styles.sensorCategory}>Camera</div>
        <div>
          <div className={styles.videoFormatGroup}>
            <select
              value={videoFormat}
              onChange={(e) => {
                actions.setVideoFormat({
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
                  actions.setRecordAudio({
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
                actions.setRecordMouse({ shouldRecord: event.target.checked });
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
                  actions.setRecordOrientation({
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
  );
}
