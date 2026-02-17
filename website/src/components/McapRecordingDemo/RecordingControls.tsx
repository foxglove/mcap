import React from "react";
import { useAsync } from "react-async";

import styles from "./McapRecordingDemo.module.css";
import { supportsPCMEncoding } from "./audioCapture.ts";
import { useStore } from "./state.ts";
import {
  supportsAV1Encoding,
  supportsH264Encoding,
  supportsH265Encoding,
  supportsVP9Encoding,
} from "./videoCapture.ts";

const hasMouse = window.matchMedia("(hover: hover)").matches;

export function RecordingControls(): JSX.Element {
  const state = useStore();
  const { actions, recordAudio, recordMouse, recordOrientation } = state;

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
            {av1Support?.supported === true && (
              <label>
                <input
                  type="checkbox"
                  checked={state.enabledVideoFormats.has("av1")}
                  onChange={(event) => {
                    actions.setVideoFormat({
                      format: "av1",
                      enabled: event.target.checked,
                    });
                  }}
                />
                AV1
              </label>
            )}
            {vp9Support?.supported === true && (
              <label>
                <input
                  type="checkbox"
                  checked={state.enabledVideoFormats.has("vp9")}
                  onChange={(event) => {
                    actions.setVideoFormat({
                      format: "vp9",
                      enabled: event.target.checked,
                    });
                  }}
                />
                VP9
              </label>
            )}
            {h265Support?.supported === true && (
              <label>
                <input
                  type="checkbox"
                  checked={state.enabledVideoFormats.has("h265")}
                  onChange={(event) => {
                    actions.setVideoFormat({
                      format: "h265",
                      enabled: event.target.checked,
                    });
                  }}
                />
                H.265
              </label>
            )}
            {h264Support?.supported === true && (
              <label>
                <input
                  type="checkbox"
                  checked={state.enabledVideoFormats.has("h264")}
                  onChange={(event) => {
                    actions.setVideoFormat({
                      format: "h264",
                      enabled: event.target.checked,
                    });
                  }}
                />
                H.264
              </label>
            )}
            <label>
              <input
                type="checkbox"
                checked={state.enabledVideoFormats.has("jpeg")}
                onChange={(event) => {
                  actions.setVideoFormat({
                    format: "jpeg",
                    enabled: event.target.checked,
                  });
                }}
              />
              JPEG
            </label>
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
