import React from "react";

import styles from "./McapRecordingDemo.module.css";
import { useStore } from "./state.ts";

export function RecordingStats(): JSX.Element | null {
  const { recordMouse, latestMouse, recordOrientation, latestOrientation } =
    useStore();

  if (!recordMouse && !recordOrientation) {
    return null;
  }

  return (
    <div className={styles.recordingControlsColumn}>
      {recordMouse && latestMouse && (
        <>
          <div className={styles.recordingStatsSection}>
            <h4>Mouse position</h4>
            <div>
              <var>X</var>: {latestMouse.clientX.toFixed(1)}
            </div>
            <div>
              <var>Y</var>: {latestMouse.clientY.toFixed(1)}
            </div>
          </div>
          <hr />
        </>
      )}
      {recordOrientation && latestOrientation && (
        <>
          <div className={styles.recordingStatsSection}>
            <h4>Device orientation</h4>
            <div>
              <var>Roll</var>: {(latestOrientation.gamma ?? 0).toFixed()}°
            </div>
            <div>
              <var>Pitch</var>: {(latestOrientation.beta ?? 0).toFixed()}°
            </div>
            <div>
              <var>Yaw</var>: {(latestOrientation.alpha ?? 0).toFixed()}°
            </div>
          </div>
          <hr />
        </>
      )}
    </div>
  );
}
