import cx from "classnames";
import React from "react";

import styles from "./McapRecordingDemo.module.css";
import { useStore } from "./state.ts";

interface AudioContainerProps {
  audioWaveformRef: React.RefObject<HTMLCanvasElement>;
}

export function AudioContainer({
  audioWaveformRef,
}: AudioContainerProps): JSX.Element {
  const state = useStore();
  const { audioError, recordAudio, actions } = state;
  const enableMicrophone = recordAudio;

  const handleClick = () => {
    if (!enableMicrophone) {
      actions.setRecordAudio({ shouldRecord: true });
    }
  };

  if (audioError) {
    return (
      <div className={cx(styles.error, styles.mediaErrorContainer)}>
        {audioError.toString()}
      </div>
    );
  }

  if (!enableMicrophone) {
    return (
      <span
        className={styles.mediaPlaceholderText}
        onClick={handleClick}
        role="button"
        tabIndex={0}
        onKeyDown={(e) => {
          if (e.key === "Enter" || e.key === " ") {
            handleClick();
          }
        }}
      >
        Enable &ldquo;Microphone&rdquo; to record audio
      </span>
    );
  }

  return <canvas ref={audioWaveformRef} className={styles.audioWaveform} />;
}
