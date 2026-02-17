import cx from "classnames";
import React from "react";
import { useAsync } from "react-async";

import styles from "./McapRecordingDemo.module.css";
import { useStore } from "./state.ts";
import {
  supportsAV1Encoding,
  supportsH264Encoding,
  supportsH265Encoding,
} from "./videoCapture.ts";

export function VideoContainer(): JSX.Element {
  const state = useStore();
  const { videoError, enabledVideoFormats, videoStarted, actions } = state;
  const enableCamera = enabledVideoFormats.size > 0;

  const { data: h264Support } = useAsync(supportsH264Encoding);
  const { data: h265Support } = useAsync(supportsH265Encoding);
  const { data: av1Support } = useAsync(supportsAV1Encoding);

  if (videoError) {
    return (
      <div className={cx(styles.error, styles.mediaErrorContainer)}>
        {videoError.toString()}
      </div>
    );
  }

  if (!enableCamera) {
    return (
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
    );
  }

  return (
    <>
      {!videoStarted && <progress className={styles.mediaLoadingIndicator} />}
      {enabledVideoFormats.has("h264") &&
        h264Support?.mayUseLotsOfKeyframes === true && (
          <div className={styles.h264Warning}>
            Note: This browser may have a bug that causes H.264 encoding to be
            less efficient.
          </div>
        )}
    </>
  );
}
