import { useCallback } from "react";

import { useStore } from "../state.ts";

/**
 * Hook to handle recording start/stop functionality with device orientation permission handling
 * @returns A callback function to handle recording button clicks
 */
export function useRecordClick(): (
  event: React.MouseEvent<HTMLAnchorElement>,
) => void {
  const { actions, recording, recordOrientation } = useStore();

  return useCallback(
    (event: React.MouseEvent<HTMLAnchorElement>) => {
      event.preventDefault();
      if (recording) {
        actions.setRecording({ isRecording: false });
        return;
      }
      actions.setRecording({ isRecording: true });

      // Requesting orientation permission must be done as part of a user gesture
      actions.setOrientationPermissionError({ hasError: false });
      if (
        recordOrientation &&
        typeof DeviceOrientationEvent !== "undefined" &&
        "requestPermission" in DeviceOrientationEvent &&
        typeof DeviceOrientationEvent.requestPermission === "function"
      ) {
        void Promise.resolve(DeviceOrientationEvent.requestPermission())
          .then((result) => {
            if (result !== "granted") {
              actions.setOrientationPermissionError({ hasError: true });
            }
          })
          .catch(console.error);
      }
    },
    [recordOrientation, recording, actions],
  );
}
