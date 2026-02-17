import { useEffect } from "react";

import { useStore } from "../state.ts";

/**
 * Hook to record device orientation events during recording
 */
export function useDeviceOrientationRecording(): void {
  const { actions, recording, recordOrientation } = useStore();

  useEffect(() => {
    if (!recording || !recordOrientation) {
      return;
    }
    const handleDeviceOrientationEvent = (
      event: DeviceOrientationEvent,
    ): void => {
      actions.addPoseMessage(event);
    };
    window.addEventListener("deviceorientation", handleDeviceOrientationEvent);
    return () => {
      window.removeEventListener(
        "deviceorientation",
        handleDeviceOrientationEvent,
      );
    };
  }, [actions, recording, recordOrientation]);
}
