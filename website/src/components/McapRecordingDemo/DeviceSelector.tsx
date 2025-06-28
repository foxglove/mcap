import React, { useEffect, useState } from "react";
import cx from "classnames";

import styles from "./McapRecordingDemo.module.css";
import { useStore } from "./state";

interface MediaDeviceInfo {
  deviceId: string;
  kind: MediaDeviceKind;
  label: string;
  groupId: string;
}

export function DeviceSelector(): JSX.Element {
  const state = useStore();
  const { actions, recordAudio, enabledVideoFormats, selectedCameraDeviceId, selectedAudioDeviceId } = state;

  const [devices, setDevices] = useState<MediaDeviceInfo[]>([]);

  const enableCamera = enabledVideoFormats.size > 0;
  const enableMicrophone = recordAudio;

  // Get available devices
  useEffect(() => {
    const getDevices = async () => {
      try {
        if (navigator.mediaDevices && navigator.mediaDevices.enumerateDevices) {
          const deviceList = await navigator.mediaDevices.enumerateDevices();
          setDevices(deviceList);

          // Set default selections if not already set
          const cameras = deviceList.filter(device => device.kind === "videoinput");
          const audioDevices = deviceList.filter(device => device.kind === "audioinput");

          if (cameras.length > 0 && !selectedCameraDeviceId) {
            actions.setSelectedCameraDeviceId(cameras[0]!.deviceId);
          }
          if (audioDevices.length > 0 && !selectedAudioDeviceId) {
            actions.setSelectedAudioDeviceId(audioDevices[0]!.deviceId);
          }
        }
      } catch (error) {
        console.error("Error getting devices:", error);
      }
    };

    getDevices();

    // Listen for device changes
    const handleDeviceChange = () => {
      getDevices();
    };

    if (navigator.mediaDevices) {
      navigator.mediaDevices.addEventListener("devicechange", handleDeviceChange);
      return () => {
        navigator.mediaDevices.removeEventListener("devicechange", handleDeviceChange);
      };
    }
  }, [selectedCameraDeviceId, selectedAudioDeviceId, actions]);

  const cameras = devices.filter(device => device.kind === "videoinput");
  const audioDevices = devices.filter(device => device.kind === "audioinput");

  const handleCameraChange = (deviceId: string) => {
    actions.setSelectedCameraDeviceId(deviceId);
    // Update the video stream with new device
    if (enableCamera) {
      actions.setVideoError(undefined);
      // The video recording hook will handle restarting with new device
    }
  };

  const handleAudioChange = (deviceId: string) => {
    actions.setSelectedAudioDeviceId(deviceId);
    // Update the audio stream with new device
    if (enableMicrophone) {
      actions.setAudioError(undefined);
      // The audio recording hook will handle restarting with new device
    }
  };

  if (!enableCamera && !enableMicrophone) {
    return null;
  }

  return (
    <div className={styles.deviceSelector}>
      {enableCamera && cameras.length > 0 && (
        <div className={styles.deviceGroup}>
          <label className={styles.deviceLabel}>Camera:</label>
          <select
            className={styles.deviceSelect}
            value={selectedCameraDeviceId}
            onChange={(e) => handleCameraChange(e.target.value)}
          >
            {cameras.map((camera) => (
              <option key={camera.deviceId} value={camera.deviceId}>
                {camera.label || `Camera ${camera.deviceId.slice(0, 8)}...`}
              </option>
            ))}
          </select>
        </div>
      )}

      {enableMicrophone && audioDevices.length > 0 && (
        <div className={styles.deviceGroup}>
          <label className={styles.deviceLabel}>Microphone:</label>
          <select
            className={styles.deviceSelect}
            value={selectedAudioDeviceId}
            onChange={(e) => handleAudioChange(e.target.value)}
          >
            {audioDevices.map((audio) => (
              <option key={audio.deviceId} value={audio.deviceId}>
                {audio.label || `Microphone ${audio.deviceId.slice(0, 8)}...`}
              </option>
            ))}
          </select>
        </div>
      )}
    </div>
  );
}
