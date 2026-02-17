import React, { useEffect, useState } from "react";

import styles from "./McapRecordingDemo.module.css";
import { useStore } from "./state.ts";

interface MediaDeviceInfo {
  deviceId: string;
  kind: MediaDeviceKind;
  label: string;
  groupId: string;
}

export function DeviceSelector(): JSX.Element {
  const state = useStore();
  const {
    actions,
    recordAudio,
    enabledVideoFormats,
    selectedCameraDeviceId,
    selectedAudioDeviceId,
    videoStarted,
    audioStream,
  } = state;

  const [devices, setDevices] = useState<MediaDeviceInfo[]>([]);

  const enableCamera = enabledVideoFormats.size > 0;
  const enableMicrophone = recordAudio;

  // Get available devices
  useEffect(() => {
    const getDevices = async () => {
      try {
        const deviceList = await navigator.mediaDevices.enumerateDevices();
        setDevices(deviceList);

        // Get current state values at the time of enumeration
        const currentState = useStore.getState();
        const currentCameraId = selectedCameraDeviceId; // Intentionally using render scope value
        const currentAudioId = currentState.selectedAudioDeviceId;
        const currentActions = currentState.actions;

        // Set default selections if not already set
        const cameras = deviceList.filter(
          (device) => device.kind === "videoinput",
        );
        const audioDevices = deviceList.filter(
          (device) => device.kind === "audioinput",
        );

        // Validate camera selection - fall back to first available if current selection is invalid
        if (cameras.length > 0) {
          const cameraExists = cameras.some(
            (camera) => camera.deviceId === currentCameraId,
          );
          if (!currentCameraId || !cameraExists) {
            currentActions.setSelectedCameraDeviceId(cameras[0]!.deviceId);
          }
        } else if (currentCameraId) {
          // No cameras available, clear selection
          currentActions.setSelectedCameraDeviceId("");
        }

        // Validate audio selection - fall back to first available if current selection is invalid
        if (audioDevices.length > 0) {
          const audioExists = audioDevices.some(
            (audio) => audio.deviceId === currentAudioId,
          );
          if (!currentAudioId || !audioExists) {
            currentActions.setSelectedAudioDeviceId(audioDevices[0]!.deviceId);
          }
        } else if (currentAudioId) {
          // No audio devices available, clear selection
          currentActions.setSelectedAudioDeviceId("");
        }
      } catch (error) {
        console.error("Error getting devices:", error);
      }
    };

    void getDevices();

    // Listen for device changes
    const handleDeviceChange = () => {
      void getDevices();
    };

    navigator.mediaDevices.addEventListener("devicechange", handleDeviceChange);
    return () => {
      navigator.mediaDevices.removeEventListener(
        "devicechange",
        handleDeviceChange,
      );
    };
  }, [videoStarted, audioStream, selectedCameraDeviceId]);

  const cameras = devices.filter((device) => device.kind === "videoinput");
  const audioDevices = devices.filter((device) => device.kind === "audioinput");

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
    return <></>;
  }

  return (
    <div className={styles.deviceSelector}>
      {enableCamera && cameras.length > 0 && (
        <>
          <label className={styles.deviceLabel}>Camera</label>
          <select
            className={styles.deviceSelect}
            value={selectedCameraDeviceId}
            onChange={(e) => {
              handleCameraChange(e.target.value);
            }}
          >
            {cameras.map((camera) => (
              <option key={camera.deviceId} value={camera.deviceId}>
                {camera.label || `Camera ${camera.deviceId.slice(0, 8)}...`}
              </option>
            ))}
          </select>
        </>
      )}

      {enableMicrophone && audioDevices.length > 0 && (
        <>
          <label className={styles.deviceLabel}>Microphone</label>
          <select
            className={styles.deviceSelect}
            value={selectedAudioDeviceId}
            onChange={(e) => {
              handleAudioChange(e.target.value);
            }}
          >
            {audioDevices.map((audio) => (
              <option key={audio.deviceId} value={audio.deviceId}>
                {audio.label || `Microphone ${audio.deviceId.slice(0, 8)}...`}
              </option>
            ))}
          </select>
        </>
      )}
    </div>
  );
}
