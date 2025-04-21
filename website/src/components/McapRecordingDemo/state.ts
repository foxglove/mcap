import { fromMillis } from "@foxglove/rostime";
import { PoseInFrame } from "@foxglove/schemas";
import { create } from "zustand";

import {
  MouseEventMessage,
  ProtobufObject,
  Recorder,
  toProtobufTime,
} from "./Recorder";
import type { AudioDataMessage } from "./audioCapture";
import type { CompressedVideoFrame } from "./videoCapture";

type McapDemoState = {
  bytesWritten: bigint;
  messageCount: bigint;
  chunkCount: number;

  latestMouse: MouseEventMessage | undefined;
  latestOrientation: DeviceOrientationEvent | undefined;

  // Recording state
  recording: boolean;
  orientationPermissionError: boolean;
  showDownloadInfo: boolean;

  // Recording options
  recordJpeg: boolean;
  recordH264: boolean;
  recordH265: boolean;
  recordVP9: boolean;
  recordAV1: boolean;
  recordAudio: boolean;
  recordMouse: boolean;
  recordOrientation: boolean;

  // Media state
  videoStarted: boolean;
  videoError: Error | undefined;
  audioError: Error | undefined;
  audioStream: MediaStream | undefined;

  // Actions
  addMouseEventMessage: (msg: MouseEventMessage) => void;
  addPoseMessage: (msg: DeviceOrientationEvent) => void;
  addJpegFrame: (blob: Blob) => void;
  addVideoFrame: (frame: CompressedVideoFrame) => void;
  addAudioData: (data: AudioDataMessage) => void;
  closeAndRestart: () => Promise<Blob>;
  setRecording: (value: { isRecording: boolean }) => void;
  setOrientationPermissionError: (value: { hasError: boolean }) => void;
  setShowDownloadInfo: (value: { shouldShow: boolean }) => void;
  setRecordJpeg: (value: { shouldRecord: boolean }) => void;
  setRecordH264: (value: { shouldRecord: boolean }) => void;
  setRecordH265: (value: { shouldRecord: boolean }) => void;
  setRecordVP9: (value: { shouldRecord: boolean }) => void;
  setRecordAV1: (value: { shouldRecord: boolean }) => void;
  setRecordAudio: (value: { shouldRecord: boolean }) => void;
  setRecordMouse: (value: { shouldRecord: boolean }) => void;
  setRecordOrientation: (value: { shouldRecord: boolean }) => void;
  setVideoStarted: (value: { isStarted: boolean }) => void;
  setVideoError: (error: Error | undefined) => void;
  setAudioError: (error: Error | undefined) => void;
  setAudioStream: (stream: MediaStream | undefined) => void;
};

const RADIANS_PER_DEGREE = Math.PI / 180;

// Adapted from https://github.com/mrdoob/three.js/blob/master/src/math/Quaternion.js
function deviceOrientationToPose(
  event: DeviceOrientationEvent,
): ProtobufObject<PoseInFrame> {
  const alpha = (event.alpha ?? 0) * RADIANS_PER_DEGREE; // z angle
  const beta = (event.beta ?? 0) * RADIANS_PER_DEGREE; // x angle
  const gamma = (event.gamma ?? 0) * RADIANS_PER_DEGREE; // y angle

  const c1 = Math.cos(beta / 2);
  const c2 = Math.cos(gamma / 2);
  const c3 = Math.cos(alpha / 2);

  const s1 = Math.sin(beta / 2);
  const s2 = Math.sin(gamma / 2);
  const s3 = Math.sin(alpha / 2);

  const x = s1 * c2 * c3 - c1 * s2 * s3;
  const y = c1 * s2 * c3 + s1 * c2 * s3;
  const z = c1 * c2 * s3 + s1 * s2 * c3;
  const w = c1 * c2 * c3 - s1 * s2 * s3;

  return {
    timestamp: toProtobufTime(fromMillis(event.timeStamp)),
    frame_id: "device",
    pose: { position: { x: 0, y: 0, z: 0 }, orientation: { x, y, z, w } },
  };
}

export function formatBytes(totalBytes: number): string {
  const units = ["B", "kiB", "MiB", "GiB", "TiB"];
  let bytes = totalBytes;
  let unit = 0;
  while (unit + 1 < units.length && bytes >= 1024) {
    bytes /= 1024;
    unit++;
  }
  return `${bytes.toFixed(unit === 0 ? 0 : 1)} ${units[unit]!}`;
}

export const useStore = create<McapDemoState>((set) => {
  const recorder = new Recorder();
  recorder.addListener("update", () => {
    set({
      bytesWritten: recorder.bytesWritten,
      messageCount: recorder.messageCount,
      chunkCount: recorder.chunkCount,
    });
  });

  return {
    bytesWritten: recorder.bytesWritten,
    messageCount: recorder.messageCount,
    chunkCount: recorder.chunkCount,
    latestMouse: undefined,
    latestOrientation: undefined,

    // Recording state
    recording: false,
    orientationPermissionError: false,
    showDownloadInfo: false,

    // Recording options
    recordJpeg: false,
    recordH264: false,
    recordH265: false,
    recordVP9: false,
    recordAV1: false,
    recordAudio: false,
    recordMouse: true,
    recordOrientation: true,

    // Media state
    videoStarted: false,
    videoError: undefined,
    audioError: undefined,
    audioStream: undefined,

    // Actions
    addMouseEventMessage(msg: MouseEventMessage) {
      void recorder.addMouseEvent(msg);
      set({ latestMouse: msg });
    },
    addPoseMessage(msg: DeviceOrientationEvent) {
      void recorder.addPose(deviceOrientationToPose(msg));
      set({ latestOrientation: msg });
    },
    addJpegFrame(blob: Blob) {
      void recorder.addJpegFrame(blob);
    },
    addVideoFrame(frame: CompressedVideoFrame) {
      void recorder.addVideoFrame(frame);
    },
    addAudioData(data: AudioDataMessage) {
      void recorder.addAudioData(data);
    },
    async closeAndRestart() {
      return await recorder.closeAndRestart();
    },
    setRecording: ({ isRecording }) => {
      set({ recording: isRecording });
    },
    setOrientationPermissionError: ({ hasError }) => {
      set({ orientationPermissionError: hasError });
    },
    setShowDownloadInfo: ({ shouldShow }) => {
      set({ showDownloadInfo: shouldShow });
    },
    setRecordJpeg: ({ shouldRecord }) => {
      set({ recordJpeg: shouldRecord });
    },
    setRecordH264: ({ shouldRecord }) => {
      set({ recordH264: shouldRecord });
    },
    setRecordH265: ({ shouldRecord }) => {
      set({ recordH265: shouldRecord });
    },
    setRecordVP9: ({ shouldRecord }) => {
      set({ recordVP9: shouldRecord });
    },
    setRecordAV1: ({ shouldRecord }) => {
      set({ recordAV1: shouldRecord });
    },
    setRecordAudio: ({ shouldRecord }) => {
      set({ recordAudio: shouldRecord });
    },
    setRecordMouse: ({ shouldRecord }) => {
      set({ recordMouse: shouldRecord });
    },
    setRecordOrientation: ({ shouldRecord }) => {
      set({ recordOrientation: shouldRecord });
    },
    setVideoStarted: ({ isStarted }) => {
      set({ videoStarted: isStarted });
    },
    setVideoError: (error) => {
      set({ videoError: error });
    },
    setAudioError: (error) => {
      set({ audioError: error });
    },
    setAudioStream: (stream) => {
      set({ audioStream: stream });
    },
  };
});
