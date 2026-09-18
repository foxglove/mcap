import React from "react";

import { timeLabel } from "./model.ts";

export function viewportPercentages(
  start: number,
  span: number,
  duration: number,
): { left: number; width: number } {
  const left =
    duration > 0 ? Math.max(0, Math.min(100, (start / duration) * 100)) : 0;
  return {
    left,
    width:
      duration > 0
        ? Math.max(0, Math.min(100 - left, (span / duration) * 100))
        : 100,
  };
}

export function ViewportNavigator({
  start,
  span,
  duration,
  onSeek,
}: {
  start: number;
  span: number;
  duration: number;
  onSeek: (fraction: number) => void;
}): React.JSX.Element {
  const { left, width } = viewportPercentages(start, span, duration);
  return (
    <div className="navigator">
      <span>0 s</span>
      <div className="overview">
        <div className="overview-track">
          <div
            className="viewport-range"
            style={{ left: `${left}%`, width: `${width}%` }}
          />
        </div>
        <input
          type="range"
          min={0}
          max={10000}
          value={
            duration > span
              ? Math.round((start / (duration - span)) * 10000)
              : 0
          }
          disabled={duration <= span}
          aria-label="Pan visible time range"
          aria-valuetext={`${timeLabel(start)} to ${timeLabel(start + span)}`}
          onChange={(event) => {
            onSeek(Number(event.target.value) / 10000);
          }}
        />
      </div>
      <span>{timeLabel(duration)}</span>
      <span id="window-size">
        Viewing {timeLabel(start)} – {timeLabel(start + span)}
      </span>
    </div>
  );
}
