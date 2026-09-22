import React, { useRef, type CSSProperties } from "react";

/** A pointer- and keyboard-accessible splitter; values are CSS pixels. */
export function ResizeHandle({
  axis,
  label,
  value,
  min,
  max,
  onChange,
  onReset,
  style,
}: {
  axis: "x" | "y";
  label: string;
  value: number;
  min: number;
  max: number;
  onChange: (value: number) => void;
  onReset: () => void;
  style?: CSSProperties;
}): React.JSX.Element {
  const drag = useRef<
    { pointer: number; origin: number; value: number } | undefined
  >(undefined);
  const change = (next: number) => {
    onChange(Math.max(min, Math.min(max, next)));
  };
  return (
    <div
      className={`resize-handle resize-${axis}`}
      style={style}
      role="separator"
      tabIndex={0}
      aria-label={label}
      aria-orientation={axis === "x" ? "vertical" : "horizontal"}
      aria-valuemin={Math.round(min)}
      aria-valuemax={Math.round(max)}
      aria-valuenow={Math.round(value)}
      aria-valuetext={`${Math.round(value)} pixels`}
      title={`${label}. Drag or use arrow keys; double-click to reset.`}
      onPointerDown={(event) => {
        if (event.button !== 0) {
          return;
        }
        event.preventDefault();
        event.currentTarget.focus();
        event.currentTarget.setPointerCapture(event.pointerId);
        drag.current = {
          pointer: event.pointerId,
          origin: axis === "x" ? event.clientX : event.clientY,
          value,
        };
      }}
      onPointerMove={(event) => {
        const active = drag.current;
        if (active?.pointer === event.pointerId) {
          change(
            active.value +
              (axis === "x" ? event.clientX : event.clientY) -
              active.origin,
          );
        }
      }}
      onPointerUp={(event) => {
        if (drag.current?.pointer === event.pointerId) {
          drag.current = undefined;
          event.currentTarget.releasePointerCapture(event.pointerId);
        }
      }}
      onPointerCancel={() => {
        drag.current = undefined;
      }}
      onLostPointerCapture={() => {
        drag.current = undefined;
      }}
      onDoubleClick={onReset}
      onKeyDown={(event) => {
        const decrease = axis === "x" ? "ArrowLeft" : "ArrowUp";
        const increase = axis === "x" ? "ArrowRight" : "ArrowDown";
        const step = event.shiftKey ? 50 : 10;
        if (event.key === decrease || event.key === increase) {
          event.preventDefault();
          change(value + (event.key === increase ? step : -step));
        } else if (event.key === "Home" || event.key === "End") {
          event.preventDefault();
          change(event.key === "Home" ? min : max);
        } else if (event.key === "Enter") {
          event.preventDefault();
          onReset();
        }
      }}
    />
  );
}
