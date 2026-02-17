import { useEffect } from "react";

import { useStore } from "../state.ts";

/**
 * Hook to record mouse events (pointer down and move) during recording
 */
export function useMouseEventRecording(): void {
  const { actions, recording, recordMouse } = useStore();

  useEffect(() => {
    if (!recording || !recordMouse) {
      return;
    }
    const handleMouseEvent = (event: PointerEvent): void => {
      actions.addMouseEventMessage({
        clientX: event.clientX,
        clientY: event.clientY,
      });
    };
    window.addEventListener("pointerdown", handleMouseEvent);
    window.addEventListener("pointermove", handleMouseEvent);
    return () => {
      window.removeEventListener("pointerdown", handleMouseEvent);
      window.removeEventListener("pointermove", handleMouseEvent);
    };
  }, [actions, recording, recordMouse]);
}
