import React, { useEffect, useRef, type CSSProperties } from "react";

import { createInspector, type InspectorHandle } from "./createInspector.ts";
import type { Recording } from "./model.ts";

export interface McapInspectorProps {
  /** Optional controlled input. Omitting it leaves the picker/drop zone available. */
  file?: File;
  /** Height of the canvas viewport, in CSS pixels. */
  height?: number;
  className?: string;
  onLoad?: (recording: Recording) => void;
  onError?: (error: Error) => void;
}

/** A local-only MCAP structural inspector. Safe to import during server rendering. */
export function McapInspector({
  file,
  height = 520,
  className,
  onLoad,
  onError,
}: McapInspectorProps): React.JSX.Element {
  const host = useRef<HTMLDivElement>(null);
  const inspector = useRef<InspectorHandle | undefined>(undefined);
  const callbacks = useRef({ onLoad, onError });
  useEffect(() => {
    callbacks.current = { onLoad, onError };
  }, [onLoad, onError]);
  useEffect(() => {
    if (!host.current) {
      return;
    }
    const mounted = createInspector(host.current, {
      createWorker: () =>
        new Worker(new URL("./loader.worker.ts", import.meta.url), {
          type: "module",
        }),
      onLoad: (recording) => callbacks.current.onLoad?.(recording),
      onError: (error) => callbacks.current.onError?.(error),
    });
    inspector.current = mounted;
    return () => {
      inspector.current = undefined;
      mounted.destroy();
    };
  }, []);
  useEffect(() => {
    if (file) {
      inspector.current?.loadFile(file);
    }
  }, [file]);
  const style = { "--mcap-inspector-height": `${height}px` } as CSSProperties;
  return (
    <div
      ref={host}
      className={className}
      style={style}
      aria-label="MCAP chunk inspector"
    />
  );
}
