import React, { forwardRef, useEffect, useRef, useState } from "react";
import { createPortal } from "react-dom";

import { InspectorApp, type InspectorAppProps } from "./InspectorApp.tsx";
import type { InspectorControls } from "./InspectorTypes.ts";
import { inspectorStyles } from "./styles.ts";

export interface McapInspectorProps
  extends Omit<InspectorAppProps, "createWorker"> {
  createWorker?: () => Worker;
  className?: string;
}

function createWorker() {
  return new Worker(new URL("./loader.worker.ts", import.meta.url), {
    type: "module",
  });
}

/** SSR-safe React UI with per-instance style isolation and a seekable input API. */
export const McapInspector = forwardRef<InspectorControls, McapInspectorProps>(
  function McapInspectorView({ className, ...props }, ref) {
    const host = useRef<HTMLDivElement>(null);
    const [shadow, setShadow] = useState<ShadowRoot>();
    useEffect(() => {
      if (host.current) {
        setShadow(
          host.current.shadowRoot ??
            host.current.attachShadow({ mode: "open" }),
        );
      }
    }, []);
    return (
      <div ref={host} className={className} aria-label="MCAP inspector">
        {shadow &&
          createPortal(
            <>
              <style>{inspectorStyles}</style>
              <InspectorApp
                {...props}
                createWorker={props.createWorker ?? createWorker}
                ref={ref}
              />
            </>,
            shadow,
          )}
      </div>
    );
  },
);
