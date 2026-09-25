import React from "react";
import { flushSync } from "react-dom";
import { createRoot } from "react-dom/client";

import { InspectorApp } from "./InspectorApp.tsx";
import type {
  InspectorControls,
  InspectorHandle,
  InspectorOptions,
} from "./InspectorTypes.ts";
import { inspectorStyles } from "./styles.ts";

export type { InspectorHandle, InspectorOptions } from "./InspectorTypes.ts";

/** Framework-independent mounting adapter; the same React UI is used by every host. */
export function createInspector(
  host: HTMLElement,
  options: InspectorOptions = {},
): InspectorHandle {
  if (host.childNodes.length > 0) {
    throw new Error("The inspector host must be empty.");
  }
  const shadow = host.shadowRoot ?? host.attachShadow({ mode: "open" });
  if (shadow.childNodes.length > 0) {
    throw new Error("The inspector host must be empty.");
  }
  const root = createRoot(shadow);
  const ref = React.createRef<InspectorControls>();
  flushSync(() => {
    root.render(
      React.createElement(
        React.Fragment,
        null,
        React.createElement("style", null, inspectorStyles),
        React.createElement(InspectorApp, {
          ...options,
          createWorker:
            options.createWorker ??
            (() =>
              new Worker(new URL("./loader.worker.ts", import.meta.url), {
                type: "module",
              })),
          ref,
        }),
      ),
    );
  });
  function controls(): InspectorControls {
    if (!ref.current) {
      throw new Error("The inspector is not mounted or has been destroyed.");
    }
    return ref.current;
  }
  let destroyed = false;
  return {
    loadFile: (file) => {
      controls().loadFile(file);
    },
    loadReadable: (readable, name) => {
      controls().loadReadable(readable, name);
    },
    setRecording: (recording) => {
      controls().setRecording(recording);
    },
    focusChunk: (id) => {
      controls().focusChunk(id);
    },
    exitChunk: () => {
      controls().exitChunk();
    },
    destroy: () => {
      if (!destroyed) {
        destroyed = true;
        root.unmount();
      }
    },
  };
}
