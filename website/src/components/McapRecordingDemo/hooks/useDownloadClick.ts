import { useCallback } from "react";

import { useStore } from "../state.ts";

/**
 * Hook to handle downloading recorded data as an MCAP file
 * @returns A callback function to handle download button clicks
 */
export function useDownloadClick(): (
  event: React.MouseEvent<HTMLAnchorElement>,
) => void {
  const { actions } = useStore();

  return useCallback(
    (event: React.MouseEvent<HTMLAnchorElement>) => {
      event.preventDefault();
      void (async () => {
        const blob = await actions.closeAndRestart();
        const url = URL.createObjectURL(blob);
        const link = document.createElement("a");
        link.href = url;

        // Create a date+time string in the local timezone to use as the filename
        const date = new Date();
        const localTime = new Date(
          date.getTime() - date.getTimezoneOffset() * 60_000,
        )
          .toISOString()
          .replace(/\..+$/, "")
          .replace("T", "_")
          .replaceAll(":", "-");

        link.download = `demo_${localTime}.mcap`;
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
        URL.revokeObjectURL(url);
        actions.setShowDownloadInfo({ shouldShow: true });
      })();
    },
    [actions],
  );
}
