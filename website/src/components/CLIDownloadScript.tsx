import { usePluginData } from "@docusaurus/useGlobalData";
import CodeBlock from "@theme/CodeBlock";
import React from "react";

export default function CLIDownloadScript(): JSX.Element {
  const latestVersion = usePluginData("latestCLIReleaseTag") as { tag: string };
  const shell = `wget https://github.com/foxglove/mcap/releases/download/${encodeURIComponent(
    latestVersion.tag,
  )}/mcap-linux-$(arch) -O mcap`;

  return <CodeBlock language="bash">$ {shell}</CodeBlock>;
}
