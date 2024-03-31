import { usePluginData } from "@docusaurus/useGlobalData";
import CodeBlock from "@theme/CodeBlock";
import React from "react";

export default function CLIDownloadScript(): JSX.Element {
  const latestVersion = usePluginData("latestCLIReleaseTag") as { tag: string };
  const tag = encodeURIComponent(latestVersion.tag);

  return (
    <CodeBlock language="bash">
      $ wget https://github.com/foxglove/mcap/releases/download/{tag}
      /mcap-linux-$(arch) -O mcap
    </CodeBlock>
  );
}
