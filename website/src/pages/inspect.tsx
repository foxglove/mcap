/* eslint-disable filenames/match-exported */
import Layout from "@theme/Layout";
import React from "react";

import { McapInspector } from "../components/McapInspector/McapInspector.tsx";

export default function InspectPage(): React.JSX.Element {
  return (
    <Layout
      title="MCAP inspector"
      description="Inspect MCAP channels, messages, and overlapping chunks locally in your browser."
    >
      <McapInspector />
    </Layout>
  );
}
