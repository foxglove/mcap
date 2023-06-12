/* eslint-disable filenames/match-exported */
import BrowserOnly from "@docusaurus/BrowserOnly";
import Link from "@docusaurus/Link";
import useDocusaurusContext from "@docusaurus/useDocusaurusContext";
import Layout from "@theme/Layout";
import React, { Suspense } from "react";

import styles from "./index.module.css";
import * as icons from "../icons";

// Async import is needed to avoid errors from WASM loading during Docusaurus build.
const McapRecordingDemo = React.lazy(async () => ({
  default: (await import("../components/McapRecordingDemo/McapRecordingDemo"))
    .McapRecordingDemo,
}));

type FeatureItem = {
  title: string;
  Icon: React.ComponentType<React.SVGProps<SVGSVGElement>>;
  description: string;
};

const FeatureList: FeatureItem[] = [
  {
    title: "Pub/sub logging",
    Icon: icons.Robot,
    description:
      "Store multiple channels of timestamped log data, including pub/sub messages or multimodal sensor data.",
  },

  {
    title: "Serialization-agnostic format",
    Icon: icons.DrawerEnvelope,
    description:
      "Record and replay binary messages in any format – like Protobuf, DDS (CDR), ROS, JSON, and more.",
  },
  {
    title: "High-performance writing",
    Icon: icons.SportsCarConvertible,
    description:
      "Use MCAP's row-oriented, append-only design to minimize disk I/O and reduce the risk of data corruption during unclean shutdowns.",
  },
  {
    title: "Self-contained",
    Icon: icons.ShipmentPackage,
    description:
      "Store your data alongside the schemas required to deserialize it, so your files can remain readable even as your codebase evolves.",
  },
  {
    title: "Efficient seeking",
    Icon: icons.Turntable,
    description:
      "Get fast indexed reading, even over a low-bandwidth connection.",
  },
  {
    title: "Optional compression",
    Icon: icons.ZipFile,
    description:
      "Choose between LZ4 or Zstandard for chunk-based compression, while still supporting efficient indexed reads.",
  },
  {
    title: "Broad language support",
    Icon: icons.ChatTranslate,
    description:
      "Use native MCAP reader and writer libraries in C++, Go, Python, Rust, Swift, and TypeScript.",
  },
  {
    title: "Flexibility",
    Icon: icons.YogaLegGrabStretch,
    description:
      "Configure optional features like chunking, indexing, CRC checksums, and compression to make the right tradeoffs for your application.",
  },
  {
    title: "Production-grade standards",
    Icon: icons.ArmyWoman1,
    description:
      "Become one of the many robotics companies that deploy MCAP in production. As the default log format for ROS 2, MCAP offers long-term support and technical robustness.",
  },
];

function Feature({ title, Icon, description }: FeatureItem) {
  return (
    <div className={styles.featureItem}>
      <Icon role="img" className={styles.featureIcon} />
      <h3>{title}</h3>
      <p>{description}</p>
    </div>
  );
}

export default function Home(): JSX.Element {
  const { siteConfig } = useDocusaurusContext();
  const blurb =
    'MCAP (pronounced "em-cap") is an open source container file format for multimodal log data. ' +
    "It supports multiple channels of timestamped pre-serialized data, and is ideal for use in pub/sub " +
    "or robotics applications.";

  return (
    <Layout description={blurb}>
      <header className={styles.hero}>
        <div className="container">
          <div className={styles.heroLogo}>
            <h1>{siteConfig.title}</h1>
            <img
              src="/img/mcap720.webp"
              alt="logo"
              width="240"
              height="180"
            ></img>
          </div>
          <p className={styles.blurb}>{blurb}</p>
          <div className={styles.heroButtons}>
            <Link className={styles.heroButtonPrimary} to="/guides">
              Get Started
            </Link>
            <Link className={styles.heroButtonSecondary} to="/reference">
              API Reference
            </Link>
          </div>
        </div>
      </header>

      <div className={styles.section}>
        <div className="container">
          <div className={styles.featureGrid}>
            {FeatureList.map((props, idx) => (
              <Feature key={idx} {...props} />
            ))}
          </div>
        </div>
      </div>

      <BrowserOnly>
        {() => (
          <Suspense fallback={""}>
            <McapRecordingDemo />
          </Suspense>
        )}
      </BrowserOnly>
    </Layout>
  );
}
