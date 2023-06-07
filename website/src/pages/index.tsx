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
      "Store multiple channels of timestamped log data, like pub/sub messages or multimodal sensor data.",
  },

  {
    title: "Serialization-agnostic container format",
    Icon: icons.DrawerEnvelope,
    description:
      "Record and replay binary messages in any format – like Protobuf, DDS (CDR), ROS, JSON, and more.",
  },
  {
    title: "High-performance writing",
    Icon: icons.SportsCarConvertible,
    description:
      "Leverage MCAP's append-only design to minimize disk I/O and reduce the risk of data corruption during an unclean shutdown.",
  },
  {
    title: "Self-contained files",
    Icon: icons.ShipmentPackage,
    description:
      "Store your data alongside the schemas required to deserialize it, so your files can remain readable even as your codebase evolves.",
  },
  {
    title: "Efficient seeking",
    Icon: icons.Turntable,
    description:
      "Enjoy fast indexed reading, even over a low-bandwidth connection.",
  },
  {
    title: "Optional compression",
    Icon: icons.ZipFile,
    description:
      "Choose between compressing your data with lz4 or zstd, while still leveraging chunk-based decompression for efficient reading.",
  },
  {
    title: "Multilingual support",
    Icon: icons.ChatTranslate,
    description:
      "Take advantage of MCAP reader and writer libraries in C++, Go, Python, Rust, Swift, Typescript, and more.",
  },
  {
    title: "Flexibility",
    Icon: icons.YogaLegGrabStretch,
    description:
      "Leverage optional features like chunking, indexing, CRC checksums, and compression to make the right tradeoffs for your application.",
  },
  {
    title: "Production-grade performance",
    Icon: icons.ArmyWoman1,
    description:
      "Find yourself among the robotics companies that trust MCAP, the default log format for ROS 2, as the new industry standard.",
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
