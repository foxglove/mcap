import React from "react";
import Link from "@docusaurus/Link";
import useDocusaurusContext from "@docusaurus/useDocusaurusContext";
import Layout from "@theme/Layout";
import * as icons from "../icons";

import styles from "./index.module.css";
import { McapRecordingDemo } from "../components/McapRecordingDemo";

type FeatureItem = {
  title: string;
  Icon: React.ComponentType<React.SVGProps<SVGSVGElement>>;
  description: string;
};

const FeatureList: FeatureItem[] = [
  {
    title: "Pub/Sub logging",
    Icon: icons.Robot,
    description:
      "MCAP is ideal for storing multiple channels of timestamped log data, such as pub/sub messages or multimodal sensor data.",
  },

  {
    title: "Serialization agnostic",
    Icon: icons.DrawerEnvelope,
    description:
      "MCAP is a container format, allowing you to record and replay binary messages in any format, such as Protobuf, DDS (CDR), ROS, JSON, etc.",
  },
  {
    title: "High-performance writing",
    Icon: icons.SportsCarConvertible,
    description:
      "MCAP utilizes a row-oriented, append-only design. This minimizes disk I/O, and reduces the risk of data corruption during an unclean shutdown. ",
  },
  {
    title: "Self-contained",
    Icon: icons.ShipmentPackage,
    description:
      "MCAP files are fully self-contained, including schemas required to deserialize each channel. Older files always remain readable, even as your codebase evolves.",
  },
  {
    title: "Efficient seeking",
    Icon: icons.Turntable,
    description:
      "MCAP files contain an optional index, allowing for fast, efficient reading, even over a low-bandwidth internet connection.",
  },
  {
    title: "Optional compression",
    Icon: icons.ZipFile,
    description:
      "Data can be compressed using lz4 or zstd, while still supporting efficient indexed reads and chunk-based decompression.",
  },
  {
    title: "Multilingual",
    Icon: icons.ChatTranslate,
    description:
      "MCAP reader and writer libraries are available in many languages, including C++, Go, Python, Rust, Swift, and Typescript.",
  },
  {
    title: "Flexible",
    Icon: icons.YogaLegGrabStretch,
    description:
      "Features such as chunking, indexing, CRC checksums, and compression are optional. You choose the right tradeoffs for your application.",
  },
  {
    title: "Production grade",
    Icon: icons.ArmyWoman1,
    description:
      "MCAP is used in production by a wide range of companies, from autonomous vehicles to drones, and is the default log format in ROS 2.",
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

export default function Home() {
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
          <div>
            <Link className={styles.heroButtonPrimary} to="/guides">
              Get Started
            </Link>
            <Link className={styles.heroButtonSecondary} to="/reference">
              API Reference
            </Link>
          </div>
          <McapRecordingDemo />
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
    </Layout>
  );
}
