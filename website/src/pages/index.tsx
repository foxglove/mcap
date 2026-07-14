/* eslint-disable filenames/match-exported */
import BrowserOnly from "@docusaurus/BrowserOnly";
import Link from "@docusaurus/Link";
import useDocusaurusContext from "@docusaurus/useDocusaurusContext";
import CodeBlock from "@theme/CodeBlock";
import Layout from "@theme/Layout";
import React, { Suspense, useState } from "react";

import styles from "./index.module.css";
import * as icons from "../icons/index.ts";

// Async import is needed to avoid errors from WASM loading during Docusaurus build.
const McapRecordingDemo = React.lazy(async () => ({
  default: (
    await import("../components/McapRecordingDemo/McapRecordingDemo.tsx")
  ).McapRecordingDemo,
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
      "Store multiple channels of timestamped log data, such as pub/sub messages or multimodal sensor data.",
  },

  {
    title: "Serialization-agnostic",
    Icon: icons.DrawerEnvelope,
    description:
      "Record and replay binary messages in any format – like Protobuf, DDS (CDR), ROS, JSON, and more.",
  },
  {
    title: "High-performance writing",
    Icon: icons.SportsCarConvertible,
    description:
      "MCAP uses a row-oriented, append-only design to minimize disk I/O and reduce the risk of data corruption during unclean shutdowns.",
  },
  {
    title: "Self-contained",
    Icon: icons.ShipmentPackage,
    description:
      "MCAP stores message schemas alongside data, so your files remain readable in the future even as your codebase evolves.",
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
      "Choose between LZ4 or Zstandard for chunk-based compression, while still supporting efficient indexed reads.",
  },
  {
    title: "Broad language support",
    Icon: icons.ChatTranslate,
    description:
      "Native reader and writer libraries are available in C++, Go, Python, Rust, Swift, and TypeScript.",
  },
  {
    title: "Flexible",
    Icon: icons.YogaLegGrabStretch,
    description:
      "Configure optional features like chunking, indexing, CRC checksums, and compression to make the right tradeoffs for your application.",
  },
  {
    title: "Production-grade",
    Icon: icons.ArmyWoman1,
    description:
      "MCAP is used in production by a wide range of companies, from autonomous vehicles to drones, and is the default log format in ROS 2.",
  },
];

function Feature({ title, Icon, description }: FeatureItem) {
  return (
    <div>
      <Icon role="img" className={styles.featureIcon} />
      <h3>{title}</h3>
      <p>{description}</p>
    </div>
  );
}

const LanguageExamples: {
  name: string;
  language: string;
  code: string;
}[] = [
  {
    name: "Python",
    language: "python",
    code: `from mcap.writer import Writer

with open("out.mcap", "wb") as f:
    writer = Writer(f)
    writer.start()

    schema_id = writer.register_schema(
        name="ExampleMsg",
        encoding="jsonschema",
        data=b'{"type":"object","properties":{"value":{"type":"number"}}}',
    )
    channel_id = writer.register_channel(
        schema_id=schema_id,
        topic="/example",
        message_encoding="json",
    )

    writer.add_message(
        channel_id,
        log_time=0,
        data=b'{"value": 1.0}',
        publish_time=0,
    )

    writer.finish()`,
  },
  {
    name: "C++",
    language: "cpp",
    code: `#include <mcap/writer.hpp>

int main() {
  mcap::McapWriter writer;
  mcap::McapWriterOptions options("");
  writer.open("out.mcap", options);

  mcap::Schema schema("ExampleMsg", "jsonschema",
                       R"({"type":"object","properties":{"value":{"type":"number"}}})");
  writer.addSchema(schema);

  mcap::Channel channel("/example", "json", schema.id);
  writer.addChannel(channel);

  mcap::Message msg;
  msg.channelId = channel.id;
  msg.logTime = 0;
  msg.publishTime = 0;
  std::string data = R"({"value": 1.0})";
  msg.data = reinterpret_cast<const std::byte*>(data.data());
  msg.dataSize = data.size();
  writer.write(msg);

  writer.close();
}`,
  },
  {
    name: "Go",
    language: "go",
    code: `package main

import (
    "os"
    "github.com/foxglove/mcap/go/mcap"
)

func main() {
    f, _ := os.Create("out.mcap")
    defer f.Close()

    writer, _ := mcap.NewWriter(f, &mcap.WriterOptions{})
    writer.WriteHeader(&mcap.Header{})

    schema := &mcap.Schema{
        ID:       1,
        Name:     "ExampleMsg",
        Encoding: "jsonschema",
        Data:     []byte(\`{"type":"object","properties":{"value":{"type":"number"}}}\`),
    }
    writer.WriteSchema(schema)

    channel := &mcap.Channel{
        ID:              1,
        SchemaID:        1,
        Topic:           "/example",
        MessageEncoding: "json",
    }
    writer.WriteChannel(channel)

    writer.WriteMessage(&mcap.Message{
        ChannelID:   1,
        LogTime:     0,
        PublishTime: 0,
        Data:        []byte(\`{"value": 1.0}\`),
    })

    writer.Close()
}`,
  },
  {
    name: "Rust",
    language: "rust",
    code: `use mcap::{Writer, Channel, Schema};
use std::fs;
use std::sync::Arc;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut out = fs::File::create("out.mcap")?;
    let mut writer = Writer::new(&mut out)?;

    let schema = Arc::new(Schema {
        name: "ExampleMsg".to_string(),
        encoding: "jsonschema".to_string(),
        data: br#"{"type":"object","properties":{"value":{"type":"number"}}}"#.to_vec().into(),
    });

    let channel = Arc::new(Channel {
        topic: "/example".to_string(),
        message_encoding: "json".to_string(),
        schema: Some(schema),
        metadata: Default::default(),
    });

    writer.add_message(&channel, 0, 0, br#"{"value": 1.0}"#)?;
    writer.finish()?;
    Ok(())
}`,
  },
  {
    name: "TypeScript",
    language: "ts",
    code: `import { McapWriter } from "@mcap/core";
import { FileHandleWritable } from "@mcap/nodejs";

const writer = new McapWriter({ writable: await FileHandleWritable.create("out.mcap") });
await writer.start({ library: "example", profile: "" });

const schemaId = await writer.registerSchema({
  name: "ExampleMsg",
  encoding: "jsonschema",
  data: new TextEncoder().encode(
    JSON.stringify({ type: "object", properties: { value: { type: "number" } } }),
  ),
});

const channelId = await writer.registerChannel({
  topic: "/example",
  schemaId,
  messageEncoding: "json",
  metadata: new Map(),
});

await writer.addMessage({
  channelId,
  sequence: 0,
  logTime: 0n,
  publishTime: 0n,
  data: new TextEncoder().encode(JSON.stringify({ value: 1.0 })),
});

await writer.end();`,
  },
  {
    name: "Swift",
    language: "swift",
    code: `import MCAP

let writer = try MCAPWriter(toFile: "out.mcap")

let schemaId = try writer.addSchema(
    name: "ExampleMsg",
    encoding: "jsonschema",
    data: Data(#"{"type":"object","properties":{"value":{"type":"number"}}}"#.utf8)
)

let channelId = try writer.addChannel(
    schemaId: schemaId,
    topic: "/example",
    messageEncoding: "json"
)

try writer.addMessage(
    channelId: channelId,
    logTime: 0,
    publishTime: 0,
    data: Data(#"{"value": 1.0}"#.utf8)
)

try writer.close()`,
  },
];

function LanguageExamplePicker(): JSX.Element {
  const [selected, setSelected] = useState(LanguageExamples[0]!);
  return (
    <>
      <div className={styles.languageGrid} role="tablist">
        {LanguageExamples.map((example) => {
          const isSelected = selected.name === example.name;
          return (
            <button
              key={example.name}
              type="button"
              role="tab"
              aria-selected={isSelected}
              className={`${styles.languageCard ?? ""} ${
                isSelected ? (styles.languageCardSelected ?? "") : ""
              }`}
              onClick={() => {
                setSelected(example);
              }}
            >
              {example.name}
            </button>
          );
        })}
      </div>
      <div className={styles.languageCode}>
        <CodeBlock language={selected.language}>{selected.code}</CodeBlock>
      </div>
    </>
  );
}

function ComparisonTable(): JSX.Element {
  return (
    <>
      <header className={styles.comparisonHeader}>
        <h2>Why not just use rosbag or SQLite?</h2>
        <p className={styles.comparisonSubhead}>
          MCAP was designed to solve the shortcomings of the recording formats
          that came before it — see the full{" "}
          <Link to="/guides#history">design history</Link> for the underlying
          requirements.
        </p>
      </header>
      <div className={styles.comparisonTableWrapper}>
        <table className={styles.comparisonTable}>
        <thead>
          <tr>
            <th scope="col" />
            <th scope="col">
              rosbag1 (<code className={styles.inlineCode}>.bag</code>)
            </th>
            <th scope="col">
              rosbag2 default (SQLite{" "}
              <code className={styles.inlineCode}>.db3</code>)
            </th>
            <th scope="col">MCAP</th>
          </tr>
        </thead>
        <tbody>
          <tr>
            <th scope="row">Serialization agnostic</th>
            <td>No — ROS1 serialization only</td>
            <td>Yes</td>
            <td>Yes — ROS 1/2, Protobuf, JSON, FlatBuffers, custom</td>
          </tr>
          <tr>
            <th scope="row">Self-describing (schemas embedded)</th>
            <td>Yes</td>
            <td>
              Not by default (
              <Link to="https://github.com/ros2/rosbag2/issues/782">
                ros2/rosbag2#782
              </Link>
              )
            </td>
            <td>Yes</td>
          </tr>
          <tr>
            <th scope="row">Append-only / write-optimized</th>
            <td>Partial</td>
            <td>No — index updated per row insert</td>
            <td>Yes — chunked, streaming-safe</td>
          </tr>
          <tr>
            <th scope="row">Chunk-level compression</th>
            <td>No</td>
            <td>No (message-level only)</td>
            <td>LZ4 or Zstandard</td>
          </tr>
          <tr>
            <th scope="row">Corruption resilience</th>
            <td>Partial (chunk-recoverable, no checksums)</td>
            <td>Depends on journal mode</td>
            <td>
              Chunk CRCs +{" "}
              <code className={styles.inlineCode}>mcap recover</code>
            </td>
          </tr>
          <tr>
            <th scope="row">Efficient remote / random-access reads</th>
            <td>Index-based</td>
            <td>
              Full DB load required in browser (
              <code className={styles.inlineCode}>sql.js</code>)
            </td>
            <td>Indexed reads over HTTP range, S3, GCS, Azure</td>
          </tr>
          <tr>
            <th scope="row">Standards-track friendly</th>
            <td>Yes</td>
            <td>
              Difficult — SQLite has no independent second implementation
            </td>
            <td>Yes — open spec, multiple implementations</td>
          </tr>
          <tr>
            <th scope="row">Default in ROS 2</th>
            <td>—</td>
            <td>Iron and earlier</td>
            <td>Iron and later (current)</td>
          </tr>
        </tbody>
        </table>
      </div>
    </>
  );
}

export default function Home(): JSX.Element {
  const { siteConfig } = useDocusaurusContext();
  const blurb =
    'MCAP (pronounced "em-cap") is an open source container file format for logging and storing multimodal data. ' +
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
            <Link
              className={styles.heroButtonSecondary}
              to="https://github.com/foxglove/mcap"
            >
              View on GitHub
            </Link>
          </div>
        </div>
      </header>

      <div className={styles.section}>
        <div className="container">
          <div className={styles.splitLayout}>
            <div className={styles.demoColumn}>
              <BrowserOnly>
                {() => (
                  <Suspense fallback={""}>
                    <McapRecordingDemo />
                  </Suspense>
                )}
              </BrowserOnly>
            </div>
            <div className={styles.featuresColumn}>
              <div className={styles.featureGrid}>
                {FeatureList.map((props, idx) => (
                  <Feature key={idx} {...props} />
                ))}
              </div>
            </div>
          </div>
        </div>
      </div>

      <div className={styles.section}>
        <div className="container">
          <ComparisonTable />
        </div>
      </div>

      <div className={styles.section}>
        <div className="container">
          <header className={styles.comparisonHeader}>
            <h2>Write your first MCAP file</h2>
            <p className={styles.comparisonSubhead}>
              MCAP libraries are available in six languages. Pick one to see a
              minimal end-to-end example.
            </p>
          </header>
          <LanguageExamplePicker />
        </div>
      </div>

      <div className={`${styles.section ?? ""} ${styles.logosSection ?? ""}`}>
        <div className="container">
          <h2 className={styles.logosHeader}>
            Trusted by leading robotics teams
          </h2>
          <div className={styles.logoList}>
            {icons.Logos.map(({ href, LightModeLogo, DarkModeLogo }, idx) => (
              <a key={idx} href={href} className={styles.logoLink ?? ""}>
                <LightModeLogo
                  role="img"
                  className={`${styles.logoIcon ?? ""} ${
                    styles.lightMode ?? ""
                  }`}
                />
                <DarkModeLogo
                  role="img"
                  className={`${styles.logoIcon ?? ""} ${
                    styles.darkMode ?? ""
                  }`}
                />
              </a>
            ))}
          </div>
        </div>
      </div>
    </Layout>
  );
}
