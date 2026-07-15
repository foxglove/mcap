/* eslint-disable filenames/match-exported */
import BrowserOnly from "@docusaurus/BrowserOnly";
import Link from "@docusaurus/Link";
import useDocusaurusContext from "@docusaurus/useDocusaurusContext";
import CodeBlock from "@theme/CodeBlock";
import Layout from "@theme/Layout";
import TabItem from "@theme/TabItem";
import Tabs from "@theme/Tabs";
import React, { Suspense } from "react";

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
  msg.sequence = 0;
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
    code: `use mcap::{records::MessageHeader, Writer};
use std::{collections::BTreeMap, fs};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut out = Writer::new(fs::File::create("out.mcap")?)?;
    let schema_id = out.add_schema(
        "ExampleMsg",
        "jsonschema",
        br#"{"type":"object","properties":{"value":{"type":"number"}}}"#,
    )?;
    let channel_id = out.add_channel(schema_id, "/example", "json", &BTreeMap::new())?;
    out.write_to_known_channel(
        &MessageHeader { channel_id, sequence: 0, log_time: 0, publish_time: 0 },
        br#"{"value": 1.0}"#,
    )?;
    out.finish()?;
    Ok(())
}`,
  },
  {
    name: "TypeScript",
    language: "ts",
    code: `import { McapWriter } from "@mcap/core";
import { FileHandleWritable } from "@mcap/nodejs";
import { open } from "node:fs/promises";

const writable = new FileHandleWritable(await open("out.mcap", "w"));
const writer = new McapWriter({ writable });
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
    code: `import Foundation
import MCAP

final class FileWritable: IWritable {
    private let handle: FileHandle

    init(path: String) throws {
        FileManager.default.createFile(atPath: path, contents: nil)
        handle = try FileHandle(forWritingTo: URL(fileURLWithPath: path))
    }

    func position() -> UInt64 {
        handle.offsetInFile
    }

    func write(_ data: Data) async {
        handle.write(data)
    }

    func close() throws {
        try handle.close()
    }
}

let sink = try FileWritable(path: "out.mcap")
defer { try? sink.close() }

let writer = MCAPWriter(sink)
await writer.start(library: "example", profile: "")

let schemaId = await writer.addSchema(
    name: "ExampleMsg",
    encoding: "jsonschema",
    data: Data(#"{"type":"object","properties":{"value":{"type":"number"}}}"#.utf8)
)

let channelId = await writer.addChannel(
    schemaID: schemaId,
    topic: "/example",
    messageEncoding: "json",
    metadata: [:]
)

await writer.addMessage(
    Message(
        channelID: channelId,
        sequence: 0,
        logTime: 0,
        publishTime: 0,
        data: Data(#"{"value": 1.0}"#.utf8)
    )
)

await writer.end()`,
  },
];

function LanguageExamplePicker(): JSX.Element {
  return (
    <Tabs>
      {LanguageExamples.map((example, index) => (
        <TabItem
          key={example.name}
          value={example.language}
          label={example.name}
          default={index === 0}
        >
          <CodeBlock language={example.language}>{example.code}</CodeBlock>
        </TabItem>
      ))}
    </Tabs>
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

      <div
        className={`${styles.section ?? ""} ${styles.featuresSection ?? ""}`}
      >
        <div className="container">
          <div className={styles.featureGrid}>
            {FeatureList.map((props, idx) => (
              <Feature key={idx} {...props} />
            ))}
          </div>
        </div>
      </div>

      <div className={`${styles.section ?? ""} ${styles.demoSection ?? ""}`}>
        <div className="container">
          <div className={styles.demoSplit}>
            <div className={styles.demoColumn}>
              <BrowserOnly>
                {() => (
                  <Suspense fallback={""}>
                    <McapRecordingDemo />
                  </Suspense>
                )}
              </BrowserOnly>
            </div>
            <aside className={styles.conversionColumn}>
              <h2>Already have ROS bag or ROS 2 .db3 data?</h2>
              <p>
                Skip straight to{" "}
                <Link to="/guides/cli#ros-bag-to-mcap-conversion">
                  converting existing recordings
                </Link>{" "}
                with the mcap CLI instead of writing files from scratch.
              </p>
              <p className={styles.foxgloveCredit}>
                MCAP is an open source project by{" "}
                <Link to="https://foxglove.dev">Foxglove</Link>.
              </p>
            </aside>
          </div>
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

      <div className={`${styles.section ?? ""} ${styles.codeSection ?? ""}`}>
        <div className="container">
          <header className={styles.sectionHeader}>
            <h2>Write your first MCAP file</h2>
            <p className={styles.sectionSubhead}>
              MCAP libraries are available in six languages. Pick one to see a
              minimal end-to-end example.
            </p>
          </header>
          <LanguageExamplePicker />
        </div>
      </div>
    </Layout>
  );
}
