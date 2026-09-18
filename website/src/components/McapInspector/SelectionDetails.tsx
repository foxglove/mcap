import React from "react";

import { bytes, timeLabel, type Recording } from "./model.ts";
import type { Selection } from "./timeline.ts";

export function SelectionDetails({
  selection,
  recording,
}: {
  selection?: Selection;
  recording?: Recording;
}): React.JSX.Element {
  if (!selection) {
    return (
      <>
        <h2>Inspect a message</h2>
        <p>
          Click a tick to select it. Its marker and channel stay highlighted
          while you inspect its details.
        </p>
        <p>
          Double-click a chunk to see only the channels and messages it
          contains.
        </p>
      </>
    );
  }
  const { channel, message, chunk, unchunked } = selection;
  const rows: [string, string][] = [];
  if (channel) {
    rows.push(
      ["Channel ID", String(channel.id)],
      ["Topic", channel.topic],
      ["Encoding", channel.encoding || "—"],
      ["Schema ID", String(channel.schemaId)],
    );
  }
  if (message) {
    rows.push(
      ["Relative log time", timeLabel(message.time)],
      ["Log time · ns", String(message.logTime)],
      ["Publish time · ns", String(message.publishTime)],
      ["Sequence", String(message.sequence)],
      ["Payload size", bytes(message.size)],
      ["Belongs to", chunk ? `Chunk #${chunk.id}` : "Unchunked record"],
    );
  }
  if (message && !chunk) {
    rows.push(["Record file offset", `${message.offset.toLocaleString()} B`]);
  }
  if (channel && !message) {
    rows.push([
      "Messages in loaded window",
      channel.messages.length.toLocaleString(),
    ]);
  }
  if (unchunked === true && recording) {
    rows.push([
      "Messages outside chunks",
      recording.looseCount.toLocaleString(),
    ]);
  }
  if (chunk && recording) {
    rows.push(
      ["Chunk", `#${chunk.id}`],
      ["Compression", chunk.compression],
      [
        "Messages in chunk",
        chunk.loaded === false
          ? "Not loaded"
          : chunk.messageCount.toLocaleString(),
      ],
      ["Start", timeLabel(Number(chunk.startTime - recording.startTime) / 1e9)],
      ["End", timeLabel(Number(chunk.endTime - recording.startTime) / 1e9)],
      ["File offset", `${chunk.offset.toLocaleString()} B`],
      ["Record size", bytes(chunk.byteLength)],
      ["Compressed records", bytes(chunk.compressedSize)],
      ["Uncompressed records", bytes(chunk.uncompressedSize)],
    );
  }
  return (
    <>
      <h2>
        {message
          ? "Selected message"
          : chunk
            ? `Chunk #${chunk.id}`
            : unchunked === true
              ? "Unchunked messages"
              : "Selected channel"}
      </h2>
      <dl>
        {rows.map(([label, value]) => (
          <div key={label}>
            <dt>{label}</dt>
            <dd>{value}</dd>
          </div>
        ))}
      </dl>
    </>
  );
}
