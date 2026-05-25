use std::collections::{BTreeMap, HashMap};
use std::io::{Seek, Write};
use std::path::Path;

use anyhow::{bail, ensure, Context, Result};
use rusqlite::{Connection, OpenFlags};

const EMBEDDED_SCHEMA_ERROR: &str = "This ROS 2 SQLite bag does not contain embedded message definitions, so the MCAP CLI cannot convert it safely. This usually means it was recorded with ROS 2 Humble or earlier. Use `ros2 bag convert` from a sourced ROS 2 workspace with the rosbag2_storage_mcap plugin instead.";

#[derive(Debug, Clone)]
struct TopicRecord {
    id: i64,
    name: String,
    typ: String,
    serialization_format: String,
    offered_qos_profiles: Option<String>,
}

#[derive(Debug, Clone)]
struct MessageDefinitionRecord {
    id: i64,
    topic_type: String,
    encoding: String,
    encoded_message_definition: String,
}

pub fn convert_ros2_db3<W: Write + Seek>(
    output: W,
    input_path: &Path,
    write_options: mcap::WriteOptions,
) -> Result<()> {
    let db = Connection::open_with_flags(input_path, OpenFlags::SQLITE_OPEN_READ_ONLY)
        .with_context(|| format!("failed to open ROS2 db3 '{}'", input_path.display()))?;

    let topics = read_topics(&db)?;
    let message_definitions = read_message_definitions(&db)?;
    let schema_ids_by_type = validate_schema_coverage(&topics, &message_definitions)?;

    let mut writer = write_options
        .create(output)
        .context("failed to create MCAP writer")?;

    for definition in &message_definitions {
        let schema_id = schema_id(definition.id)?;
        writer
            .add_schema_with_id(
                schema_id,
                &definition.topic_type,
                &definition.encoding,
                definition.encoded_message_definition.as_bytes(),
            )
            .with_context(|| {
                format!(
                    "failed to write ROS2 schema for type {}",
                    definition.topic_type
                )
            })?;
    }

    let mut channel_ids_by_topic_id = HashMap::new();
    for topic in &topics {
        let channel_id = channel_id(topic.id)?;
        let schema_id = *schema_ids_by_type
            .get(&topic.typ)
            .with_context(|| format!("missing schema for topic type {}", topic.typ))?;
        let mut metadata = BTreeMap::new();
        if let Some(qos) = &topic.offered_qos_profiles {
            metadata.insert("offered_qos_profiles".to_string(), qos.clone());
        }
        writer
            .add_channel_with_id(
                channel_id,
                schema_id,
                &topic.name,
                &topic.serialization_format,
                &metadata,
            )
            .with_context(|| format!("failed to write ROS2 channel for topic {}", topic.name))?;
        channel_ids_by_topic_id.insert(topic.id, channel_id);
    }

    let mut sequences = BTreeMap::<u16, u32>::new();
    write_messages(&db, &mut writer, &channel_ids_by_topic_id, &mut sequences)?;

    writer.finish().context("failed to finalize MCAP writer")?;
    Ok(())
}

fn validate_schema_coverage(
    topics: &[TopicRecord],
    message_definitions: &[MessageDefinitionRecord],
) -> Result<HashMap<String, u16>> {
    let mut schema_ids_by_type = HashMap::new();
    for definition in message_definitions {
        ensure!(
            definition.encoding == "ros2msg" || definition.encoding == "ros2idl",
            "unsupported ROS2 message definition encoding '{}' for type {}",
            definition.encoding,
            definition.topic_type
        );
        let id = schema_id(definition.id)?;
        if schema_ids_by_type
            .insert(definition.topic_type.clone(), id)
            .is_some()
        {
            bail!(
                "ROS2 db3 contains duplicate embedded message definitions for type {}",
                definition.topic_type
            );
        }
    }

    for topic in topics {
        ensure!(
            topic.serialization_format == "cdr",
            "unsupported ROS2 serialization format '{}' for topic {} (expected cdr)",
            topic.serialization_format,
            topic.name
        );
        if !schema_ids_by_type.contains_key(&topic.typ) {
            bail!(
                "ROS2 db3 topic '{}' has type '{}' but the bag does not contain an embedded message definition for that type. Use `ros2 bag convert` from a sourced ROS 2 workspace with the rosbag2_storage_mcap plugin instead.",
                topic.name,
                topic.typ
            );
        }
    }

    Ok(schema_ids_by_type)
}

fn read_topics(db: &Connection) -> Result<Vec<TopicRecord>> {
    let has_qos_profiles = column_exists(db, "topics", "offered_qos_profiles")?;
    let query = if has_qos_profiles {
        "SELECT id, name, type, serialization_format, offered_qos_profiles FROM topics ORDER BY id"
    } else {
        "SELECT id, name, type, serialization_format, NULL FROM topics ORDER BY id"
    };

    let mut stmt = db.prepare(query).context("failed to query ROS2 topics")?;
    let topics = stmt
        .query_map([], |row| {
            Ok(TopicRecord {
                id: row.get(0)?,
                name: row.get(1)?,
                typ: row.get(2)?,
                serialization_format: row.get(3)?,
                offered_qos_profiles: row.get(4)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()
        .context("failed to read ROS2 topics")?;
    Ok(topics)
}

fn read_message_definitions(db: &Connection) -> Result<Vec<MessageDefinitionRecord>> {
    if !table_exists(db, "message_definitions")? {
        bail!(EMBEDDED_SCHEMA_ERROR);
    }

    let mut stmt = db
        .prepare(
            "SELECT id, topic_type, encoding, encoded_message_definition \
             FROM message_definitions ORDER BY id",
        )
        .context("failed to query ROS2 message definitions")?;
    let definitions = stmt
        .query_map([], |row| {
            Ok(MessageDefinitionRecord {
                id: row.get(0)?,
                topic_type: row.get(1)?,
                encoding: row.get(2)?,
                encoded_message_definition: row.get(3)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()
        .context("failed to read ROS2 message definitions")?;

    if definitions.is_empty() {
        bail!(EMBEDDED_SCHEMA_ERROR);
    }
    Ok(definitions)
}

fn write_messages<W: Write + Seek>(
    db: &Connection,
    writer: &mut mcap::Writer<W>,
    channel_ids_by_topic_id: &HashMap<i64, u16>,
    sequences: &mut BTreeMap<u16, u32>,
) -> Result<()> {
    let mut stmt = db
        .prepare(
            "SELECT topic_id, timestamp, data \
             FROM messages ORDER BY timestamp ASC, id ASC",
        )
        .context("failed to query ROS2 messages")?;
    let mut rows = stmt.query([]).context("failed to read ROS2 messages")?;

    while let Some(row) = rows.next().context("failed to read ROS2 message row")? {
        let topic_id: i64 = row.get(0).context("failed to read ROS2 message topic id")?;
        let timestamp = timestamp(
            row.get(1)
                .context("failed to read ROS2 message timestamp")?,
        )?;
        let data: Vec<u8> = row.get(2).context("failed to read ROS2 message data")?;
        let channel_id = *channel_ids_by_topic_id
            .get(&topic_id)
            .with_context(|| format!("message references unknown topic id {topic_id}"))?;
        let sequence = sequences.entry(channel_id).or_insert(0);

        writer
            .write_to_known_channel(
                &mcap::records::MessageHeader {
                    channel_id,
                    sequence: *sequence,
                    log_time: timestamp,
                    publish_time: timestamp,
                },
                &data,
            )
            .context("failed to write converted ROS2 message")?;
        *sequence = sequence.wrapping_add(1);
    }

    Ok(())
}

fn table_exists(db: &Connection, table: &str) -> Result<bool> {
    let exists: bool = db
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?1)",
            [table],
            |row| row.get(0),
        )
        .with_context(|| format!("failed to check whether table '{table}' exists"))?;
    Ok(exists)
}

fn column_exists(db: &Connection, table: &str, column: &str) -> Result<bool> {
    let query = format!("SELECT COUNT(*) FROM pragma_table_info('{table}') WHERE name = ?1");
    let count: i64 = db
        .query_row(&query, [column], |row| row.get(0))
        .with_context(|| format!("failed to inspect columns for table '{table}'"))?;
    Ok(count > 0)
}

fn schema_id(id: i64) -> Result<u16> {
    ensure!(id > 0, "invalid ROS2 message definition id {id}");
    u16::try_from(id).with_context(|| format!("ROS2 message definition id {id} exceeds u16::MAX"))
}

fn channel_id(id: i64) -> Result<u16> {
    ensure!(id >= 0, "invalid ROS2 topic id {id}");
    u16::try_from(id).with_context(|| format!("ROS2 topic id {id} exceeds u16::MAX"))
}

fn timestamp(timestamp: i64) -> Result<u64> {
    ensure!(
        timestamp >= 0,
        "invalid negative ROS2 timestamp {timestamp}"
    );
    Ok(timestamp as u64)
}
