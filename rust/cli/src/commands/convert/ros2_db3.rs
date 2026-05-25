use std::collections::{BTreeMap, HashMap};
use std::fs::File;
use std::io::BufWriter;
use std::io::{Seek, Write};
use std::path::Path;

use anyhow::{bail, ensure, Context, Result};
use rusqlite::{params, Connection, OpenFlags};

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
    topic_type: String,
    encoding: String,
    encoded_message_definition: String,
}

struct ConversionPlan {
    topics: Vec<TopicRecord>,
    message_definitions: Vec<MessageDefinitionRecord>,
    definitions_by_type: HashMap<String, usize>,
}

pub fn convert_ros2_db3_file(
    input_path: &Path,
    output_path: &Path,
    write_options: mcap::WriteOptions,
) -> Result<()> {
    let db = open_db(input_path)?;
    let plan = read_conversion_plan(&db)?;

    let output = File::create(output_path)
        .with_context(|| format!("failed to open output '{}'", output_path.display()))?;
    convert_open_ros2_db3(BufWriter::new(output), &db, plan, write_options)
}

#[cfg(test)]
pub fn convert_ros2_db3<W: Write + Seek>(
    output: W,
    input_path: &Path,
    write_options: mcap::WriteOptions,
) -> Result<()> {
    let db = open_db(input_path)?;
    let plan = read_conversion_plan(&db)?;
    convert_open_ros2_db3(output, &db, plan, write_options)
}

fn open_db(input_path: &Path) -> Result<Connection> {
    Connection::open_with_flags(input_path, OpenFlags::SQLITE_OPEN_READ_ONLY)
        .with_context(|| format!("failed to open ROS 2 db3 '{}'", input_path.display()))
}

fn read_conversion_plan(db: &Connection) -> Result<ConversionPlan> {
    let topics = read_topics(db)?;
    let message_definitions = if topics.is_empty() {
        Vec::new()
    } else {
        read_message_definitions(db)?
    };
    let definitions_by_type = validate_schema_coverage(&topics, &message_definitions)?;

    Ok(ConversionPlan {
        topics,
        message_definitions,
        definitions_by_type,
    })
}

fn convert_open_ros2_db3<W: Write + Seek>(
    output: W,
    db: &Connection,
    plan: ConversionPlan,
    write_options: mcap::WriteOptions,
) -> Result<()> {
    let mut writer = write_options
        .create(output)
        .context("failed to create MCAP writer")?;

    let mut channel_ids_by_topic_id = HashMap::new();
    let mut schema_ids_by_type = HashMap::new();
    for topic in &plan.topics {
        let schema_id = if let Some(id) = schema_ids_by_type.get(&topic.typ) {
            *id
        } else {
            let definition =
                &plan.message_definitions[*plan
                    .definitions_by_type
                    .get(&topic.typ)
                    .with_context(|| format!("missing schema for topic type {}", topic.typ))?];
            let id = writer
                .add_schema(
                    &definition.topic_type,
                    &definition.encoding,
                    definition.encoded_message_definition.as_bytes(),
                )
                .with_context(|| {
                    format!(
                        "failed to write ROS 2 schema for type {}",
                        definition.topic_type
                    )
                })?;
            schema_ids_by_type.insert(topic.typ.clone(), id);
            id
        };
        let mut metadata = BTreeMap::new();
        if let Some(qos) = &topic.offered_qos_profiles {
            metadata.insert("offered_qos_profiles".to_string(), qos.clone());
        }
        let channel_id = writer
            .add_channel(
                schema_id,
                &topic.name,
                &topic.serialization_format,
                &metadata,
            )
            .with_context(|| format!("failed to write ROS 2 channel for topic {}", topic.name))?;
        channel_ids_by_topic_id.insert(topic.id, channel_id);
    }

    let mut sequences = BTreeMap::<u16, u32>::new();
    write_messages(db, &mut writer, &channel_ids_by_topic_id, &mut sequences)?;

    writer.finish().context("failed to finalize MCAP writer")?;
    Ok(())
}

fn validate_schema_coverage(
    topics: &[TopicRecord],
    message_definitions: &[MessageDefinitionRecord],
) -> Result<HashMap<String, usize>> {
    let mut definitions_by_type = HashMap::new();
    for (index, definition) in message_definitions.iter().enumerate() {
        ensure!(
            definition.encoding == "ros2msg" || definition.encoding == "ros2idl",
            "unsupported ROS 2 message definition encoding '{}' for type {}",
            definition.encoding,
            definition.topic_type
        );
        if definitions_by_type
            .insert(definition.topic_type.clone(), index)
            .is_some()
        {
            bail!(
                "ROS 2 db3 contains duplicate embedded message definitions for type {}",
                definition.topic_type
            );
        }
    }

    for topic in topics {
        ensure!(
            topic.serialization_format == "cdr",
            "unsupported ROS 2 serialization format '{}' for topic {} (expected cdr)",
            topic.serialization_format,
            topic.name
        );
        if !definitions_by_type.contains_key(&topic.typ) {
            bail!(
                "ROS 2 db3 topic '{}' has type '{}' but the bag does not contain an embedded message definition for that type. Use `ros2 bag convert` from a sourced ROS 2 workspace with the rosbag2_storage_mcap plugin instead.",
                topic.name,
                topic.typ
            );
        }
    }

    Ok(definitions_by_type)
}

fn read_topics(db: &Connection) -> Result<Vec<TopicRecord>> {
    if !table_exists(db, "topics")? {
        bail!("input is a SQLite database, but it does not look like a ROS 2 db3 bag: missing 'topics' table");
    }
    let has_qos_profiles = column_exists(db, "topics", "offered_qos_profiles")?;
    let query = if has_qos_profiles {
        "SELECT id, name, type, serialization_format, offered_qos_profiles FROM topics ORDER BY id"
    } else {
        "SELECT id, name, type, serialization_format, NULL FROM topics ORDER BY id"
    };

    let mut stmt = db.prepare(query).context("failed to query ROS 2 topics")?;
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
        .context("failed to read ROS 2 topics")?;
    Ok(topics
        .into_iter()
        .filter(|topic| is_message_topic_type(&topic.typ))
        .collect())
}

fn read_message_definitions(db: &Connection) -> Result<Vec<MessageDefinitionRecord>> {
    if !table_exists(db, "message_definitions")? {
        bail!(EMBEDDED_SCHEMA_ERROR);
    }

    let mut stmt = db
        .prepare(
            "SELECT topic_type, encoding, encoded_message_definition \
             FROM message_definitions ORDER BY id",
        )
        .context("failed to query ROS 2 message definitions")?;
    let definitions = stmt
        .query_map([], |row| {
            Ok(MessageDefinitionRecord {
                topic_type: row.get(0)?,
                encoding: row.get(1)?,
                encoded_message_definition: row.get(2)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()
        .context("failed to read ROS 2 message definitions")?;

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
    if !table_exists(db, "messages")? {
        bail!("input is a SQLite database, but it does not look like a ROS 2 db3 bag: missing 'messages' table");
    }

    let mut stmt = db
        .prepare(
            "SELECT topic_id, timestamp, data \
             FROM messages ORDER BY timestamp ASC, id ASC",
        )
        .context("failed to query ROS 2 messages")?;
    let mut rows = stmt.query([]).context("failed to read ROS 2 messages")?;

    while let Some(row) = rows.next().context("failed to read ROS 2 message row")? {
        let topic_id: i64 = row
            .get(0)
            .context("failed to read ROS 2 message topic id")?;
        let timestamp = timestamp(
            row.get(1)
                .context("failed to read ROS 2 message timestamp")?,
        )?;
        let data: Vec<u8> = row.get(2).context("failed to read ROS 2 message data")?;
        let Some(channel_id) = channel_ids_by_topic_id.get(&topic_id).copied() else {
            continue;
        };
        let sequence = sequences.entry(channel_id).or_insert(0);
        ensure!(
            *sequence < u32::MAX,
            "too many messages on ROS 2 channel {channel_id} to assign monotonic u32 sequence numbers"
        );

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
            .context("failed to write converted ROS 2 message")?;
        *sequence += 1;
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
    let count: i64 = db
        .query_row(
            "SELECT COUNT(*) FROM pragma_table_info(?1) WHERE name = ?2",
            params![table, column],
            |row| row.get(0),
        )
        .with_context(|| format!("failed to inspect columns for table '{table}'"))?;
    Ok(count > 0)
}

fn is_message_topic_type(typ: &str) -> bool {
    let mut parts = typ.split('/');
    matches!(
        (parts.next(), parts.next(), parts.next()),
        (Some(package), Some("msg"), Some(_)) if !package.is_empty()
    )
}

fn timestamp(timestamp: i64) -> Result<u64> {
    ensure!(
        timestamp >= 0,
        "invalid negative ROS 2 timestamp {timestamp}"
    );
    Ok(timestamp as u64)
}

#[cfg(test)]
mod tests {
    use super::is_message_topic_type;

    #[test]
    fn message_topic_type_filter_matches_go_converter_scope() {
        assert!(is_message_topic_type("std_msgs/msg/String"));
        assert!(is_message_topic_type("rcl_interfaces/msg/ParameterEvent"));
        assert!(!is_message_topic_type(
            "example_interfaces/srv/AddTwoInts_Event"
        ));
        assert!(!is_message_topic_type("action_msgs/action/GoalStatusArray"));
        assert!(!is_message_topic_type("legacy/TypeName"));
    }
}
