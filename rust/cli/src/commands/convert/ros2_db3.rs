use std::collections::{BTreeMap, HashMap};
use std::fs::File;
use std::io::BufWriter;
use std::io::{Read, Seek, Write};
use std::path::Path;

use anyhow::{bail, ensure, Context, Result};
use rusqlite::{params, Connection, OpenFlags};

const EMBEDDED_SCHEMA_ERROR: &str = "This ROS 2 SQLite bag does not contain embedded message definitions, so the MCAP CLI cannot convert it safely. This usually means it was recorded with an older version of rosbag2 before embedded schemas were available. Use `ros2 bag convert` from a sourced ROS 2 workspace with the rosbag2_storage_mcap plugin installed instead.";

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
    definitions_by_type: HashMap<String, MessageDefinitionRecord>,
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

fn open_db(input_path: &Path) -> Result<Connection> {
    validate_sqlite_magic(input_path)?;
    Connection::open_with_flags(
        input_path,
        OpenFlags::SQLITE_OPEN_READ_ONLY
            | OpenFlags::SQLITE_OPEN_NO_MUTEX
            | OpenFlags::SQLITE_OPEN_URI,
    )
    .with_context(|| format!("failed to open ROS 2 db3 '{}'", input_path.display()))
}

fn validate_sqlite_magic(input_path: &Path) -> Result<()> {
    const SQLITE_MAGIC: &[u8] = b"SQLite format 3\0";

    let mut input = File::open(input_path)
        .with_context(|| format!("failed to open input '{}'", input_path.display()))?;
    let mut magic = [0u8; SQLITE_MAGIC.len()];
    let bytes_read = input.read(&mut magic).with_context(|| {
        format!(
            "failed to read SQLite magic from '{}'",
            input_path.display()
        )
    })?;
    ensure!(
        bytes_read == SQLITE_MAGIC.len() && magic == SQLITE_MAGIC,
        "invalid ROS 2 db3 magic (expected SQLite format 3)"
    );
    Ok(())
}

fn read_conversion_plan(db: &Connection) -> Result<ConversionPlan> {
    let topics = read_topics(db)?;
    ensure_table_exists(db, "messages")?;
    let message_definitions = if topics.is_empty() {
        Vec::new()
    } else {
        read_message_definitions(db)?
    };
    let definitions_by_type = validate_schema_coverage(&topics, &message_definitions)?;

    Ok(ConversionPlan {
        topics,
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
            let definition = plan
                .definitions_by_type
                .get(&topic.typ)
                .with_context(|| format!("missing schema for topic type {}", topic.typ))?;
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
) -> Result<HashMap<String, MessageDefinitionRecord>> {
    let mut definitions_by_type = HashMap::new();
    for definition in message_definitions {
        if definitions_by_type
            .insert(definition.topic_type.clone(), definition.clone())
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
        let definition = definitions_by_type.get(&topic.typ).ok_or_else(|| {
            anyhow::anyhow!(
                "ROS 2 db3 topic '{}' has type '{}' but the bag's embedded message definitions do not include that type. Use `ros2 bag convert` from a sourced ROS 2 workspace with the rosbag2_storage_mcap plugin instead.",
                topic.name,
                topic.typ
            )
        })?;
        ensure!(
            definition.encoding == "ros2msg" || definition.encoding == "ros2idl",
            "unsupported ROS 2 message definition encoding '{}' for topic '{}' with type {}",
            definition.encoding,
            topic.name,
            topic.typ
        );
    }

    Ok(definitions_by_type)
}

fn read_topics(db: &Connection) -> Result<Vec<TopicRecord>> {
    ensure_table_exists(db, "topics")?;
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
    Ok(topics)
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
        let channel_id = channel_ids_by_topic_id
            .get(&topic_id)
            .copied()
            .with_context(|| format!("message references unknown ROS 2 topic id {topic_id}"))?;
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

fn ensure_table_exists(db: &Connection, table: &str) -> Result<()> {
    if !table_exists(db, table)? {
        bail!("input is a SQLite database, but it does not look like a ROS 2 db3 bag: missing '{table}' table");
    }
    Ok(())
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
    use std::fs;
    use std::path::{Path, PathBuf};

    use super::convert_ros2_db3_file;

    const IRON_TALKER_DB3: &str = "testdata/db3/talker-iron.db3";
    const HUMBLE_TALKER_DB3: &str = "testdata/db3/talker-humble.db3";

    struct TempOutput {
        path: PathBuf,
    }

    impl TempOutput {
        fn new(name: &str) -> Self {
            let path = std::env::temp_dir().join(format!(
                "mcap-rust-ros2-db3-{name}-{}.mcap",
                std::process::id()
            ));
            let _ = fs::remove_file(&path);
            Self { path }
        }
    }

    impl Drop for TempOutput {
        fn drop(&mut self) {
            let _ = fs::remove_file(&self.path);
        }
    }

    fn fixture_path(relative_from_repo_root: &str) -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .join(relative_from_repo_root)
    }

    fn temp_input(name: &str, bytes: &[u8]) -> PathBuf {
        let path = std::env::temp_dir().join(format!(
            "mcap-rust-ros2-db3-input-{}-{name}",
            std::process::id()
        ));
        fs::write(&path, bytes).expect("write temp input");
        path
    }

    fn temp_path(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("mcap-rust-ros2-db3-{}-{name}", std::process::id()))
    }

    fn write_options() -> mcap::WriteOptions {
        mcap::WriteOptions::new()
            .profile("ros2")
            .compression(None)
            .chunk_size(Some(1024))
    }

    fn convert_fixture(path: &Path) -> Vec<u8> {
        let output = TempOutput::new("converted");
        convert_ros2_db3_file(path, &output.path, write_options()).expect("convert ROS 2 db3");
        fs::read(&output.path).expect("read converted MCAP")
    }

    #[test]
    fn converts_iron_talker_db3_with_embedded_schemas() {
        let bytes = convert_fixture(&fixture_path(IRON_TALKER_DB3));
        let summary = mcap::Summary::read(&bytes)
            .expect("summary read")
            .expect("summary present");
        let stats = summary.stats.expect("statistics");

        assert_eq!(stats.message_count, 20);
        assert_eq!(summary.channels.len(), 3);
        assert_eq!(summary.schemas.len(), 3);
        assert!(summary
            .schemas
            .values()
            .all(|schema| schema.encoding == "ros2msg"));
        let topic_channel = summary
            .channels
            .values()
            .find(|channel| channel.topic == "/topic")
            .expect("topic channel");
        assert_eq!(topic_channel.message_encoding, "cdr");
        assert!(topic_channel.metadata.contains_key("offered_qos_profiles"));
    }

    #[test]
    fn rejects_humble_talker_db3_without_embedded_schemas() {
        let output = TempOutput::new("humble");

        let err = convert_ros2_db3_file(
            &fixture_path(HUMBLE_TALKER_DB3),
            &output.path,
            write_options(),
        )
        .expect_err("humble db3 should fail");

        assert!(err
            .to_string()
            .contains("does not contain embedded message definitions"));
        assert!(err.to_string().contains("ros2 bag convert"));
    }

    #[test]
    fn rejects_invalid_sqlite_magic() {
        let input = temp_input("invalid.db3", b"not sqlite");
        let output = TempOutput::new("invalid-magic");

        let err = convert_ros2_db3_file(&input, &output.path, write_options())
            .expect_err("invalid sqlite magic should fail");

        assert!(err.to_string().contains("invalid ROS 2 db3 magic"));
        fs::remove_file(input).expect("remove temp input");
    }

    #[test]
    fn rejects_humble_talker_db3_without_truncating_existing_output() {
        let output_path = temp_input("existing-output.mcap", b"keep me");

        let err = convert_ros2_db3_file(
            &fixture_path(HUMBLE_TALKER_DB3),
            &output_path,
            write_options(),
        )
        .expect_err("humble db3 should fail");

        assert!(err
            .to_string()
            .contains("does not contain embedded message definitions"));
        assert_eq!(
            fs::read(&output_path).expect("read existing output"),
            b"keep me"
        );
        fs::remove_file(output_path).expect("remove temp output");
    }

    #[test]
    fn rejects_sqlite_database_without_rosbag2_topics_table() {
        let sqlite_path = temp_path("not-a-rosbag2.db3");
        let _ = fs::remove_file(&sqlite_path);
        {
            let db = rusqlite::Connection::open(&sqlite_path).expect("create sqlite db");
            db.execute("CREATE TABLE unrelated(id INTEGER PRIMARY KEY)", [])
                .expect("create unrelated table");
        }
        let output = TempOutput::new("not-a-rosbag2");

        let err = convert_ros2_db3_file(&sqlite_path, &output.path, write_options())
            .expect_err("non-rosbag2 sqlite should fail");

        assert!(err
            .to_string()
            .contains("does not look like a ROS 2 db3 bag"));
        fs::remove_file(sqlite_path).expect("remove temp sqlite");
    }

    #[test]
    fn rejects_sqlite_database_without_rosbag2_messages_table() {
        let sqlite_path = temp_path("not-a-rosbag2-no-messages.db3");
        let _ = fs::remove_file(&sqlite_path);
        {
            let db = rusqlite::Connection::open(&sqlite_path).expect("create sqlite db");
            db.execute(
                "CREATE TABLE topics(
                    id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    type TEXT NOT NULL,
                    serialization_format TEXT NOT NULL
                )",
                [],
            )
            .expect("create topics table");
        }
        let output = TempOutput::new("not-a-rosbag2-no-messages");

        let err = convert_ros2_db3_file(&sqlite_path, &output.path, write_options())
            .expect_err("sqlite without messages should fail");

        assert!(err.to_string().contains("missing 'messages' table"));
        fs::remove_file(sqlite_path).expect("remove temp sqlite");
    }

    #[test]
    fn converts_non_msg_topics_when_embedded_schema_exists() {
        let sqlite_path = temp_path("service-event.db3");
        let _ = fs::remove_file(&sqlite_path);
        {
            let db = rusqlite::Connection::open(&sqlite_path).expect("create sqlite db");
            db.execute_batch(
                "CREATE TABLE topics(
                    id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    type TEXT NOT NULL,
                    serialization_format TEXT NOT NULL
                );
                CREATE TABLE message_definitions(
                    id INTEGER PRIMARY KEY,
                    topic_type TEXT NOT NULL,
                    encoding TEXT NOT NULL,
                    encoded_message_definition TEXT NOT NULL,
                    type_description_hash TEXT NOT NULL
                );
                CREATE TABLE messages(
                    id INTEGER PRIMARY KEY,
                    topic_id INTEGER NOT NULL,
                    timestamp INTEGER NOT NULL,
                    data BLOB NOT NULL
                );",
            )
            .expect("create db3 tables");
            db.execute(
                "INSERT INTO topics(id, name, type, serialization_format) VALUES(1, '/add_two_ints/_service_event', 'example_interfaces/srv/AddTwoInts_Event', 'cdr')",
                [],
            )
            .expect("insert topic");
            db.execute(
                "INSERT INTO message_definitions(id, topic_type, encoding, encoded_message_definition, type_description_hash) VALUES(1, 'example_interfaces/srv/AddTwoInts_Event', 'ros2msg', 'int64 a\nint64 b\n', '')",
                [],
            )
            .expect("insert message definition");
            db.execute(
                "INSERT INTO messages(topic_id, timestamp, data) VALUES(1, 42, x'010203')",
                [],
            )
            .expect("insert message");
        }

        let bytes = convert_fixture(&sqlite_path);
        let summary = mcap::Summary::read(&bytes)
            .expect("summary read")
            .expect("summary present");
        assert!(summary
            .channels
            .values()
            .any(|channel| channel.topic == "/add_two_ints/_service_event"));
        assert!(summary
            .schemas
            .values()
            .any(|schema| schema.name == "example_interfaces/srv/AddTwoInts_Event"));
        fs::remove_file(sqlite_path).expect("remove temp sqlite");
    }

    #[test]
    fn ignores_unused_definitions_with_unknown_encoding() {
        let sqlite_path = temp_path("unused-future-definition.db3");
        let _ = fs::remove_file(&sqlite_path);
        {
            let db = rusqlite::Connection::open(&sqlite_path).expect("create sqlite db");
            db.execute_batch(
                "CREATE TABLE topics(
                    id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    type TEXT NOT NULL,
                    serialization_format TEXT NOT NULL
                );
                CREATE TABLE message_definitions(
                    id INTEGER PRIMARY KEY,
                    topic_type TEXT NOT NULL,
                    encoding TEXT NOT NULL,
                    encoded_message_definition TEXT NOT NULL,
                    type_description_hash TEXT NOT NULL
                );
                CREATE TABLE messages(
                    id INTEGER PRIMARY KEY,
                    topic_id INTEGER NOT NULL,
                    timestamp INTEGER NOT NULL,
                    data BLOB NOT NULL
                );",
            )
            .expect("create db3 tables");
            db.execute(
                "INSERT INTO topics(id, name, type, serialization_format) VALUES(1, '/topic', 'std_msgs/msg/String', 'cdr')",
                [],
            )
            .expect("insert topic");
            db.execute(
                "INSERT INTO message_definitions(id, topic_type, encoding, encoded_message_definition, type_description_hash) VALUES(1, 'std_msgs/msg/String', 'ros2msg', 'string data', '')",
                [],
            )
            .expect("insert used message definition");
            db.execute(
                "INSERT INTO message_definitions(id, topic_type, encoding, encoded_message_definition, type_description_hash) VALUES(2, 'future_msgs/msg/Unused', 'ros2future', 'uint8 data', '')",
                [],
            )
            .expect("insert unused message definition");
            db.execute(
                "INSERT INTO messages(topic_id, timestamp, data) VALUES(1, 42, x'010203')",
                [],
            )
            .expect("insert message");
        }

        let bytes = convert_fixture(&sqlite_path);
        let summary = mcap::Summary::read(&bytes)
            .expect("summary read")
            .expect("summary present");
        assert_eq!(summary.schemas.len(), 1);
        assert!(summary
            .schemas
            .values()
            .any(|schema| schema.name == "std_msgs/msg/String"));
        fs::remove_file(sqlite_path).expect("remove temp sqlite");
    }
}
