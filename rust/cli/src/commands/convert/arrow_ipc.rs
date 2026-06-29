use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::Arc;

use anyhow::{anyhow, bail, ensure, Context, Result};
use arrow::array::{
    Array, Date32Array, Date64Array, Int16Array, Int32Array, Int64Array, Int8Array, RecordBatch,
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::error::ArrowError;
use arrow::ipc::reader::{FileReader, StreamReader};
use arrow::ipc::writer::{
    write_message, CompressionContext, DictionaryTracker, IpcDataGenerator, IpcWriteOptions,
};

use crate::cli::{ConvertCommand, TimestampUnit};

/// Apache Arrow IPC file magic. An IPC *file* starts with these bytes; an IPC
/// *stream* does not, which is how we tell the two framings apart.
const ARROW_FILE_MAGIC: &[u8] = b"ARROW1";

const MESSAGE_ENCODING: &str = "arrow";
const SCHEMA_ENCODING: &str = "arrow";

/// Options that control how an Arrow input is mapped onto MCAP records.
#[derive(Debug, Clone)]
pub struct ArrowConvertOptions {
    pub topic: Option<String>,
    pub schema_name: Option<String>,
    pub log_time_field: Option<String>,
    pub publish_time_field: Option<String>,
    pub timestamp_unit: TimestampUnit,
    pub rows_per_message: u64,
    /// Default topic when `topic` is unset, derived from the original input
    /// name (not the materialized temp file used for remote inputs).
    default_topic: String,
}

impl ArrowConvertOptions {
    pub fn from_command(args: &ConvertCommand) -> Self {
        Self {
            topic: args.topic.clone(),
            schema_name: args.schema_name.clone(),
            log_time_field: args.log_time_field.clone(),
            publish_time_field: args.publish_time_field.clone(),
            timestamp_unit: args.timestamp_unit,
            rows_per_message: args.rows_per_message,
            default_topic: default_topic(&args.input),
        }
    }
}

pub fn convert_arrow_file(
    input_path: &Path,
    output_path: &Path,
    write_options: mcap::WriteOptions,
    opts: &ArrowConvertOptions,
) -> Result<()> {
    ensure!(
        opts.rows_per_message >= 1,
        "--rows-per-message must be at least 1"
    );

    let mut input = File::open(input_path)
        .with_context(|| format!("failed to open input '{}'", input_path.display()))?;
    let is_file_format = detect_arrow_file_format(&mut input)
        .with_context(|| format!("failed to read Arrow magic from '{}'", input_path.display()))?;
    input
        .seek(SeekFrom::Start(0))
        .context("failed to rewind Arrow input")?;

    // Open the reader (which validates the Arrow framing and parses the schema)
    // before touching the output path, so malformed input never clobbers an
    // existing output file.
    if is_file_format {
        let reader = FileReader::try_new(BufReader::new(input), None)
            .context("failed to open Arrow IPC file")?;
        let schema = reader.schema();
        run_conversion(schema, reader, output_path, write_options, opts)
    } else {
        let reader = StreamReader::try_new(BufReader::new(input), None)
            .context("failed to open Arrow IPC stream")?;
        let schema = reader.schema();
        run_conversion(schema, reader, output_path, write_options, opts)
    }
}

fn run_conversion<I>(
    source_schema: SchemaRef,
    batches: I,
    output_path: &Path,
    write_options: mcap::WriteOptions,
    opts: &ArrowConvertOptions,
) -> Result<()>
where
    I: Iterator<Item = std::result::Result<RecordBatch, ArrowError>>,
{
    // Resolve everything that can fail from the schema alone before creating the
    // output file.
    let hydrated_schema = Arc::new(hydrate_schema(&source_schema)?);
    let plan = TimePlan::resolve(&hydrated_schema, opts)?;
    let schema_bytes = encode_schema(&hydrated_schema)?;
    let topic = opts
        .topic
        .clone()
        .unwrap_or_else(|| opts.default_topic.clone());
    let schema_name = opts.schema_name.clone().unwrap_or_else(|| topic.clone());

    let output = File::create(output_path)
        .with_context(|| format!("failed to open output '{}'", output_path.display()))?;
    let mut writer = write_options
        .create(BufWriter::new(output))
        .context("failed to create MCAP writer")?;

    let outcome = write_messages(
        &mut writer,
        &hydrated_schema,
        &schema_bytes,
        &topic,
        &schema_name,
        &plan,
        batches,
    )
    .and_then(|()| writer.finish().context("failed to finalize MCAP writer"));
    drop(writer);

    if let Err(err) = outcome {
        // A failure (e.g. a bad timestamp deep in the file) leaves a partial,
        // never-finalized MCAP; remove it rather than leaving a truncated file.
        let _ = std::fs::remove_file(output_path);
        return Err(err);
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn write_messages<W, I>(
    writer: &mut mcap::Writer<W>,
    hydrated_schema: &SchemaRef,
    schema_bytes: &[u8],
    topic: &str,
    schema_name: &str,
    plan: &TimePlan,
    batches: I,
) -> Result<()>
where
    W: Write + Seek,
    I: Iterator<Item = std::result::Result<RecordBatch, ArrowError>>,
{
    let schema_id = writer
        .add_schema(schema_name, SCHEMA_ENCODING, schema_bytes)
        .context("failed to write Arrow schema")?;
    let channel_id = writer
        .add_channel(schema_id, topic, MESSAGE_ENCODING, &BTreeMap::new())
        .context("failed to write Arrow channel")?;

    let rows_per_message = plan.rows_per_message;
    let mut sequence: u32 = 0;

    for batch in batches {
        let batch = batch.context("failed to read Arrow record batch")?;
        let batch = hydrate_batch(&batch, hydrated_schema)?;

        let num_rows = batch.num_rows();
        let mut start = 0;
        while start < num_rows {
            let len = rows_per_message.min(num_rows - start);
            // All rows in a batch share the message's log_time, so only the
            // group's first row contributes the message timestamps.
            let log_time = plan.log_time_at(&batch, start)?;
            let publish_time = plan.publish_time_at(&batch, start, log_time)?;
            let slice = batch.slice(start, len);
            let data = encode_record_batch(&slice)?;

            writer
                .write_to_known_channel(
                    &mcap::records::MessageHeader {
                        channel_id,
                        sequence,
                        log_time,
                        publish_time,
                    },
                    &data,
                )
                .context("failed to write converted Arrow message")?;

            ensure!(
                sequence < u32::MAX,
                "too many messages to assign monotonic u32 sequence numbers"
            );
            sequence += 1;
            start += len;
        }
    }

    Ok(())
}

/// Returns true if the input is an Arrow IPC *file* (begins with the `ARROW1`
/// magic), false if it should be read as an Arrow IPC *stream*.
fn detect_arrow_file_format(input: &mut File) -> Result<bool> {
    let mut magic = [0u8; ARROW_FILE_MAGIC.len()];
    let mut filled = 0;
    while filled < magic.len() {
        let read = input.read(&mut magic[filled..])?;
        if read == 0 {
            break;
        }
        filled += read;
    }
    Ok(&magic[..filled] == ARROW_FILE_MAGIC)
}

/// Where each message's log_time / publish_time comes from.
struct TimePlan {
    log_index: usize,
    publish: PublishSource,
    unit: TimestampUnit,
    rows_per_message: usize,
}

enum PublishSource {
    Column(usize),
    SameAsLog,
}

impl TimePlan {
    fn resolve(schema: &Schema, opts: &ArrowConvertOptions) -> Result<Self> {
        let log_index = resolve_log_index(schema, opts)?;
        validate_time_field(schema.field(log_index))?;

        let publish = if let Some(name) = &opts.publish_time_field {
            let index = index_of_field(schema, name)?;
            validate_time_field(schema.field(index))?;
            PublishSource::Column(index)
        } else if let Ok(index) = schema.index_of("publish_time") {
            // A field literally named `publish_time` is treated as an explicit
            // selection: commit to it and error if it is not a time field.
            validate_time_field(schema.field(index))?;
            PublishSource::Column(index)
        } else {
            PublishSource::SameAsLog
        };

        Ok(Self {
            log_index,
            publish,
            unit: opts.timestamp_unit,
            rows_per_message: opts.rows_per_message.max(1) as usize,
        })
    }

    fn log_time_at(&self, batch: &RecordBatch, row: usize) -> Result<u64> {
        let field = batch.schema_ref().field(self.log_index);
        value_to_nanos(batch.column(self.log_index), row, self.unit, field.name())
    }

    fn publish_time_at(&self, batch: &RecordBatch, row: usize, log_time: u64) -> Result<u64> {
        match self.publish {
            PublishSource::SameAsLog => Ok(log_time),
            PublishSource::Column(index) => {
                let field = batch.schema_ref().field(index);
                value_to_nanos(batch.column(index), row, self.unit, field.name())
            }
        }
    }
}

fn resolve_log_index(schema: &Schema, opts: &ArrowConvertOptions) -> Result<usize> {
    if let Some(name) = &opts.log_time_field {
        return index_of_field(schema, name);
    }
    if let Ok(index) = schema.index_of("log_time") {
        return Ok(index);
    }
    for (index, field) in schema.fields().iter().enumerate() {
        if is_temporal(field.data_type()) {
            return Ok(index);
        }
    }
    bail!(
        "could not determine a log_time field: the Arrow schema has no field named 'log_time' \
         and no timestamp/date field. Pass --log-time-field (and --timestamp-unit if it is an \
         integer column)."
    )
}

fn index_of_field(schema: &Schema, name: &str) -> Result<usize> {
    schema
        .index_of(name)
        .map_err(|_| anyhow!("field '{name}' was not found in the Arrow schema"))
}

fn validate_time_field(field: &Field) -> Result<()> {
    let dt = field.data_type();
    ensure!(
        is_temporal(dt) || is_integer(dt),
        "field '{}' has type {dt:?}, which cannot be interpreted as a timestamp (expected a \
         timestamp, date, or integer field)",
        field.name()
    );
    Ok(())
}

fn is_temporal(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::Timestamp(_, _) | DataType::Date32 | DataType::Date64
    )
}

fn is_integer(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
    )
}

fn default_topic(input_path: &Path) -> String {
    input_path
        .file_stem()
        .and_then(|stem| stem.to_str())
        .filter(|stem| !stem.is_empty())
        .unwrap_or("arrow")
        .to_string()
}

/// Convert a single row of an Arrow time column to an MCAP nanosecond timestamp.
fn value_to_nanos(
    array: &dyn Array,
    row: usize,
    unit: TimestampUnit,
    field_name: &str,
) -> Result<u64> {
    let integer_scale = unit.nanos_per_unit();

    macro_rules! scaled {
        ($ty:ty, $scale:expr) => {{
            let typed = array
                .as_any()
                .downcast_ref::<$ty>()
                .expect("array data type checked before downcast");
            ensure!(
                !typed.is_null(row),
                "field '{field_name}' has a null value at row {row}; cannot determine a \
                 message time"
            );
            checked_nanos(typed.value(row) as i128, $scale, field_name, row)?
        }};
    }

    let value = match array.data_type() {
        DataType::Timestamp(TimeUnit::Second, _) => scaled!(TimestampSecondArray, 1_000_000_000),
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            scaled!(TimestampMillisecondArray, 1_000_000)
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => scaled!(TimestampMicrosecondArray, 1_000),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => scaled!(TimestampNanosecondArray, 1),
        DataType::Date32 => scaled!(Date32Array, 86_400_000_000_000),
        DataType::Date64 => scaled!(Date64Array, 1_000_000),
        DataType::Int8 => scaled!(Int8Array, integer_scale),
        DataType::Int16 => scaled!(Int16Array, integer_scale),
        DataType::Int32 => scaled!(Int32Array, integer_scale),
        DataType::Int64 => scaled!(Int64Array, integer_scale),
        DataType::UInt8 => scaled!(UInt8Array, integer_scale),
        DataType::UInt16 => scaled!(UInt16Array, integer_scale),
        DataType::UInt32 => scaled!(UInt32Array, integer_scale),
        DataType::UInt64 => scaled!(UInt64Array, integer_scale),
        other => bail!(
            "field '{field_name}' has type {other:?}, which cannot be interpreted as a timestamp"
        ),
    };
    Ok(value)
}

fn checked_nanos(value: i128, scale: i128, field_name: &str, row: usize) -> Result<u64> {
    let nanos = value.checked_mul(scale).ok_or_else(|| {
        anyhow!("field '{field_name}' row {row} overflows when scaled to nanoseconds")
    })?;
    u64::try_from(nanos).map_err(|_| {
        anyhow!(
            "field '{field_name}' row {row} has an out-of-range time value (must be \
             non-negative and fit in u64 nanoseconds since the Unix epoch)"
        )
    })
}

/// Produce a schema with all dictionary-encoded fields replaced by their value
/// type. Errors if a dictionary is nested inside another type, which v1 does
/// not hydrate.
fn hydrate_schema(schema: &Schema) -> Result<Schema> {
    let mut fields = Vec::with_capacity(schema.fields().len());
    for field in schema.fields() {
        fields.push(hydrate_field(field)?);
    }
    Ok(Schema::new(fields).with_metadata(schema.metadata().clone()))
}

fn hydrate_field(field: &Field) -> Result<Field> {
    match field.data_type() {
        DataType::Dictionary(_, value_type) => {
            ensure!(
                !contains_dictionary(value_type),
                "field '{}' is a dictionary whose value type also contains a dictionary, which \
                 is not supported; hydrate it before converting",
                field.name()
            );
            Ok(Field::new(
                field.name(),
                value_type.as_ref().clone(),
                field.is_nullable(),
            )
            .with_metadata(field.metadata().clone()))
        }
        other => {
            ensure!(
                !contains_dictionary(other),
                "field '{}' contains a nested dictionary-encoded value, which is not supported; \
                 hydrate it before converting",
                field.name()
            );
            Ok(field.clone())
        }
    }
}

fn contains_dictionary(dt: &DataType) -> bool {
    match dt {
        DataType::Dictionary(_, _) => true,
        DataType::List(f)
        | DataType::LargeList(f)
        | DataType::FixedSizeList(f, _)
        | DataType::Map(f, _)
        | DataType::RunEndEncoded(_, f) => contains_dictionary(f.data_type()),
        DataType::Struct(fields) => fields.iter().any(|f| contains_dictionary(f.data_type())),
        DataType::Union(fields, _) => fields
            .iter()
            .any(|(_, f)| contains_dictionary(f.data_type())),
        _ => false,
    }
}

/// Cast any dictionary columns to their value type so the emitted RecordBatch
/// matches the hydrated schema.
fn hydrate_batch(batch: &RecordBatch, hydrated_schema: &SchemaRef) -> Result<RecordBatch> {
    let source_schema = batch.schema();
    let mut columns = Vec::with_capacity(batch.num_columns());
    for (index, column) in batch.columns().iter().enumerate() {
        if matches!(
            source_schema.field(index).data_type(),
            DataType::Dictionary(_, _)
        ) {
            let target = hydrated_schema.field(index).data_type();
            columns.push(
                cast(column, target)
                    .with_context(|| format!("failed to hydrate dictionary column {index}"))?,
            );
        } else {
            columns.push(column.clone());
        }
    }
    RecordBatch::try_new(hydrated_schema.clone(), columns)
        .context("failed to assemble hydrated Arrow record batch")
}

fn encode_schema(schema: &Schema) -> Result<Vec<u8>> {
    let generator = IpcDataGenerator {};
    let mut tracker = DictionaryTracker::new(false);
    let options = IpcWriteOptions::default();
    let encoded = generator.schema_to_bytes_with_dictionary_tracker(schema, &mut tracker, &options);
    encapsulate(encoded, &options).context("failed to encode Arrow schema message")
}

fn encode_record_batch(batch: &RecordBatch) -> Result<Vec<u8>> {
    let generator = IpcDataGenerator {};
    let mut tracker = DictionaryTracker::new(false);
    let options = IpcWriteOptions::default();
    let mut compression = CompressionContext::default();
    let (dictionaries, encoded) = generator
        .encode(batch, &mut tracker, &options, &mut compression)
        .context("failed to encode Arrow record batch")?;
    ensure!(
        dictionaries.is_empty(),
        "unexpected dictionary batch after hydration"
    );
    encapsulate(encoded, &options).context("failed to encode Arrow record batch message")
}

fn encapsulate(
    encoded: arrow::ipc::writer::EncodedData,
    options: &IpcWriteOptions,
) -> Result<Vec<u8>> {
    let mut buffer = Vec::new();
    write_message(&mut buffer, encoded, options)?;
    Ok(buffer)
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;
    use std::sync::Arc;

    use arrow::array::{
        Array, ArrayRef, Date32Array, Float64Array, Int32Array, Int64Array, StringArray,
        StringDictionaryBuilder, TimestampMicrosecondArray, TimestampMillisecondArray,
        TimestampSecondArray,
    };
    use arrow::datatypes::{DataType, Int32Type, SchemaRef};
    use arrow::ipc::reader::StreamReader;
    use arrow::ipc::writer::{FileWriter, StreamWriter};
    use arrow::record_batch::RecordBatch;

    use super::*;

    // Arrow IPC stream end-of-stream marker: continuation + zero metadata length.
    const EOS: [u8; 8] = [0xff, 0xff, 0xff, 0xff, 0, 0, 0, 0];

    fn default_opts() -> ArrowConvertOptions {
        ArrowConvertOptions {
            topic: None,
            schema_name: None,
            log_time_field: None,
            publish_time_field: None,
            timestamp_unit: TimestampUnit::Ns,
            rows_per_message: 1,
            default_topic: "fixture".to_string(),
        }
    }

    fn test_write_options() -> mcap::WriteOptions {
        mcap::WriteOptions::new()
            .profile("")
            .compression(None)
            .chunk_size(Some(1024))
    }

    fn batch(columns: Vec<(&str, ArrayRef)>) -> RecordBatch {
        RecordBatch::try_from_iter(columns).expect("build record batch")
    }

    fn ipc_file_bytes(batches: &[RecordBatch]) -> Vec<u8> {
        let schema = batches[0].schema();
        let mut buf = Vec::new();
        {
            let mut writer = FileWriter::try_new(&mut buf, schema.as_ref()).expect("file writer");
            for b in batches {
                writer.write(b).expect("write batch");
            }
            writer.finish().expect("finish file");
        }
        buf
    }

    fn ipc_stream_bytes(batches: &[RecordBatch]) -> Vec<u8> {
        let schema = batches[0].schema();
        let mut buf = Vec::new();
        {
            let mut writer =
                StreamWriter::try_new(&mut buf, schema.as_ref()).expect("stream writer");
            for b in batches {
                writer.write(b).expect("write batch");
            }
            writer.finish().expect("finish stream");
        }
        buf
    }

    fn convert_bytes(bytes: &[u8], ext: &str, opts: &ArrowConvertOptions) -> Result<Vec<u8>> {
        let dir = tempfile::tempdir().expect("tempdir");
        let input = dir.path().join(format!("fixture.{ext}"));
        std::fs::write(&input, bytes).expect("write fixture");
        let output = dir.path().join("out.mcap");
        convert_arrow_file(&input, &output, test_write_options(), opts)?;
        Ok(std::fs::read(&output).expect("read output"))
    }

    struct DecodedMessage {
        sequence: u32,
        log_time: u64,
        publish_time: u64,
        data: Vec<u8>,
    }

    fn read_messages(mcap_bytes: &[u8]) -> Vec<DecodedMessage> {
        let mut messages: Vec<DecodedMessage> = mcap::MessageStream::new(mcap_bytes)
            .expect("message stream")
            .map(|m| {
                let m = m.expect("message");
                DecodedMessage {
                    sequence: m.sequence,
                    log_time: m.log_time,
                    publish_time: m.publish_time,
                    data: m.data.into_owned(),
                }
            })
            .collect();
        messages.sort_by_key(|m| m.sequence);
        messages
    }

    fn schema_record_bytes(mcap_bytes: &[u8]) -> (String, Vec<u8>) {
        let summary = mcap::Summary::read(mcap_bytes)
            .expect("summary read")
            .expect("summary present");
        let schema = summary.schemas.values().next().expect("one schema");
        assert_eq!(schema.encoding, super::SCHEMA_ENCODING);
        (schema.name.clone(), schema.data.to_vec())
    }

    fn decode_schema(schema_msg: &[u8]) -> SchemaRef {
        let mut stream = Vec::new();
        stream.extend_from_slice(schema_msg);
        stream.extend_from_slice(&EOS);
        StreamReader::try_new(Cursor::new(stream), None)
            .expect("schema stream reader")
            .schema()
    }

    fn decode_batch(schema_msg: &[u8], batch_msg: &[u8]) -> RecordBatch {
        let mut stream = Vec::new();
        stream.extend_from_slice(schema_msg);
        stream.extend_from_slice(batch_msg);
        stream.extend_from_slice(&EOS);
        let mut reader =
            StreamReader::try_new(Cursor::new(stream), None).expect("batch stream reader");
        reader
            .next()
            .expect("at least one batch")
            .expect("decode batch")
    }

    fn assert_channel(mcap_bytes: &[u8], expected_topic: &str) {
        let summary = mcap::Summary::read(mcap_bytes)
            .expect("summary read")
            .expect("summary present");
        let channel = summary.channels.values().next().expect("one channel");
        assert_eq!(channel.message_encoding, super::MESSAGE_ENCODING);
        assert_eq!(channel.topic, expected_topic);
    }

    #[test]
    fn auto_detects_first_timestamp_and_scales_milliseconds() {
        let batch = batch(vec![
            (
                "value",
                Arc::new(Int64Array::from(vec![10, 20])) as ArrayRef,
            ),
            (
                "ts",
                Arc::new(TimestampMillisecondArray::from(vec![1000, 2000])) as ArrayRef,
            ),
        ]);
        let mcap_bytes = convert_bytes(&ipc_file_bytes(&[batch]), "arrow", &default_opts())
            .expect("convert should succeed");

        assert_channel(&mcap_bytes, "fixture");
        let messages = read_messages(&mcap_bytes);
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0].log_time, 1_000_000_000);
        assert_eq!(messages[0].publish_time, 1_000_000_000);
        assert_eq!(messages[1].log_time, 2_000_000_000);

        // The emitted message must decode against the Schema record as a one-row RecordBatch.
        let (_name, schema_msg) = schema_record_bytes(&mcap_bytes);
        let decoded = decode_batch(&schema_msg, &messages[0].data);
        assert_eq!(decoded.num_rows(), 1);
        let value = decoded
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(value.value(0), 10);
    }

    #[test]
    fn uses_named_log_time_and_publish_time_microseconds() {
        let batch = batch(vec![
            (
                "log_time",
                Arc::new(TimestampMicrosecondArray::from(vec![1, 2])) as ArrayRef,
            ),
            (
                "publish_time",
                Arc::new(TimestampMicrosecondArray::from(vec![5, 6])) as ArrayRef,
            ),
            (
                "x",
                Arc::new(Float64Array::from(vec![1.5, 2.5])) as ArrayRef,
            ),
        ]);
        let mcap_bytes = convert_bytes(&ipc_file_bytes(&[batch]), "arrow", &default_opts())
            .expect("convert should succeed");

        let messages = read_messages(&mcap_bytes);
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0].log_time, 1_000);
        assert_eq!(messages[0].publish_time, 5_000);
        assert_eq!(messages[1].log_time, 2_000);
        assert_eq!(messages[1].publish_time, 6_000);
    }

    #[test]
    fn integer_log_time_uses_timestamp_unit() {
        let batch = batch(vec![
            (
                "log_time",
                Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef,
            ),
            ("v", Arc::new(Int32Array::from(vec![7, 8, 9])) as ArrayRef),
        ]);
        let mut opts = default_opts();
        opts.timestamp_unit = TimestampUnit::Us;
        let mcap_bytes = convert_bytes(&ipc_file_bytes(&[batch]), "arrow", &opts)
            .expect("convert should succeed");

        let messages = read_messages(&mcap_bytes);
        assert_eq!(
            messages.iter().map(|m| m.log_time).collect::<Vec<_>>(),
            vec![1_000, 2_000, 3_000]
        );
    }

    #[test]
    fn auto_detects_date32_field() {
        let batch = batch(vec![(
            "d",
            Arc::new(Date32Array::from(vec![1, 2])) as ArrayRef,
        )]);
        let mcap_bytes = convert_bytes(&ipc_file_bytes(&[batch]), "arrow", &default_opts())
            .expect("convert should succeed");
        let messages = read_messages(&mcap_bytes);
        assert_eq!(messages[0].log_time, 86_400_000_000_000);
        assert_eq!(messages[1].log_time, 2 * 86_400_000_000_000);
    }

    #[test]
    fn hydrates_dictionary_columns() {
        let mut labels = StringDictionaryBuilder::<Int32Type>::new();
        labels.append_value("a");
        labels.append_value("b");
        let batch = batch(vec![
            (
                "ts",
                Arc::new(TimestampSecondArray::from(vec![1, 2])) as ArrayRef,
            ),
            ("label", Arc::new(labels.finish()) as ArrayRef),
        ]);
        let mcap_bytes = convert_bytes(&ipc_file_bytes(&[batch]), "arrow", &default_opts())
            .expect("convert should succeed");

        let (_name, schema_msg) = schema_record_bytes(&mcap_bytes);
        let decoded_schema = decode_schema(&schema_msg);
        assert_eq!(
            decoded_schema.field_with_name("label").unwrap().data_type(),
            &DataType::Utf8,
            "dictionary column should be hydrated to its value type"
        );

        let messages = read_messages(&mcap_bytes);
        let decoded = decode_batch(&schema_msg, &messages[0].data);
        let label = decoded
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("hydrated label column is Utf8");
        assert_eq!(label.value(0), "a");
    }

    #[test]
    fn rows_per_message_packs_rows() {
        let batch = batch(vec![(
            "ts",
            Arc::new(TimestampSecondArray::from(vec![1, 2, 3, 4])) as ArrayRef,
        )]);
        let mut opts = default_opts();
        opts.rows_per_message = 2;
        let mcap_bytes = convert_bytes(&ipc_file_bytes(&[batch]), "arrow", &opts)
            .expect("convert should succeed");

        let (_name, schema_msg) = schema_record_bytes(&mcap_bytes);
        let messages = read_messages(&mcap_bytes);
        assert_eq!(messages.len(), 2);
        // Each message carries the log_time of its first row.
        assert_eq!(messages[0].log_time, 1_000_000_000);
        assert_eq!(messages[1].log_time, 3_000_000_000);
        assert_eq!(decode_batch(&schema_msg, &messages[0].data).num_rows(), 2);
    }

    #[test]
    fn rows_per_message_ignores_non_leader_row_times() {
        // Only the first row of each group sets the message time, so null
        // values in non-leader rows must not fail the conversion.
        let batch = batch(vec![(
            "ts",
            Arc::new(TimestampSecondArray::from(vec![
                Some(1),
                None,
                Some(3),
                None,
            ])) as ArrayRef,
        )]);
        let mut opts = default_opts();
        opts.rows_per_message = 2;
        let mcap_bytes = convert_bytes(&ipc_file_bytes(&[batch]), "arrow", &opts)
            .expect("non-leader nulls should not fail conversion");
        let messages = read_messages(&mcap_bytes);
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0].log_time, 1_000_000_000);
        assert_eq!(messages[1].log_time, 3_000_000_000);
    }

    #[test]
    fn default_topic_uses_original_input_not_materialized_path() {
        use std::path::PathBuf;

        use crate::cli::{CompressionFormat, ConvertCommand};

        let args = ConvertCommand {
            input: PathBuf::from("s3://bucket/path/imu.arrow"),
            output: PathBuf::from("out.mcap"),
            compression: CompressionFormat::None,
            chunk_size: 1024,
            no_crc: true,
            no_chunks: false,
            topic: None,
            schema_name: None,
            log_time_field: None,
            publish_time_field: None,
            timestamp_unit: TimestampUnit::Ns,
            rows_per_message: 1,
        };
        let opts = ArrowConvertOptions::from_command(&args);
        assert_eq!(opts.default_topic, "imu");
    }

    #[test]
    fn reads_arrow_ipc_stream_format() {
        let batch = batch(vec![(
            "ts",
            Arc::new(TimestampSecondArray::from(vec![1, 2])) as ArrayRef,
        )]);
        let mcap_bytes = convert_bytes(&ipc_stream_bytes(&[batch]), "ipc", &default_opts())
            .expect("convert stream should succeed");
        assert_eq!(read_messages(&mcap_bytes).len(), 2);
    }

    #[test]
    fn converts_multiple_record_batches() {
        let b1 = batch(vec![(
            "ts",
            Arc::new(TimestampSecondArray::from(vec![1, 2])) as ArrayRef,
        )]);
        let b2 = batch(vec![(
            "ts",
            Arc::new(TimestampSecondArray::from(vec![3])) as ArrayRef,
        )]);
        let mcap_bytes = convert_bytes(&ipc_file_bytes(&[b1, b2]), "arrow", &default_opts())
            .expect("convert should succeed");
        let messages = read_messages(&mcap_bytes);
        assert_eq!(messages.len(), 3);
        assert_eq!(
            messages.iter().map(|m| m.sequence).collect::<Vec<_>>(),
            vec![0, 1, 2]
        );
    }

    #[test]
    fn honors_topic_and_schema_name_overrides() {
        let batch = batch(vec![(
            "ts",
            Arc::new(TimestampSecondArray::from(vec![1])) as ArrayRef,
        )]);
        let mut opts = default_opts();
        opts.topic = Some("/sensors/imu".to_string());
        opts.schema_name = Some("ImuBatch".to_string());
        let mcap_bytes = convert_bytes(&ipc_file_bytes(&[batch]), "arrow", &opts)
            .expect("convert should succeed");

        assert_channel(&mcap_bytes, "/sensors/imu");
        let (name, _data) = schema_record_bytes(&mcap_bytes);
        assert_eq!(name, "ImuBatch");
    }

    #[test]
    fn errors_when_no_time_field_can_be_resolved() {
        let batch = batch(vec![(
            "x",
            Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef,
        )]);
        let err = convert_bytes(&ipc_file_bytes(&[batch]), "arrow", &default_opts())
            .expect_err("missing time field should fail");
        assert!(err
            .to_string()
            .contains("could not determine a log_time field"));
    }

    #[test]
    fn errors_on_null_time_value() {
        let batch = batch(vec![(
            "log_time",
            Arc::new(TimestampSecondArray::from(vec![Some(1), None])) as ArrayRef,
        )]);
        let err = convert_bytes(&ipc_file_bytes(&[batch]), "arrow", &default_opts())
            .expect_err("null time should fail");
        assert!(err.to_string().contains("null value at row 1"));
    }

    #[test]
    fn errors_on_negative_integer_time() {
        let batch = batch(vec![(
            "log_time",
            Arc::new(Int64Array::from(vec![-1])) as ArrayRef,
        )]);
        let err = convert_bytes(&ipc_file_bytes(&[batch]), "arrow", &default_opts())
            .expect_err("negative time should fail");
        assert!(err.to_string().contains("out-of-range time value"));
    }

    #[test]
    fn errors_when_named_log_time_field_missing() {
        let batch = batch(vec![(
            "ts",
            Arc::new(TimestampSecondArray::from(vec![1])) as ArrayRef,
        )]);
        let mut opts = default_opts();
        opts.log_time_field = Some("nonexistent".to_string());
        let err = convert_bytes(&ipc_file_bytes(&[batch]), "arrow", &opts)
            .expect_err("missing named field should fail");
        assert!(err
            .to_string()
            .contains("was not found in the Arrow schema"));
    }
}
