//! Functions for parsing byte buffers into their MCAP record contents.
use crate::records::*;
use std::borrow::Cow;
use std::collections::BTreeMap;
use std::error::Error;

/// Returned from [`parse_record`] when parsing fails.
#[derive(Debug)]
pub enum ParseError {
    StringEncoding(std::str::Utf8Error),
    OpCodeNotImplemented(OpCode),
    DataTooShort,
}

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::DataTooShort => write!(f, "data ended unexpectedly before end of record"),
            Self::OpCodeNotImplemented(opcode) => write!(f, "opcode {:?} not supported", opcode),
            Self::StringEncoding(err) => write!(f, "string field not valid utf-8: {}", err),
        }
    }
}

impl Error for ParseError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            ParseError::StringEncoding(err) => Some(err),
            _ => None,
        }
    }
}

impl From<std::str::Utf8Error> for ParseError {
    fn from(err: std::str::Utf8Error) -> Self {
        ParseError::StringEncoding(err)
    }
}

fn parse_u16(data: &[u8]) -> Result<(u16, &[u8]), ParseError> {
    if data.len() < std::mem::size_of::<u16>() {
        return Err(ParseError::DataTooShort);
    }
    let (int_bytes, data) = data.split_at(std::mem::size_of::<u16>());
    Ok((
        u16::from_le_bytes(int_bytes.try_into().expect("expected 2 bytes")),
        data,
    ))
}

fn parse_u32(data: &[u8]) -> Result<(u32, &[u8]), ParseError> {
    if data.len() < std::mem::size_of::<u32>() {
        return Err(ParseError::DataTooShort);
    }
    let (int_bytes, data) = data.split_at(std::mem::size_of::<u32>());
    Ok((
        u32::from_le_bytes(int_bytes.try_into().expect("expected 4 bytes")),
        data,
    ))
}

pub(crate) fn parse_u64(data: &[u8]) -> Result<(u64, &[u8]), ParseError> {
    if data.len() < std::mem::size_of::<u64>() {
        return Err(ParseError::DataTooShort);
    }
    let (int_bytes, data) = data.split_at(std::mem::size_of::<u64>());
    Ok((
        u64::from_le_bytes(int_bytes.try_into().expect("expected 8 bytes")),
        data,
    ))
}

pub(crate) fn parse_byte_array(data: &[u8]) -> Result<(&[u8], &[u8]), ParseError> {
    let (len, data) = parse_u32(data)?;
    Ok(data.split_at(len as usize))
}

pub(crate) fn parse_long_byte_array(data: &[u8]) -> Result<(&[u8], &[u8]), ParseError> {
    let (len, data) = parse_u64(data)?;
    Ok(data.split_at(len as usize))
}

fn parse_str(data: &[u8]) -> Result<(&str, &[u8]), ParseError> {
    let (str_len, data) = parse_u32(data)?;
    if data.len() < str_len as usize {
        return Err(ParseError::DataTooShort);
    }
    let (str_bytes, data) = data.split_at(str_len as usize);
    Ok((std::str::from_utf8(str_bytes)?, data))
}

type StrMap<'a> = BTreeMap<Cow<'a, str>, Cow<'a, str>>;

fn parse_str_map(data: &[u8]) -> Result<(StrMap<'_>, &[u8]), ParseError> {
    let (map_len, data) = parse_u32(data)?;
    let (map_data, remainder) = data.split_at(map_len as usize);
    let mut result: BTreeMap<Cow<'_, str>, Cow<'_, str>> = BTreeMap::new();
    let mut unparsed_map_data = map_data;
    {
        while !unparsed_map_data.is_empty() {
            let (key, data) = parse_str(unparsed_map_data)?;
            let (val, data) = parse_str(data)?;
            unparsed_map_data = data;
            result.insert(key.into(), val.into());
        }
    }
    Ok((result, remainder))
}

fn parse_header(data: &[u8]) -> Result<Header, ParseError> {
    let (profile, data) = parse_str(data)?;
    let (library, _) = parse_str(data)?;
    Ok(Header {
        profile: profile.into(),
        library: library.into(),
    })
}

fn parse_footer(data: &[u8]) -> Result<Footer, ParseError> {
    let (summary_start, data) = parse_u64(data)?;
    let (summary_offset_start, data) = parse_u64(data)?;
    let (summary_crc, _) = parse_u32(data)?;
    Ok(Footer {
        summary_start,
        summary_offset_start,
        summary_crc,
    })
}

fn parse_schema(data: &[u8]) -> Result<Schema, ParseError> {
    let (id, data) = parse_u16(data)?;
    let (name, data) = parse_str(data)?;
    let (encoding, data) = parse_str(data)?;
    let (schema_data, _) = parse_byte_array(data)?;
    Ok(Schema {
        id,
        name: name.into(),
        encoding: encoding.into(),
        data: schema_data.into(),
    })
}

fn parse_channel(data: &[u8]) -> Result<Channel, ParseError> {
    let (id, data) = parse_u16(data)?;
    let (schema_id, data) = parse_u16(data)?;
    let (topic, data) = parse_str(data)?;
    let (message_encoding, data) = parse_str(data)?;
    let (metadata, _) = parse_str_map(data)?;

    Ok(Channel {
        id,
        schema_id,
        topic: topic.into(),
        message_encoding: message_encoding.into(),
        metadata,
    })
}

fn parse_message(data: &[u8]) -> Result<Message, ParseError> {
    let (channel_id, data) = parse_u16(data)?;
    let (sequence, data) = parse_u32(data)?;
    let (log_time, data) = parse_u64(data)?;
    let (publish_time, data) = parse_u64(data)?;
    Ok(Message {
        channel_id,
        sequence,
        log_time,
        publish_time,
        data: data.into(),
    })
}

fn parse_chunk(data: &[u8]) -> Result<Chunk, ParseError> {
    let (message_start_time, data) = parse_u64(data)?;
    let (message_end_time, data) = parse_u64(data)?;
    let (uncompressed_size, data) = parse_u64(data)?;
    let (uncompressed_crc, data) = parse_u32(data)?;
    let (compression, data) = parse_str(data)?;
    let (records, _) = parse_long_byte_array(data)?;
    Ok(Chunk {
        message_start_time,
        message_end_time,
        uncompressed_size,
        uncompressed_crc,
        compression: compression.into(),
        records: records.into(),
    })
}

fn parse_message_index(data: &[u8]) -> Result<MessageIndex, ParseError> {
    let (channel_id, data) = parse_u16(data)?;
    let (array_data_len, data) = parse_u32(data)?;
    let (array_data, _) = data.split_at(array_data_len as usize);
    let mut remaining_array_data = array_data;
    let mut records: Vec<(Timestamp, u64)> = Vec::new();
    while !remaining_array_data.is_empty() {
        let (timestamp, more) = parse_u64(remaining_array_data)?;
        let (offset, more) = parse_u64(more)?;
        remaining_array_data = more;
        records.push((timestamp, offset));
    }
    Ok(MessageIndex {
        channel_id,
        records,
    })
}

fn parse_chunk_index(data: &[u8]) -> Result<ChunkIndex, ParseError> {
    let (message_start_time, data) = parse_u64(data)?;
    let (message_end_time, data) = parse_u64(data)?;
    let (chunk_start_offset, data) = parse_u64(data)?;
    let (chunk_length, data) = parse_u64(data)?;
    let (message_index_offsets_len, data) = parse_u32(data)?;
    let (message_index_offset_data, data) = data.split_at(message_index_offsets_len as usize);
    let mut remaining_message_index_offset_data = message_index_offset_data;
    let mut message_index_offsets: BTreeMap<u16, u64> = BTreeMap::new();
    while !remaining_message_index_offset_data.is_empty() {
        let (channel_id, more) = parse_u16(remaining_message_index_offset_data)?;
        let (offset, more) = parse_u64(more)?;
        remaining_message_index_offset_data = more;
        message_index_offsets.insert(channel_id, offset);
    }
    let (message_index_length, data) = parse_u64(data)?;
    let (compression, data) = parse_str(data)?;
    let (compressed_size, data) = parse_u64(data)?;
    let (uncompressed_size, _) = parse_u64(data)?;
    Ok(ChunkIndex {
        message_start_time,
        message_end_time,
        chunk_start_offset,
        chunk_length,
        message_index_offsets,
        message_index_length,
        compression: compression.into(),
        compressed_size,
        uncompressed_size,
    })
}

fn parse_attachment(data: &[u8]) -> Result<Attachment, ParseError> {
    let (log_time, data) = parse_u64(data)?;
    let (create_time, data) = parse_u64(data)?;
    let (name, data) = parse_str(data)?;
    let (content_type, data) = parse_str(data)?;
    let (attachment_data, data) = parse_long_byte_array(data)?;
    let (crc, _) = parse_u32(data)?;
    Ok(Attachment {
        log_time,
        create_time,
        name: name.into(),
        content_type: content_type.into(),
        data: attachment_data.into(),
        crc,
    })
}

fn parse_attachment_index(data: &[u8]) -> Result<AttachmentIndex, ParseError> {
    let (offset, data) = parse_u64(data)?;
    let (length, data) = parse_u64(data)?;
    let (log_time, data) = parse_u64(data)?;
    let (create_time, data) = parse_u64(data)?;
    let (data_size, data) = parse_u64(data)?;
    let (name, data) = parse_str(data)?;
    let (content_type, _) = parse_str(data)?;
    Ok(AttachmentIndex {
        offset,
        length,
        log_time,
        create_time,
        data_size,
        name: name.into(),
        content_type: content_type.into(),
    })
}

fn parse_statistics(data: &[u8]) -> Result<Statistics, ParseError> {
    let (message_count, data) = parse_u64(data)?;
    let (schema_count, data) = parse_u16(data)?;
    let (channel_count, data) = parse_u32(data)?;
    let (attachment_count, data) = parse_u32(data)?;
    let (metadata_count, data) = parse_u32(data)?;
    let (chunk_count, data) = parse_u32(data)?;
    let (message_start_time, data) = parse_u64(data)?;
    let (message_end_time, data) = parse_u64(data)?;
    let (channel_message_counts_data_len, data) = parse_u32(data)?;
    if data.len() < channel_message_counts_data_len as usize {
        return Err(ParseError::DataTooShort);
    }
    let (channel_message_counts_data, _) = data.split_at(channel_message_counts_data_len as usize);
    let mut remaining = channel_message_counts_data;
    let mut channel_message_counts: BTreeMap<u16, u64> = BTreeMap::new();
    while !remaining.is_empty() {
        let (channel_id, more) = parse_u16(remaining)?;
        let (count, more) = parse_u64(more)?;
        channel_message_counts.insert(channel_id, count);
        remaining = more;
    }

    Ok(Statistics {
        message_count,
        schema_count,
        channel_count,
        attachment_count,
        metadata_count,
        chunk_count,
        message_start_time,
        message_end_time,
        channel_message_counts,
    })
}

fn parse_metadata(data: &[u8]) -> Result<Metadata<'_>, ParseError> {
    let (name, data) = parse_str(data)?;
    let (metadata, _) = parse_str_map(data)?;
    Ok(Metadata {
        name: name.into(),
        metadata,
    })
}

fn parse_metadata_index(data: &[u8]) -> Result<MetadataIndex<'_>, ParseError> {
    let (offset, data) = parse_u64(data)?;
    let (length, data) = parse_u64(data)?;
    let (name, _) = parse_str(data)?;
    Ok(MetadataIndex {
        offset,
        length,
        name: name.into(),
    })
}

fn parse_summary_offset(data: &[u8]) -> Result<SummaryOffset, ParseError> {
    if data.is_empty() {
        return Err(ParseError::DataTooShort);
    }
    let (opcode_buf, data) = data.split_at(1);
    let group_opcode = opcode_buf[0];
    let (group_start, data) = parse_u64(data)?;
    let (group_length, _) = parse_u64(data)?;
    Ok(SummaryOffset {
        group_opcode,
        group_start,
        group_length,
    })
}

fn parse_data_end(data: &[u8]) -> Result<DataEnd, ParseError> {
    let (data_section_crc, _) = parse_u32(data)?;
    Ok(DataEnd { data_section_crc })
}

/// Parses the content of an MCAP record from a buffer, without copying any string
/// or array fields. use the [`lifetime::IntoStatic`] implementation to create
/// an owned copy of the resulting parsed Record.
pub fn parse_record(opcode: OpCode, data: &[u8]) -> Result<Record<'_>, ParseError> {
    match opcode {
        OpCode::Header => Ok(Record::Header(parse_header(data)?)),
        OpCode::Footer => Ok(Record::Footer(parse_footer(data)?)),
        OpCode::Schema => Ok(Record::Schema(parse_schema(data)?)),
        OpCode::Channel => Ok(Record::Channel(parse_channel(data)?)),
        OpCode::Message => Ok(Record::Message(parse_message(data)?)),
        OpCode::Chunk => Ok(Record::Chunk(parse_chunk(data)?)),
        OpCode::MessageIndex => Ok(Record::MessageIndex(parse_message_index(data)?)),
        OpCode::ChunkIndex => Ok(Record::ChunkIndex(parse_chunk_index(data)?)),
        OpCode::Attachment => Ok(Record::Attachment(parse_attachment(data)?)),
        OpCode::AttachmentIndex => Ok(Record::AttachmentIndex(parse_attachment_index(data)?)),
        OpCode::Statistics => Ok(Record::Statistics(parse_statistics(data)?)),
        OpCode::Metadata => Ok(Record::Metadata(parse_metadata(data)?)),
        OpCode::MetadataIndex => Ok(Record::MetadataIndex(parse_metadata_index(data)?)),
        OpCode::SummaryOffset => Ok(Record::SummaryOffset(parse_summary_offset(data)?)),
        OpCode::DataEnd => Ok(Record::DataEnd(parse_data_end(data)?)),
        OpCode::UserOpcode(_) => Err(ParseError::OpCodeNotImplemented(opcode)),
    }
}

/// Parses only the first fields of an Attachment record, before the `data` field. This
/// allows a reader to read and parse only the header of an attachment before deciding whether
/// to use the rest of the attachment.
pub fn parse_attachment_header(data: &'_ [u8]) -> Result<AttachmentHeader<'_>, ParseError> {
    let (log_time, data) = parse_u64(data)?;
    let (create_time, data) = parse_u64(data)?;
    let (name, data) = parse_str(data)?;
    let (content_type, data) = parse_str(data)?;
    let (data_len, _) = parse_u64(data)?;
    Ok(AttachmentHeader {
        log_time,
        create_time,
        name: name.into(),
        content_type: content_type.into(),
        data_len,
    })
}
