use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::error::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OpCode {
    Header,
    Footer,
    Schema,
    Channel,
    Message,
    Chunk,
    MessageIndex,
    ChunkIndex,
    Attachment,
    AttachmentIndex,
    Statistics,
    Metadata,
    MetadataIndex,
    SummaryOffset,
    DataEnd,
    UserOpcode(u8),
}

impl TryFrom<u8> for OpCode {
    type Error = ParseError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0x00 => Err(ParseError::InvalidOpcode(0x00)),
            0x01 => Ok(OpCode::Header),
            0x02 => Ok(OpCode::Footer),
            0x03 => Ok(OpCode::Schema),
            0x04 => Ok(OpCode::Channel),
            0x05 => Ok(OpCode::Message),
            0x06 => Ok(OpCode::Chunk),
            0x07 => Ok(OpCode::MessageIndex),
            0x08 => Ok(OpCode::ChunkIndex),
            0x09 => Ok(OpCode::Attachment),
            0x0A => Ok(OpCode::AttachmentIndex),
            0x0B => Ok(OpCode::Statistics),
            0x0C => Ok(OpCode::Metadata),
            0x0D => Ok(OpCode::MetadataIndex),
            0x0E => Ok(OpCode::SummaryOffset),
            0x0F => Ok(OpCode::DataEnd),
            x if x < 0x80 => Err(ParseError::InvalidOpcode(x)),
            x => Ok(OpCode::UserOpcode(x)),
        }
    }
}

impl Into<u8> for OpCode {
    fn into(self) -> u8 {
        match self {
            OpCode::Header => 0x01,
            OpCode::Footer => 0x02,
            OpCode::Schema => 0x03,
            OpCode::Channel => 0x04,
            OpCode::Message => 0x05,
            OpCode::Chunk => 0x06,
            OpCode::MessageIndex => 0x07,
            OpCode::ChunkIndex => 0x08,
            OpCode::Attachment => 0x09,
            OpCode::AttachmentIndex => 0x0A,
            OpCode::Statistics => 0x0B,
            OpCode::Metadata => 0x0C,
            OpCode::MetadataIndex => 0x0D,
            OpCode::SummaryOffset => 0x0E,
            OpCode::DataEnd => 0x0F,
            OpCode::UserOpcode(x) => x,
        }
    }
}

type Timestamp = u64;

#[derive(PartialEq, Debug, Serialize, Deserialize)]
pub enum Record<'a> {
    Header {
        library: &'a str,
        profile: &'a str,
    },
    Footer {
        summary_start: u64,
        summary_offset_start: u64,
        summary_crc: u32,
    },
    Schema {
        id: u16,
        name: &'a str,
        encoding: &'a str,
        data: &'a [u8],
    },
    Channel {
        id: u16,
        schema_id: u16,
        topic: &'a str,
        message_encoding: &'a str,
        metadata: BTreeMap<&'a str, &'a str>,
    },
    Message {
        channel_id: u16,
        sequence: u32,
        log_time: Timestamp,
        publish_time: Timestamp,
        data: &'a [u8],
    },
    Chunk {
        message_start_time: Timestamp,
        message_end_time: Timestamp,
        uncompressed_size: u64,
        uncompressed_crc: u32,
        compression: &'a str,
        records: &'a [u8],
    },
    MessageIndex {
        channel_id: u16,
        records: Vec<(Timestamp, u64)>,
    },
    ChunkIndex {
        message_start_time: Timestamp,
        message_end_time: Timestamp,
        chunk_start_offset: u64,
        chunk_length: u64,
        message_index_offsets: BTreeMap<u16, u64>,
        message_index_length: u64,
        compression: &'a str,
        compressed_size: u64,
        uncompressed_size: u64,
    },
    Attachment {
        log_time: Timestamp,
        create_time: Timestamp,
        name: &'a str,
        content_type: &'a str,
        data: &'a [u8],
        crc: u32,
    },
    AttachmentIndex {
        offset: u64,
        length: u64,
        log_time: Timestamp,
        create_time: Timestamp,
        data_size: u64,
        name: &'a str,
        content_type: &'a str,
    },
    Statistics {
        message_count: u64,
        schema_count: u16,
        channel_count: u32,
        attachment_count: u32,
        metadata_count: u32,
        chunk_count: u32,
        message_start_time: Timestamp,
        message_end_time: Timestamp,
        channel_message_counts: BTreeMap<u16, u64>,
    },
    Metadata {
        name: &'a str,
        metadata: BTreeMap<&'a str, &'a str>,
    },
    MetadataIndex {
        offset: u64,
        length: u64,
        name: &'a str,
    },
    SummaryOffset {
        group_opcode: OpCode,
        group_start: u64,
        group_length: u64,
    },
    DataEnd {
        data_section_crc: u32,
    },
}

#[derive(Debug)]
pub enum ParseError {
    BadMagic,
    InvalidOpcode(u8),
    StringEncoding(std::str::Utf8Error),
    OpCodeNotImplemented(OpCode),
    DataTooShort,
    RecordTooLong(Vec<u8>),
}

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{:?}", self)
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

fn parse_u16<'a>(data: &'a [u8]) -> Result<(u16, &'a [u8]), ParseError> {
    if data.len() < std::mem::size_of::<u16>() {
        return Err(ParseError::DataTooShort);
    }
    let (int_bytes, data) = data.split_at(std::mem::size_of::<u16>());
    return Ok((u16::from_le_bytes(int_bytes.try_into().unwrap()), data));
}

fn parse_u32<'a>(data: &'a [u8]) -> Result<(u32, &'a [u8]), ParseError> {
    if data.len() < std::mem::size_of::<u32>() {
        return Err(ParseError::DataTooShort);
    }
    let (int_bytes, data) = data.split_at(std::mem::size_of::<u32>());
    return Ok((u32::from_le_bytes(int_bytes.try_into().unwrap()), data));
}

pub fn parse_u64<'a>(data: &'a [u8]) -> Result<(u64, &'a [u8]), ParseError> {
    if data.len() < std::mem::size_of::<u64>() {
        return Err(ParseError::DataTooShort);
    }
    let (int_bytes, data) = data.split_at(std::mem::size_of::<u64>());
    return Ok((u64::from_le_bytes(int_bytes.try_into().unwrap()), data));
}

pub fn parse_bytearray<'a>(data: &'a [u8]) -> Result<(&'a [u8], &'a [u8]), ParseError> {
    let (len, data) = parse_u32(data)?;
    Ok(data.split_at(len as usize))
}

pub fn parse_long_bytearray<'a>(data: &'a [u8]) -> Result<(&'a [u8], &'a [u8]), ParseError> {
    let (len, data) = parse_u64(data)?;
    Ok(data.split_at(len as usize))
}

fn parse_str<'a>(data: &'a [u8]) -> Result<(&'a str, &'a [u8]), ParseError> {
    let (str_len, data) = parse_u32(data)?;
    if data.len() < str_len as usize {
        return Err(ParseError::DataTooShort);
    }
    let (str_bytes, data) = data.split_at(str_len as usize);
    return Ok((std::str::from_utf8(str_bytes)?, data));
}

fn parse_str_map<'a>(data: &'a [u8]) -> Result<(BTreeMap<&'a str, &'a str>, &'a [u8]), ParseError> {
    let (map_len, data) = parse_u32(data)?;
    let (map_data, remainder) = data.split_at(map_len as usize);
    let mut result: BTreeMap<&'a str, &'a str> = BTreeMap::new();
    let mut unparsed_map_data = map_data;
    {
        while unparsed_map_data.len() > 0 {
            let (key, data) = parse_str(unparsed_map_data)?;
            let (val, data) = parse_str(data)?;
            unparsed_map_data = data;
            result.insert(key, val);
        }
    }
    Ok((result, remainder))
}

fn parse_header<'a>(data: &'a [u8]) -> Result<Record<'a>, ParseError> {
    let (profile, data) = parse_str(data)?;
    let (library, data) = parse_str(data)?;
    if data.len() > 0 {
        return Err(ParseError::RecordTooLong(data.into()));
    }
    Ok(Record::Header {
        profile: profile,
        library: library,
    })
}

fn parse_footer<'a>(data: &'a [u8]) -> Result<Record<'a>, ParseError> {
    let (summary_start, data) = parse_u64(data)?;
    let (summary_offset_start, data) = parse_u64(data)?;
    let (crc, data) = parse_u32(data)?;
    if data.len() > 0 {
        return Err(ParseError::RecordTooLong(data.into()));
    }
    Ok(Record::Footer {
        summary_start: summary_start,
        summary_offset_start: summary_offset_start,
        summary_crc: crc,
    })
}

fn parse_schema<'a>(data: &'a [u8]) -> Result<Record<'a>, ParseError> {
    let (id, data) = parse_u16(data)?;
    let (name, data) = parse_str(data)?;
    let (encoding, data) = parse_str(data)?;
    let (schema_data, data) = parse_bytearray(data)?;
    if data.len() > 0 {
        return Err(ParseError::RecordTooLong(data.into()));
    }
    Ok(Record::Schema {
        id: id,
        name: name,
        encoding: encoding,
        data: schema_data,
    })
}

fn parse_channel<'a>(data: &'a [u8]) -> Result<Record<'a>, ParseError> {
    let (id, data) = parse_u16(data)?;
    let (schema_id, data) = parse_u16(data)?;
    let (topic, data) = parse_str(data)?;
    let (message_encoding, data) = parse_str(data)?;
    let (metadata, data) = parse_str_map(data)?;
    if data.len() > 0 {
        return Err(ParseError::RecordTooLong(data.into()));
    }

    Ok(Record::Channel {
        id: id,
        schema_id: schema_id,
        topic: topic,
        message_encoding: message_encoding,
        metadata: metadata,
    })
}

fn parse_message<'a>(data: &'a [u8]) -> Result<Record<'a>, ParseError> {
    let (channel_id, data) = parse_u16(data)?;
    let (sequence, data) = parse_u32(data)?;
    let (log_time, data) = parse_u64(data)?;
    let (publish_time, data) = parse_u64(data)?;
    Ok(Record::Message {
        channel_id: channel_id,
        sequence: sequence,
        log_time: log_time,
        publish_time: publish_time,
        data: data,
    })
}

fn parse_chunk<'a>(data: &'a [u8]) -> Result<Record<'a>, ParseError> {
    let (message_start_time, data) = parse_u64(data)?;
    let (message_end_time, data) = parse_u64(data)?;
    let (uncompressed_size, data) = parse_u64(data)?;
    let (uncompressed_crc, data) = parse_u32(data)?;
    let (compression, data) = parse_str(data)?;
    let (records, data) = parse_long_bytearray(data)?;
    if data.len() != 0 {
        return Err(ParseError::RecordTooLong(data.into()));
    }
    Ok(Record::Chunk {
        message_start_time: message_start_time,
        message_end_time: message_end_time,
        uncompressed_size: uncompressed_size,
        uncompressed_crc: uncompressed_crc,
        compression: compression,
        records: records,
    })
}

fn parse_message_index<'a>(data: &'a [u8]) -> Result<Record<'a>, ParseError> {
    let (channel_id, data) = parse_u16(data)?;
    let (array_data_len, data) = parse_u32(data)?;
    let (array_data, data) = data.split_at(array_data_len as usize);
    if data.len() > 0 {
        return Err(ParseError::RecordTooLong(data.into()));
    }
    let mut remaining_array_data = array_data;
    let mut records: Vec<(Timestamp, u64)> = Vec::new();
    while remaining_array_data.len() > 0 {
        let (timestamp, more) = parse_u64(remaining_array_data)?;
        let (offset, more) = parse_u64(more)?;
        remaining_array_data = more;
        records.push((timestamp, offset));
    }
    Ok(Record::MessageIndex {
        channel_id: channel_id,
        records: records,
    })
}

fn parse_chunk_index<'a>(data: &'a [u8]) -> Result<Record<'a>, ParseError> {
    let (message_start_time, data) = parse_u64(data)?;
    let (message_end_time, data) = parse_u64(data)?;
    let (chunk_start_offset, data) = parse_u64(data)?;
    let (chunk_length, data) = parse_u64(data)?;
    let (message_index_offsets_len, data) = parse_u32(data)?;
    let (message_index_offset_data, data) = data.split_at(message_index_offsets_len as usize);
    let mut remaining_message_index_offset_data = message_index_offset_data;
    let mut message_index_offsets: BTreeMap<u16, u64> = BTreeMap::new();
    while remaining_message_index_offset_data.len() > 0 {
        let (channel_id, more) = parse_u16(remaining_message_index_offset_data)?;
        let (offset, more) = parse_u64(more)?;
        remaining_message_index_offset_data = more;
        message_index_offsets.insert(channel_id, offset);
    }
    let (message_index_length, data) = parse_u64(data)?;
    let (compression, data) = parse_str(data)?;
    let (compressed_size, data) = parse_u64(data)?;
    let (uncompressed_size, data) = parse_u64(data)?;
    if data.len() > 0 {
        return Err(ParseError::RecordTooLong(data.into()));
    }
    Ok(Record::ChunkIndex {
        message_start_time: message_start_time,
        message_end_time: message_end_time,
        chunk_start_offset: chunk_start_offset,
        chunk_length: chunk_length,
        message_index_offsets: message_index_offsets,
        message_index_length: message_index_length,
        compression: compression,
        compressed_size: compressed_size,
        uncompressed_size: uncompressed_size,
    })
}

fn parse_attachment<'a>(data: &'a [u8]) -> Result<Record<'a>, ParseError> {
    let (log_time, data) = parse_u64(data)?;
    let (create_time, data) = parse_u64(data)?;
    let (name, data) = parse_str(data)?;
    let (content_type, data) = parse_str(data)?;
    let (attachment_data, data) = parse_long_bytearray(data)?;
    let (crc, data) = parse_u32(data)?;
    if data.len() > 0 {
        return Err(ParseError::RecordTooLong(data.into()));
    }
    Ok(Record::Attachment {
        log_time: log_time,
        create_time: create_time,
        name: name,
        content_type: content_type,
        data: attachment_data,
        crc: crc,
    })
}

fn parse_attachment_index<'a>(data: &'a [u8]) -> Result<Record<'a>, ParseError> {
    let (offset, data) = parse_u64(data)?;
    let (length, data) = parse_u64(data)?;
    let (log_time, data) = parse_u64(data)?;
    let (create_time, data) = parse_u64(data)?;
    let (data_size, data) = parse_u64(data)?;
    let (name, data) = parse_str(data)?;
    let (content_type, data) = parse_str(data)?;
    if data.len() > 0 {
        return Err(ParseError::RecordTooLong(data.into()));
    }
    Ok(Record::AttachmentIndex {
        offset: offset,
        length: length,
        log_time: log_time,
        create_time: create_time,
        data_size: data_size,
        name: name,
        content_type: content_type,
    })
}

fn parse_statistics<'a>(data: &'a [u8]) -> Result<Record<'a>, ParseError> {
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
    let (channel_message_counts_data, data) =
        data.split_at(channel_message_counts_data_len as usize);
    if data.len() > 0 {
        return Err(ParseError::RecordTooLong(data.into()));
    }
    let mut remaining = channel_message_counts_data;
    let mut channel_message_counts: BTreeMap<u16, u64> = BTreeMap::new();
    while remaining.len() > 0 {
        let (channel_id, more) = parse_u16(remaining)?;
        let (count, more) = parse_u64(more)?;
        channel_message_counts.insert(channel_id, count);
        remaining = more;
    }

    Ok(Record::Statistics {
        message_count: message_count,
        schema_count: schema_count,
        channel_count: channel_count,
        attachment_count: attachment_count,
        metadata_count: metadata_count,
        chunk_count: chunk_count,
        message_start_time: message_start_time,
        message_end_time: message_end_time,
        channel_message_counts: channel_message_counts,
    })
}

fn parse_metadata<'a>(data: &'a [u8]) -> Result<Record<'a>, ParseError> {
    let (name, data) = parse_str(data)?;
    let (metadata, data) = parse_str_map(data)?;
    if data.len() > 0 {
        return Err(ParseError::RecordTooLong(data.into()));
    }
    Ok(Record::Metadata {
        name: name,
        metadata: metadata,
    })
}

fn parse_metadata_index<'a>(data: &'a [u8]) -> Result<Record<'a>, ParseError> {
    let (offset, data) = parse_u64(data)?;
    let (length, data) = parse_u64(data)?;
    let (name, data) = parse_str(data)?;
    if data.len() > 0 {
        return Err(ParseError::RecordTooLong(data.into()));
    }
    Ok(Record::MetadataIndex {
        offset: offset,
        length: length,
        name: name,
    })
}

fn parse_summary_offset<'a>(data: &'a [u8]) -> Result<Record<'a>, ParseError> {
    if data.len() < 1 {
        return Err(ParseError::DataTooShort);
    }
    let opcode = OpCode::try_from(data[0])?;
    let (group_start, data) = parse_u64(data)?;
    let (group_length, data) = parse_u64(data)?;
    if data.len() > 0 {
        return Err(ParseError::RecordTooLong(data.into()));
    }
    Ok(Record::SummaryOffset {
        group_opcode: opcode,
        group_start: group_start,
        group_length: group_length,
    })
}

fn parse_data_end<'a>(data: &'a [u8]) -> Result<Record<'a>, ParseError> {
    let (data_section_crc, _) = parse_u32(data)?;
    Ok(Record::DataEnd {
        data_section_crc: data_section_crc,
    })
}

pub fn parse_record<'a>(opcode: OpCode, data: &'a [u8]) -> Result<Record<'a>, ParseError> {
    match opcode {
        OpCode::Header => parse_header(data),
        OpCode::Footer => parse_footer(data),
        OpCode::Schema => parse_schema(data),
        OpCode::Channel => parse_channel(data),
        OpCode::Message => parse_message(data),
        OpCode::Chunk => parse_chunk(data),
        OpCode::MessageIndex => parse_message_index(data),
        OpCode::ChunkIndex => parse_chunk_index(data),
        OpCode::Attachment => parse_attachment(data),
        OpCode::AttachmentIndex => parse_attachment_index(data),
        OpCode::Statistics => parse_statistics(data),
        OpCode::Metadata => parse_metadata(data),
        OpCode::MetadataIndex => parse_metadata_index(data),
        OpCode::SummaryOffset => parse_summary_offset(data),
        OpCode::DataEnd => parse_data_end(data),
        OpCode::UserOpcode(_) => Err(ParseError::OpCodeNotImplemented(opcode)),
    }
}

#[cfg(test)]
mod tests {}
